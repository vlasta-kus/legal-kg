import json
import csv
import os
import time
from src.utils import LoggingHandler
from src.neo4jWriter import Neo4jWriter
from src.dataLoader import DataLoader
from src.openaiQuery import OpenAIQuery


class KnowledgeGraph(LoggingHandler):
    def __init__(self, neo4j_db: str, config_dir: str):
        super().__init__(self)
        self.neo4j_db = neo4j_db
        #self.neo = Neo4jWriter(neo4j_db)
        self.config_dir = config_dir
        self.queries = self.read_queries()
        self.initialise_indices()

        # read desired KG schema
        schema_path = os.path.join(self.config_dir, 'schema.txt')
        if not os.path.exists(schema_path):
            self.log.error(f"Missing KG schema definition! Please add file: {schema_path}")
        with open(schema_path, 'r') as f:
            reader = csv.reader(f)
            self.SCHEMA = list(reader)

    #def close(self):
    #    self.neo.close()

    def initialise_indices(self):
        QUERIES = [
            {'query': "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Directory) REQUIRE n.id IS NODE KEY", 'data': None},
            {'query': "CREATE CONSTRAINT IF NOT EXISTS FOR (n:File) REQUIRE n.id IS NODE KEY", 'data': None},
            {'query': "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Page) REQUIRE n.id IS NODE KEY", 'data': None},

            # node indices
            {'query': "CREATE TEXT INDEX node_entity_name IF NOT EXISTS FOR (n:Entity) ON (n.name)", 'data': None},
            {'query': "CREATE TEXT INDEX node_entity_label IF NOT EXISTS FOR (n:Entity) ON (n._label_llm)", 'data': None},
            {'query': "CREATE TEXT INDEX per_name IF NOT EXISTS FOR (n:Person) ON (n.name)", 'data': None},
            {'query': "CREATE TEXT INDEX org_name IF NOT EXISTS FOR (n:Organization) ON (n.name)", 'data': None},
            {'query': "CREATE TEXT INDEX fac_name IF NOT EXISTS FOR (n:Facility) ON (n.name)", 'data': None},
            {'query': "CREATE TEXT INDEX loc_name IF NOT EXISTS FOR (n:Location) ON (n.name)", 'data': None},
            {'query': "CREATE TEXT INDEX substance_name IF NOT EXISTS FOR (n:Substance) ON (n.name)", 'data': None},
            {'query': "CREATE TEXT INDEX health_name IF NOT EXISTS FOR (n:HealthFactor) ON (n.name)", 'data': None},
            {'query': "CREATE TEXT INDEX product_name IF NOT EXISTS FOR (n:Product) ON (n.name)", 'data': None},
            {'query': "CREATE TEXT INDEX event_name IF NOT EXISTS FOR (n:Event) ON (n.name)", 'data': None},

            # fulltext indices
            {'query': "CREATE FULLTEXT INDEX pageTexts IF NOT EXISTS FOR (n:Page) ON EACH [n.text]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX fileNames IF NOT EXISTS FOR (n:File) ON EACH [n.name]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX entityNames IF NOT EXISTS FOR (n:Entity) ON EACH [n.name]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX perNames IF NOT EXISTS FOR (n:Person) ON EACH [n.name]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX orgNames IF NOT EXISTS FOR (n:Organization) ON EACH [n.name]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX facNames IF NOT EXISTS FOR (n:Facility) ON EACH [n.name]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX locNames IF NOT EXISTS FOR (n:Location) ON EACH [n.name]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX substanceNames IF NOT EXISTS FOR (n:Substance) ON EACH [n.name]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX healthNames IF NOT EXISTS FOR (n:HealthFactor) ON EACH [n.name]", 'data': None},
            {'query': "CREATE FULLTEXT INDEX productNames IF NOT EXISTS FOR (n:Product) ON EACH [n.name]", 'data': None}
            #{'query': "", 'data': None}
        ]
        neo = Neo4jWriter(self.neo4j_db)
        self.log.info(f"Creating {len(QUERIES)} Neo4j indices & constraints.")
        neo.run_multi_queries(QUERIES)
        neo.close()

    def read_queries(self) -> dict:
        """
        Read pre-built Cypher queries that will be later on used to build the KG.
        :return:
        """
        dir = os.path.join(self.config_dir, 'queries')
        if not os.path.exists(dir):
            self.log.error(f"Directory with Cypher queries not found: {dir}")
            return None
        queries = dict()
        self.log.debug(f"Reading error query.")
        with open(os.path.join(dir, "error.txt"), 'r') as f:
            queries['error'] = f.read()
        self.log.debug(f"Reading entities query.")
        with open(os.path.join(dir, "entities.txt"), 'r') as f:
            queries['entities'] = f.read()
        self.log.debug(f"Reading relations query.")
        with open(os.path.join(dir, "relations.txt"), 'r') as f:
            queries['relations'] = f.read()
        self.log.debug(f"Reading KG creation query.")
        with open(os.path.join(dir, "create_kg.txt"), 'r') as f:
            queries['create_kg'] = f.read()
        return queries

    def ingest_data(self, data_dir: str):
        """
        Crawl the specified directory with all its subdirectories and store this data structure & file contents
        to Neo4j DB.
        :param data_dir:
        :return:
        """
        QUERY_DIRS = """MERGE (d1:Directory {id: $id1}) SET d1.name = $name1
        MERGE (d2:Directory {id: $id2}) SET d2.name = $name2
        MERGE (d1)-[:CONTAINS_DIR]->(d2)
        """

        QUERY_FILES = """MATCH (d:Directory {id: $directory_id})
        MERGE (f:File {id: $id}) 
        SET f.name = $name
        MERGE (d)-[:CONTAINS_FILE]->(f)
        
        WITH f
        UNWIND $pages AS page
        
        MERGE (p:Page {id: page.id})
        SET p += page.others
        MERGE (f)-[:CONTAINS_PAGE]->(p)
        """

        neo = Neo4jWriter(self.neo4j_db)
        data = DataLoader.from_path(data_dir)

        full_content = data.crawl_and_identify()

        self.log.info(f"Storing directory structure ({len(full_content['directory_pairs'])} dir-subdir pairs) to Neo4j.")
        for dirs in full_content['directory_pairs']:
            payload = {'id1': dirs['source'], 'name1': dirs['source'].split("/")[-1],
                       'id2': dirs['target'], 'name2': dirs['target'].split("/")[-1]
                       }
            neo.run_simple_query(QUERY_DIRS, payload)

        self.log.info(f"Storing {len(full_content['files'])} files to Neo4j.")
        wait_counter = 0
        for file in full_content['files']:
            wait_counter += 1
            if wait_counter % 50 == 0:
                self.log.info(f"Waiting 5 sec because of Neo4j Aura `Unable to retrieve routing information` error.")
                wait_counter = 0
                time.sleep(5)
            payload = {'id': file['path'],
                       'name': file['name'],
                       'directory_id': file['directory'],
                       'pages': list()
                       }
            pages = data.read_json(file['path']) # typically, files are sub-structured into pages
            for page in pages:
                if 'text' not in page:
                    self.log.warning(f"A page in given file missing `text` field: {file['path']}")
                    page['text'] = ""
                else:
                    page['text'] = page['text'].strip()
                if 'pageNumber' not in page:
                    self.log.error(f"A page in given file missing `pageNumber` field: {file['path']}")
                    continue
                if 'id' in page:
                    page['id_page'] = page['id']
                    del page['id']
                for key, val in page.items():
                    if isinstance(val, dict):
                        page[key] = str(val)
                payload['pages'].append({'id': f"{file['path']}__{page['pageNumber']}",
                                         'others': page
                                         })
            try:
                neo.run_simple_query(QUERY_FILES, payload)
            except Exception as e:
                self.log.error(f"Failure when storing a file to Neo4j: {e}\n\tFile: {file['path']}")
                self.log.debug(f"\n\tContent: {payload}")

        neo.close()

        return len(full_content['files']) > 0

    def extract_knowledge(self, data_query: str, model: str, prompt_version: str, max_tokens: int=2000):
        neo = Neo4jWriter(self.neo4j_db)
        data = neo.run_simple_query(data_query)
        self.log.info(f"Extracting knowledge from {len(data)} documents.")

        openai = OpenAIQuery(os.path.join(self.config_dir, "prompts"), prompt_version, max_tokens)
        for doc in data:
            #if 'text' not in doc['properties']:
            #    self.log.info(f"Missing text property in document with ID {doc['elementId']}")
            #    continue
            if len(doc['text']) < 100: #doc['properties']
                self.log.info(f"Text too short in document with ID {doc['element_id']}")
                continue
            self.log.info(f"Running LLM for document ID {doc['element_id']}")
            result = openai.query(doc['text'], model)
            #with open("resources/gpt_output.json", 'w') as f:
            #    json.dump(result, f)
            queries = self.generate_cypher(doc['element_id'], result)
            if queries is None:
                continue
            self.log.info(f"Storing {len(result['entities'])} entities and {len(result['relations'])} relations to Neo4j.")
            neo.run_multi_queries(queries)

        neo.close()

    def generate_cypher(self, element_id: str, gpt_output: dict) -> list:
        self.log.debug("Generating Cypher from LLM output.")
        if 'error' in gpt_output:
            return [{'query': self.queries['error'],
                    'data': {'element_id': element_id, 'error': gpt_output['error'], 'response': gpt_output['response']}
                    }]
        if 'entities' not in gpt_output:
            self.log.warning(f"Missing `entities` key:\n{gpt_output}")
            return None
        if 'relations' not in gpt_output:
            self.log.warning(f"Missing `relations` key:\n{gpt_output}")
            return None
        content_type = gpt_output['content_type'] if 'content_type' in gpt_output else None

        ents, rels = list(), list()
        for label, arr in gpt_output['entities'].items():
            for e in arr:
                e['_label_llm'] = label.strip()
                #e['_label'] = "".join([x[0].upper() + x[1:].lower() for x in label.strip().split()])
                if 'wikipedia_id' in e and e['wikipedia_id'] is not None:
                    e['wikipedia_url'] = "https://en.wikipedia.org/wiki/" + e['wikipedia_id']
                e['_all_properties'] = str({k:v for k,v in e.items() if not k.startswith("_") and k not in ["id"]})
                ents.append(e)
        for rel_type, arr in gpt_output['relations'].items():
            for r in arr:
                r['_type_llm'] = rel_type.strip()
                #r['_type'] = "_".join(rel_type.strip().upper().split())
                r['_all_properties'] = str({k:v for k,v in r.items() if not k.startswith("_") and k not in ["source", "target"]})
                rels.append(r)

        return [{'query': self.queries['entities'], 'data': {'element_id': element_id, 'entities': ents, 'content_type': content_type}},
                {'query': self.queries['relations'], 'data': {'element_id': element_id, 'relations': rels}}]

    def create_knowledge_layer(self):
        neo = Neo4jWriter(self.neo4j_db)

        # Reset the knowledge layer
        QUERY_DELETE_KG = """MATCH (n) 
            WHERE n:KGEntity OR n:ExplainRelation
            DETACH DELETE n
            """
        self.log.info("Cleansing the knowledge layer.")
        neo.run_simple_query(QUERY_DELETE_KG)

        self.log.info("Creating final knowledge layer.")
        res = neo.run_simple_query(self.queries['create_kg'], {'schema': self.SCHEMA})
        self.log.info(f"Created {res[0]['n_rels']} relationships.")

        neo.close()

        return res[0]['n_rels']