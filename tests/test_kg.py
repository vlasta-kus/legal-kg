import json
import unittest
import logging
from src.kg import KnowledgeGraph

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s: %(message)s")


class TestKG(unittest.TestCase):
    def test_kg_initialisation(self):
        kg = KnowledgeGraph('neo4j', 'resources')
        self.assertIsNotNone(kg)

    @unittest.skip("Makes changes to Neo4j DB.")
    def test_kg_ingestion(self):
        kg = KnowledgeGraph('neo4j', 'resources')
        kg.initialise_indices()
        succ = kg.ingest_data("data")
        #kg.close()
        self.assertTrue(succ, "No files ingested.")  # add assertion here

    def test_kg_cypher_generation(self):
        with open("resources/gpt_output.json", 'r') as f:
            gpt_output = json.load(f)
        kg = KnowledgeGraph('neo4j', 'resources')
        queries = kg.generate_cypher("4:fd52e840-5039-4b9a-825c-7de81396aab9:9328", gpt_output)
        print(queries[1])
        self.assertTrue(len(queries) == 2, "Unexpected number of entity & relation storage queries.")

    @unittest.skip("Makes changes to Neo4j DB.")
    def test_kg_extraction(self):
        QUERY = """MATCH (f:File)-[:CONTAINS_PAGE]->(p:Page)
        WHERE elementId(f) = "4:fd52e840-5039-4b9a-825c-7de81396aab9:9328"
        RETURN elementId(f) AS element_id,
        REDUCE(mergedString="", page IN collect(p.text) | mergedString + page + '\n\n') AS text
        """

        # Whole batch of test documents
        QUERY_BATCH = """MATCH (f:TestSet)-[:CONTAINS_PAGE]->(p:Page)
        WHERE NOT f:LLMProcessed
        RETURN elementId(f) AS element_id, REDUCE(mergedString="", page IN collect(p.text) | mergedString + page + '\n\n') AS text
        """

        kg = KnowledgeGraph('neo4j', 'resources')
        kg.extract_knowledge(QUERY_BATCH, "gpt-4", "generic_v4", 5000)

    def test_kg_final_layer(self):
        kg = KnowledgeGraph('neo4j', 'resources')
        n_rels = kg.create_knowledge_layer()
        self.assertTrue(n_rels > 0, "No relationships created!")

if __name__ == '__main__':
    unittest.main()
