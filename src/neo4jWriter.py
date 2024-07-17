import os
from typing import List, Dict
from neo4j import GraphDatabase, basic_auth
from src.utils import LoggingHandler


class Neo4jWriter(LoggingHandler):
    def __init__(self, db: str):#, uri:str, username: str, pwd: str):
        super().__init__(self)
        self.db = db

        if 'NEO4J_URI' not in os.environ:
            self.log.error(f"Missing environment variable: NEO4J_URI")
        if 'NEO4J_USER' not in os.environ:
            self.log.error(f"Missing environment variable: NEO4J_USER")
        if 'NEO4J_PWD' not in os.environ:
            self.log.error(f"Missing environment variable: NEO4J_PWD")

        # Initialise Neo4j driver
        #self.driver = GraphDatabase.driver(uri, auth=basic_auth(username, pwd))
        self.driver = GraphDatabase.driver(
            os.environ['NEO4J_URI'],
            auth=basic_auth(os.environ['NEO4J_USER'], os.environ['NEO4J_PWD'])
        )

    def close(self):
        self.driver.close()

    def run_simple_query(self, query: str, data: dict = None, db: str = None) -> List[Dict]:
        """
        Execute simple read or write (can contain payload in `data` parameter) query.
        :param query: Cypher query
        :param data: payload (typically for write query) accessible from Cypher through `$data` parameter
        :param db: Neo4j DB to execute against (optional)
        :return: list of dicts corresponding to the RETURN Cypher statement
        """
        with self.driver.session(database=self.db if db is None else db) as session:
            result = session.run(query, data).data() if data is not None\
                else session.run(query).data()
            self.log.debug(result)
        return result

    def run_multi_queries(self, queries: list, db: str = None) -> List[List[Dict]]:
        results = list()
        with self.driver.session(database=self.db if db is None else db) as session:
            for q in queries:
                self.log.debug(f"Query: {q}")
                result = session.run(q['query'], q['data']).data() if q['data'] is not None \
                    else session.run(q['query']).data()
                results.append(result)
                self.log.debug(f"Result: {result}")
        return results