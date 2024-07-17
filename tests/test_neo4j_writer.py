import unittest
import logging
from src.neo4jWriter import Neo4jWriter

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s: %(message)s")


class TestNeo4jWriter(unittest.TestCase):
    def test_simple_query(self):
        neo = Neo4jWriter('neo4j')
        res = neo.run_simple_query("MATCH (n) RETURN COUNT(*) AS count")
        self.assertTrue(len(res) == 1 and 'count' in res[0], "Simple query did not return expected output format.")
        res = neo.run_simple_query("MATCH (n) WHERE n.name = $name RETURN COUNT(*) AS count", {'name': "Mary"})
        self.assertTrue(len(res) == 1 and 'count' in res[0], "Simple query with payload failed.")
        neo.close()

    def test_multi_query(self):
        neo = Neo4jWriter('neo4j')
        queries = [{'query': "MATCH (n) WHERE n.name = $name RETURN COUNT(*) AS count", 'data': {'name': "Johnny"}},
                   {'query': "MATCH (n) RETURN COUNT(*) AS count", 'data': None}
                   ]
        res = neo.run_multi_queries(queries)
        neo.close()
        print(res)
        self.assertTrue(len(res) == 2, f"Unexpected number of outputs: {len(res)}")


if __name__ == '__main__':
    unittest.main()
