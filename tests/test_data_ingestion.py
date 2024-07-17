import unittest
import logging
from src.dataLoader import DataLoader

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s: %(message)s")


class TestDataIngestion(unittest.TestCase):
    def test_path_crawling(self):
        loader = DataLoader.from_path("data")
        self.assertIsNotNone(loader)
        content = loader.crawl_and_identify()
        self.assertTrue(len(content['files']) > 0)
        self.assertTrue(len(content['directory_pairs']) > 0)
        print(len(content['files']))
        print(content['files'][:10])
        #print(len(content['directory_pairs']))

    def test_read_json_file(self):
        #file = "data/Sample Data Set for MC+A-20210708T192749Z-001/Sample Data Set for MC+A/Collected Documents/Chambers Works/DuPont-Chemours Spill Act Directive 8-30-17.json"
        file = "data/Sample Data Set for MC+A-20210708T192749Z-001/Sample Data Set for MC+A/Collected Documents/Pompton Lakes Works/ABD Proposed Permit Modification.json"
        content = DataLoader.read_json(file)
        self.assertTrue(len(content), "No content found!")
        self.assertTrue('text' in content[0], "No `text` key found.")
        print(content[0].keys())
        print(content[0])


if __name__ == '__main__':
    unittest.main()
