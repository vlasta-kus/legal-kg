import os
import json
from typing import List, Dict
from src.utils import LoggingHandler


class DataLoader(LoggingHandler):
    def __init__(self, data_dir: str):
        super().__init__(self)
        self.data_dir = data_dir

    @classmethod
    def from_path(cls, path: str):
        if not os.path.exists(path) or not os.path.isdir(path):
            return None
        return cls(path)

    @staticmethod
    def read_json(path: str) -> List[Dict]:
        IGNORE_KEYS = ['words']
        with open(path, 'r') as f:
            content = json.load(f)

        # drop irrelevant (too verbose) keys
        content2 = list()
        for x in content:
            for k in IGNORE_KEYS:
                if k in x:
                    x.pop(k)
            content2.append(x)

        return content2

    def crawl_and_identify(self) -> dict:
        """
        Crawl directory structure and identify all relevant JSON files, return then with their full path.
        :param path: root directory location
        :return: list of JSON files with metadata
        """
        self.log.debug("Crawling directory structure.")
        jsons, dirs = list(), list()

        for root, dirs, files in os.walk(self.data_dir):
            #self.log.info(f"{root} - {dirs} - {files}")
            jsons += [os.path.join(root, f) for f in files if f.endswith(".json")]

        jsons = [{
            'name': x.split("/")[-1],
            'path': x,
            'directory': "/".join(x.split("/")[:-1])
        } for x in jsons]

        dirs = set()
        for f in jsons:
            sp = f['directory'].split("/")
            while len(sp) > 1:
                dirs.add("/".join(sp[:-1]) + "-->" + "/".join(sp))
                sp = sp[:-1]
        dirs = [{'source': x.split("-->")[0], 'target': x.split("-->")[1]} for x in dirs]

        return {'files': jsons, 'directory_pairs': dirs}