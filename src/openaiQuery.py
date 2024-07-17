import os
import json
import time
from typing import List, Dict
from openai import OpenAI
from src.utils import LoggingHandler


class OpenAIQuery(LoggingHandler):
    def __init__(self, prompt_path: str, prompt_version: str, max_tokens: int):
        super().__init__(self)
        self.prompt_path = prompt_path
        self.prompt_version = prompt_version
        self.max_tokens = max_tokens
        self.gpt_client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

        if not os.path.exists(prompt_path):
            self.log.error(f"Path {prompt_path} does not exist.")
        if not os.path.isdir(prompt_path):
            self.log.error(f"Path {prompt_path} should be a directory.")
        self.messages = self.build_prompt(prompt_path, prompt_version)

    def query(self, query: str, model: str) -> dict:
        prompt = [{"role": "system", "content": self.messages['task']},
                  {"role": "user", "content": self.messages['example']},
                  {"role": "assistant", "content": self.messages['example_output']},
                  {"role": "user", "content": query}
                  ]

        self.log.debug(f"Calling model {model} ...")

        t_start = time.time()
        try:
            response = self.gpt_client.chat.completions.create(model=model, messages=prompt,
                                                               temperature=0., max_tokens=self.max_tokens
                                                               )
            self.log.debug(response)
        except Exception as e:
            self.log.error(f"OpenAI API call failed: {e}\nQuery: {query}")
            return {"error": str(e), "response": str(response)}
        # print(response.choices[0].message.content)
        self.log.debug(f"Query time: {round(time.time() - t_start, 1)} sec\n")

        # string -> json
        try:
            output = json.loads(response.choices[0].message.content)
        except Exception as e:
            self.log.error(f"FAILED to parse GPT output.\n{e}\n{response}")
            return {"error": f"GPT output parsing (str -> JSON) failed with: {str(e)}", "response": str(response)}
        self.pretty_print_rels(output)

        return output

    @staticmethod
    def build_prompt(path: str, prefix: str):
        messages = dict()
        with open(os.path.join(path, prefix + ".txt"), 'r') as f:
            messages['task'] = f.read()
        with open(os.path.join(path, prefix + "_example.txt"), 'r') as f:
            messages['example'] = f.read()
        with open(os.path.join(path, prefix + "_example_output.txt"), 'r') as f:
            messages['example_output'] = f.read()
        return messages

    @staticmethod
    def pretty_print_rels(output: dict):
        print("--- Entities ---")
        for e in output['entities'].keys():
            print(f"{e}: {[x['name'] for x in output['entities'][e]]}")
        print("\n--- Relations ---")
        for r in output['relations'].keys():
            for x in output['relations'][r]:
                sources = [e['name'] for arr in output['entities'].values() for e in arr if e['id'] == x['source']]
                source = sources[0] if len(sources) > 0 else None
                targets = [e['name'] for arr in output['entities'].values() for e in arr if e['id'] == x['target']]
                target = targets[0] if len(targets) > 0 else None
                x2 = {key: val for key, val in x.items() if key not in ['source', 'target']}
                print(f"{source} - {r.upper()} ({x2}) -> {target}")