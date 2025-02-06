from openai import OpenAI
from utils.read_data import read_files
import pandas as pd
import json


keys = json.load(open("./keys.json", "r"))


def client(model:str) -> OpenAI:
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=keys[model]["api-key"]
    )


def LLM():
    print(type(client("deep-seek-1.5B")))


if __name__ == "__main__":
    LLM()