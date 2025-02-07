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

def create_data(dir_path:str, output_path:str ,instance_per_table:int) -> None:
    files = read_files(dir_path)

    with open("./data/processed.txt", "w") as file:
        for name, dataframe in files:
            file.write(f"Dataframe: {name}\n")
            file.write(dataframe.head(instance_per_table).to_string(index=False))
            file.write("\n" + "#" * 100 + "\n")

def LLM():
    create_data("./data/raw", "./data/processed,txt", 20)


if __name__ == "__main__":
    LLM()