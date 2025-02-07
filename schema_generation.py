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

    with open(output_path, "w") as file:
        for name, dataframe in files:
            df: pd.DataFrame = dataframe
            file.write(f"Dataframe: {name}\n")
            file.write(df.head(instance_per_table).to_string(index=False))
            file.write("\n" + "#" * 100 + "\n")

def LLM():
    create_data("./data/raw", "./data/processed.txt", 10)


if __name__ == "__main__":
    LLM()