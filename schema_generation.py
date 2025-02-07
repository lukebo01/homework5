from openai import OpenAI
from utils.read_data import read_files
import pandas as pd
import json


keys = json.load(open("./keys.json", "r"))


def get_client(model:str) -> OpenAI:
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

def send_message(input_file, model: str):
    client:OpenAI = get_client(model)

    completion = client.chat.completions.create(
        model = keys[model]["model"],
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Create a mediated schema with at least 20 attributes. " + 
                                "Return only the mediated schema in this format: a json file " +
                                "where for every column of the mediated schema is associated a list " +
                                "where this list contains the attributes of the original tables. " +
                                "This are all the tables that I have: \n" 
                    }
                ]
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": f"{input_file}"
                    }
                ]
            }
        ]
    )

    return completion.choices[0].message.content

def LLM():
    # creates response for LLM
    create_data("./data/raw", "./data/processed.txt", 10)

    # read response
    processed = list(map(lambda x: x.strip("\n"),open("./data/processed.txt", "r").readlines()))

    response = send_message(processed, "deep-seek-1.5B")
    

if __name__ == "__main__":
    LLM()