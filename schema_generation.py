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
            file.write(df.head(instance_per_table).to_csv(index=False, header=True))
            file.write("\n" + "#" * 100 + "\n")

def send_message(query_file:str, input_file, model: str, save_type:str) -> dict:
    client:OpenAI = get_client(model)

    completion = client.chat.completions.create(
        model = keys[model]["model"],
        messages=[
            {"role": "user","content": [{"type": "text","text": query_file}]},
            {"role": "user","content": [{"type": "text","text": f"{input_file}"}]}
        ]
    )

    response = completion.choices[0].message.content

    if save_type == "json":
        with open("./data/response/response.json", "w") as file:
            json_response = json.loads(response.strip("```json").strip("```"))
            json.dump(json_response, file, indent=4)
    
    if save_type == "txt":
        with open("./data/response/response.txt", "w") as file:
            file.write(response)

def LLM():
    # creates response for LLM
    create_data("./data/raw", "./data/processed.txt", 10)

    # reads processed.txt created with `create_data()` and read query_file
    processed = list(map(lambda x: x.strip("\n"),open("./data/processed.txt", "r").readlines()))
    query_file = " ".join(open("./data/query/query1.txt", "r").readlines()).strip("\n")
    # ask LLM the mediated schema
    send_message(query_file,
                 processed, 
                 "1", 
                 "txt")

def schema_population():
    # read mediated schema info
    response:dict = json.load(open("./data/response/response.json", "r"))
    mediated_attributes = list(response.keys())

    # create mediated schema
    df = pd.DataFrame(columns=mediated_attributes)

    # read original tables and convert it to dict
    files: list[str, pd.DataFrame] = read_files("./data/raw")

if __name__ == "__main__":
    LLM()
    #schema_population()

    #response:dict = json.load(open("./data/response.txt", "r"))
    #print(response.keys())