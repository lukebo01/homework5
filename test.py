from openai import OpenAI
import pandas as pd
import os
import json

keys = json.load(open("keys.json", "r"))
model = "deep-seek-32B"

client = OpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=keys[model]["api-key"]
)


def read_files(dir_path:str) -> None:
    '''
    Reads all data and make a summary to pass
    '''
    raw_dir:list[str] = sorted(os.listdir(dir_path))

    dataframes = []

    for folder in raw_dir:
        folder_dir:list[str] = sorted(os.listdir(os.path.join(dir_path, folder)))
        for data_name in folder_dir:
            # get the filename and extension
            split_name = data_name.split(".")
            filename, extension = "", ""

            if len(split_name) > 2:
                filename = ".".join(split_name[:len(filename) - 1])
                extension = split_name[-1]
            else:
                filename = split_name[0]
                extension = split_name[1]

            
            file_path = os.path.join(dir_path, folder, f"{filename}.{extension}")
            df: pd.DataFrame = None
            if extension in ["json", "jsonl"]:
                df = pd.read_json(file_path, lines=True)
            
            if extension == "csv":
                df = pd.read_csv(file_path)
            
            if extension == "xls":
                df = pd.read_excel(file_path, engine="xlrd")

            df = df.drop(columns=["Unnamed: 0"], errors='ignore')

            dataframes.append((filename, df))

            #current_attributes = df.columns.to_list()
            #print(current_attributes)

        with open("./data/processed.txt", "w") as file:
            for _, (name, dataframe) in enumerate(dataframes):
                file.write(f"Dataframe: {name}\n")
                file.write(dataframe.head(40).to_string(index=False))
                file.write("\n" + "#" * 100 + "\n")

# merge di prima di azienda e poi di persone
def merge_ariregister():
    special_dir = f"./data/raw/ariregister.rik.ee"
    df_activity = pd.read_csv(os.path.join(special_dir, "wissel-activity-ariregister.rik.ee.csv"))
    df_companies = pd.read_csv(os.path.join(special_dir, "wissel-aziende-ariregister.rik.ee.csv"))
    #df_partners = pd.read_csv(os.path.join(special_dir, "wissel-partners-ariregister.rik.ee.csv"))
    df_representatives = pd.read_csv(os.path.join(special_dir, "wissel-rappresentanti-ariregister.rik.ee.csv"))
 
    df_companies = df_companies.rename(columns={"ID": "ID azienda"})
    df_representatives = df_representatives.rename(columns={
        "Name": "Representative Name",
        "Code": "Representative Code",
        "Role": "Representative Role",
        "Start Date": "Representative Start Date"
    })

    df_activity_companies = (
        df_activity
        .merge(df_companies, on=["ID azienda"], how="outer")
        .merge(df_representatives, on=["ID azienda"], how="outer")
    )
    df_activity_companies.to_csv("wissel-ariregister.csv", index=False)

def get_mediated_schema(processed_path: str):
    processed = " ".join(open(processed_path, "r").readlines())

    completion = client.chat.completions.create(
        model=keys[model]["model"],
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": "Create a mediated schema, for every column of the mediated schema tell me what is" +  
                                "the corresponding column of the original tables, return only the mediated schema." + 
                                "I am going to give you the first 40 elements of every table that I have so you can have more information" +  
                                f"This are all the tables that I have:\n{processed}" + 
                                "Return the mediated schema in this format: a list of object that has 2 keys:" + 
                                "first key is the column name and the second key are the corresponding original columns." + 
                                "The format of the second value should a list where elements are represented like this: 'table_name: column_name'"
                    }
                ]
            }
        ]
    )
    print(completion)

    return completion.choices[0].message.content

def convert_response_to_json(path_to_response:str):
    pass

def populate_mediated_schema(tables_path: str, response_path:str):
    raw_dir:list[str] = sorted(os.listdir(tables_path))

    attributes = []

    for folder in raw_dir:
        folder_dir:list[str] = sorted(os.listdir(os.path.join(tables_path, folder)))
        for data_name in folder_dir:
            split_name = data_name.split(".")
            filename, extension = "", ""
            if len(split_name) > 2:
                filename = ".".join(split_name[:len(filename) - 1])
                extension = split_name[-1]
            else:
                filename = split_name[0]
                extension = split_name[1]
            file_path = os.path.join(tables_path, folder, f"{filename}.{extension}")
            df: pd.DataFrame = None
            if extension in ["json", "jsonl"]:
                df = pd.read_json(file_path, lines=True)
            if extension == "csv":
                df = pd.read_csv(file_path)
            if extension == "xls":
                df = pd.read_excel(file_path, engine="xlrd")
            df = df.drop(columns=["Unnamed: 0"], errors='ignore')

            current_attributes = list(map(lambda x: f"{filename}.{x}", df.columns.to_list()))
            print(current_attributes)
            #attributes.append(current_attributes)
            attributes += current_attributes

    #print(attributes)
    #print(len(attributes))

    mediated_schema:dict = json.load(open(response_path))["Mediated-Schema"]
    
    count = 0
    for mediated_column in mediated_schema:
        original_columns:str = mediated_column["original_columns"]

        for column in original_columns:
            split_name = column.split(".")
            #print(len(split_name))
        #print(original_columns)
        
    print(count)

if __name__ == "__main__":
    #merge_ariregister()
    #read_files("./data/raw")

    #response = get_mediated_schema("./data/processed.txt")
    #with open("./data/response.txt", "w") as file:
    #    file.write(response)

    populate_mediated_schema("./data/raw", "./mediated_schema.json")