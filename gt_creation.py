import pandas as pd
from utils.read_data import read_files
from difflib import SequenceMatcher
import json


def create_raw_file():
    print("creating raw file")
    final_schema = pd.read_csv("main_outputs/final_mediated_schema.csv", low_memory=False)
    reduced: pd.DataFrame = final_schema[["company_name", "_source_table"]].sort_values(by="company_name")
    reduced.to_csv("final.csv")

    files: list[str, pd.DataFrame] = read_files("./data/raw")
    for name, df in files:
        df.to_json(f"data_json/{name}.json", orient="records", lines=True)


def similar(a, b) -> float:
    return SequenceMatcher(None, a, b).ratio()


def create_initial_gt():
    print("create initial ground truth")
    df = pd.read_csv("final.csv")

    # Crea un dizionario per la mappatura
    ground_truth = []

    # Itera sulla colonna B per trovare nomi simili
    counter = 0
    for i, _ in enumerate(df['company_name']):
        a:pd.Series = df.loc[i]
        b:pd.Series = df.loc[i + 1]

        a_name = str(a["company_name"])
        b_name = str(b["company_name"])

        source_a = str(a['_source_table'])
        source_b = str(b['_source_table'])

        similarity = similar(a_name,b_name)
        if (similarity > 0.6 and similarity < 0.7) and (source_a != source_b):
            if counter % 12 == 0:
                ground_truth.append({
                    f"{a_name}":f"{a['_source_table']}",
                    f"{b_name}":f"{b['_source_table']}"
                })
            counter += 1
        if counter == 2500:
            break

    result = {}
    result["ground_truth"] = ground_truth

    print(f"ground truth has {len(ground_truth)} instances")

    with open("./data/ground_truth.json", "w") as file:
        json.dump(result, file, indent=4)


if __name__ == "__main__":
    #create_raw_file()
    create_initial_gt()