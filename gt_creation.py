import pandas as pd
from utils.read_data import read_files
from difflib import SequenceMatcher
import json
import ast


def create_raw_file():
    print("creating raw file")
    final_schema = pd.read_csv("main_outputs/final_mediated_schema.csv", low_memory=False)
    reduced: pd.DataFrame = final_schema[["company_name", "industry", "headquarters_country", "_source_table"]].sort_values(by="company_name")
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
        a_industry = str(a["industry"])
        a_country = str(a["headquarters_country"])
        a_source = str(a['_source_table'])

        b_name = str(b["company_name"])
        b_industry = str(b["industry"])
        b_country = str(b["headquarters_country"])
        b_source = str(b['_source_table'])

        # control type
        if a_country == "nan":
            a_country = None
        if b_country == "nan":
            b_country = None

        if a_industry == "nan":
            a_industry = None
        elif a_industry[0] == '[':
            a_industry = ast.literal_eval(a_industry)
        if b_industry == "nan":
            b_industry = None
        elif b_industry[0] == '[':
            b_industry = ast.literal_eval(b_industry)

        similarity = similar(a_name,b_name)
        if (similarity > 0.6 and similarity < 0.7) and (a_source != b_source):
            if counter % 12 == 0:
                ground_truth.append(dict(
                    pairs = [
                        {
                            "company-name": a_name,
                            "industry": a_industry,
                            "country": a_country,
                            "source": a_source
                        },
                        {
                            "company-name": b_name,
                            "industry": b_industry,
                            "country": b_country,
                            "source": b_source
                        }
                    ],
                    match = True
                ))
                '''
                ground_truth.append({
                    f"{a_name}":f"{a['_source_table']}",
                    f"{b_name}":f"{b['_source_table']}",
                    "matching": True
                })
                '''
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