from sklearn.model_selection import train_test_split
import pandas as pd
import json
import time


def normalize_value(value):
    if isinstance(value, list):
        value = "|".join(list(map(lambda x: f"'{x}'",value)))
    if value == None:
        value = "Unknown"
    return value


def convert_to_format(pair: dict) -> str:
    print(pair)
    name = pair["company-name"]
    industry = normalize_value(pair["industry"])
    country = normalize_value(pair["country"])

    return f"COL company_name VAL {name} COL company_sector VAL {industry} COL company_country VAL {country}"


def pd_to_txt(df: pd.DataFrame, name: str):
    file = open(f"{name}.txt", "w")
    for index, row in df.iterrows():
        pair1, pair2 = tuple(row["pairs"])
        label: bool = row["match"]

        file.write(f"{convert_to_format(pair1)}\t{convert_to_format(pair2)}\t{int(label)}\n")

    file.close()


file:dict = json.load(open("gt.json", "r"))
ground_truth: list = file["ground_truth"]

df = pd.DataFrame(ground_truth)

true_df = df[df["match"] == True]
false_df = df[df["match"] == False]

min_size = min(len(true_df), len(false_df))
true_df = true_df.sample(min_size, random_state=42)
false_df = false_df.sample(min_size, random_state=42)

balanced_df = pd.concat([true_df, false_df]).sample(frac=1, random_state=42)  # Shuffle

# Suddividere in train (60%), val (20%) e test (20%)
train_df, temp_df = train_test_split(balanced_df, test_size=0.4, random_state=42, stratify=balanced_df["match"])
val_df, test_df = train_test_split(temp_df, test_size=0.5, random_state=42, stratify=temp_df["match"])

# Risultati
print("Train size:", len(train_df))
print("Validation size:", len(val_df))
print("Test size:", len(test_df))

pd_to_txt(train_df, "ditto_train")
pd_to_txt(val_df, "ditto_val")
pd_to_txt(test_df, "ditto_test")
