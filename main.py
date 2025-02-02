import pandas as pd
import os


def read_files(dir_path:str) -> None:
    raw_dir:list[str] = sorted(os.listdir(dir_path))

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

            current_attributes = df.columns.to_list()
            print(current_attributes)

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

if __name__ == "__main__":
    #merge_ariregister()
    read_files("./data/raw")
