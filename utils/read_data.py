import pandas as pd
import os

def read_files(dir_path:str) -> list[tuple[str, pd.DataFrame]]:
    '''
    Reads all data and return: list(filename, pandas.Dataframe)
    '''
    raw_dir:list[str] = sorted(os.listdir(dir_path))

    dataframes = []

    for file in raw_dir:
            # get the filename and extension
        split_name = file.split(".")
        filename = split_name[0]
        extension = split_name[1]
            
        file_path = os.path.join(dir_path, f"{filename}.{extension}")
        df: pd.DataFrame = None
        if extension in ["json", "jsonl"]:
            df = pd.read_json(file_path, lines=True)
        
        if extension == "csv":
            df = pd.read_csv(file_path)
        
        if extension == "xls":
            df = pd.read_excel(file_path, engine="xlrd")

        df = df.drop(columns=["Unnamed: 0"], errors='ignore')

        dataframes.append((filename, df))

    return dataframes