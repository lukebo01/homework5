import pandas as pd
import os
import json
import time

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
            
            if extension in ["json", "jsonl"]:
                '''
                with open(os.path.join(dir_path, folder, f"{filename}.{extension}"), "r") as file:
                    for line in file:
                        data:dict = json.loads(line)
                        print(data.keys())
                '''
                df = pd.read_json(file_path, lines=True)
            
            if extension == "csv":
                df = pd.read_csv(file_path)
                pass
            
            if extension == "xls":
                df = pd.read_excel(file_path, engine="xlrd")



if __name__ == "__main__":
    read_files("./data/raw")
