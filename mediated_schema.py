from openai import OpenAI
from valentine.algorithms import Cupid, DistributionBased, JaccardDistanceMatcher
from valentine import valentine_match, valentine_match_batch
from utils.read_data import read_files
import pandas as pd

client = OpenAI(
    base_url="",
    api_key=""
)


def LLM():
    pass

def valentine(dir_path:str):
    # define matcher
    matcher = Cupid()

    dfs = read_files(dir_path)

    dataframes = list(map(lambda x: x[1], dfs))

    #matches = valentine_match(df1, df2, matcher)
    matches = valentine_match_batch(dataframes, dataframes, matcher)

    matches_list = [
        {
            "Table 1 Column": match[0][1],
            "Table 2 Column": match[1][1],
            "Similarity Score": score
        }
        for match, score in matches.items()
    ]

    # Crea un DataFrame da quella lista
    matches_df = pd.DataFrame(matches_list)

    print(matches_df)




if __name__ == "__main__":
    valentine("./data/raw")