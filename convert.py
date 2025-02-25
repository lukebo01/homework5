import pandas as pd
import os


df = pd.DataFrame(columns=["company_name_left", "industry_left", "headquarters_country_left", "company_name_right", "industry_left", "headquarters_country_right"])
result = list(map(lambda row: row.split("\n")[0],open("./result.txt", "r").readlines()))

for i, row in enumerate(result):
    a, b, sim = tuple(row.split(" \t "))
    #company_a, industry_a, country_a = a.split(" | ")
    #company_b, industry_b, country_b = b.split(" | ")
    #df.loc[len(df)] = [company_a, industry_a, country_a, company_b, industry_b, country_b]
    df.loc[len(df)] = a.split(" | ") + b.split(" | ")
    print(i)

df.to_csv("result.csv")