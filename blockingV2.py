import pandas as pd
import recordlinkage
import jellyfish
import json

# Caricamento dei dati
df = pd.read_csv("main_outputs/final_mediated_schema.csv", low_memory=False)

# Assicurati che il nome dell'azienda sia stringa
df["company_name"] = df["company_name"].astype(str)

# Creazione di un identificativo unico per ogni record
df["index"] = df.index

# Strategie di Blocking
indexer = recordlinkage.Index()

# Blocking 1: Basato sulle prime 3 lettere del nome dell'azienda
df["prefix_3"] = df["company_name"].str[:3]
indexer.block(left_on="prefix_3")

# Blocking 2: Basato su Soundex (usando jellyfish)
df["soundex"] = df["company_name"].apply(lambda x: jellyfish.soundex(x))
indexer.block(left_on="soundex")

# Creazione dei candidati per il matching
candidate_links_1 = indexer.index(df)
candidate_links_2 = indexer.index(df)

# Funzione per trasformare gli ID in valori leggibili
def convert_pairs_to_json(candidate_links, filename):
    pairs_list = []
    
    for id1, id2 in candidate_links:
        row1 = df.loc[id1]
        row2 = df.loc[id2]
        
        pairs_list.append({
            "company_1": row1["company_name"],
            "company_2": row2["company_name"],
            "source_1": row1["_source_table"] if "_source_table" in df.columns else None,
            "source_2": row2["_source_table"] if "_source_table" in df.columns else None
        })
    
    # Salva in JSON
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(pairs_list, f, indent=4, ensure_ascii=False)

# Salvare i risultati del blocking come JSON leggibile
convert_pairs_to_json(candidate_links_1, "blocking_output_1.json")
convert_pairs_to_json(candidate_links_2, "blocking_output_2.json")

print("Blocking completato e salvato in JSON!")
