import pandas as pd
import recordlinkage
import json
import re

# --- CONFIGURAZIONE E CARICAMENTO DATI ---
# Assumiamo di avere un file CSV "final_mediated_schema.csv" con almeno le colonne "company_name", "industry", "_source_table"
df = pd.read_csv("main_outputs/final_mediated_schema.csv")

# Assicuriamoci che le colonne usate esistano, altrimenti adattare i nomi in base al CSV
required_columns = ["company_name", "industry", "_source_table"]
for col in required_columns:
    if col not in df.columns:
        raise ValueError(f"Il file CSV deve contenere la colonna '{col}'.")

# Funzione di normalizzazione per il company name:
def normalize_string(s):
    # Rimuove punteggiatura, spazi in eccesso e trasforma in lowercase
    s = str(s).lower()
    s = re.sub(r'[^a-z0-9]', '', s)
    return s

def safe_get(record, key):
    value = record.get(key, "")
    if pd.isnull(value):
        return ""
    return value

# Aggiungiamo una colonna "company_name_norm" per il blocking
df["company_name_norm"] = df["company_name"].apply(normalize_string)

# Per la strategia B: creiamo una chiave combinata: industry normalizzata + "_" + prime 2 lettere di company_name_norm
df["industry_norm"] = df["industry"].str.lower().str.strip()
df["blocking_key"] = df["industry_norm"] + "_" + df["company_name_norm"].str[:2]

# --- STRATEGIA A: BLOCKING SUL CAMPO company_name_norm ---

# Utilizziamo lo sorted neighbourhood (o un semplice blocco) sul campo company_name_norm
indexer_A = recordlinkage.Index()
# Qui si blocca semplicemente per company_name_norm (in alternativa, si può usare il metodo "sortedneighbourhood" con una finestra)
indexer_A.block('company_name_norm')
candidate_links_A = indexer_A.index(df)

print(f"Strategia A - Numero di coppie candidate: {len(candidate_links_A)}")

# --- STRATEGIA B: BLOCKING BASATO SU blocking_key ---
indexer_B = recordlinkage.Index()
indexer_B.block('blocking_key')
candidate_links_B = indexer_B.index(df)

print(f"Strategia B - Numero di coppie candidate: {len(candidate_links_B)}")

# --- FUNZIONE PER GENERARE IL JSON DEI CANDIDATE PAIRS ---
def generate_candidate_pairs_json(candidate_index, df, output_file):
    """
    Per ogni coppia candidate (tuple di indici), genera un dizionario con la struttura:
    {
        "record1": {"company_name": <nome_record1>, "industry": <industry_record1>, "_source_table": <fonte_record1>},
        "record2": {"company_name": <nome_record2>, "industry": <industry_record2>, "_source_table": <fonte_record2>}
    }
    e salva la lista in un file JSON.
    """
    candidate_pairs = []
    for idx1, idx2 in candidate_index:
        rec1 = df.loc[idx1]
        rec2 = df.loc[idx2]
        pair = {
            "record1": {
                "company_name": safe_get(rec1, "company_name"),
                "industry": safe_get(rec1, "industry"),
                "other_attributes": {
                    "headquarters_address": safe_get(rec1, "headquarters_address"),
                    "headquarters_city": safe_get(rec1, "headquarters_city"),
                    "headquarters_country": safe_get(rec1, "headquarters_country"),
                    "year_founded": safe_get(rec1, "year_founded"),
                    "ownership": safe_get(rec1, "ownership"),
                    "company_number": safe_get(rec1, "company_number"),
                    "employee_count": safe_get(rec1, "employee_count"),
                    "market_cap_usd": safe_get(rec1, "market_cap_usd"),
                    "total_revenue_usd": safe_get(rec1, "total_revenue_usd"),
                    "net_profit_usd": safe_get(rec1, "net_profit_usd"),
                    "total_assets_usd": safe_get(rec1, "total_assets_usd"),
                    "company_website": safe_get(rec1, "company_website"),
                    "social_media_links": safe_get(rec1, "social_media_links"),
                    "representative_name": safe_get(rec1, "representative_name"),
                    "total_raised": safe_get(rec1, "total_raised"),
                    "company_description": safe_get(rec1, "company_description"),
                    "company_stage": safe_get(rec1, "company_stage"),
                    "share_price": safe_get(rec1, "share_price"),
                    "legal_form": safe_get(rec1, "legal_form")
                },
                "_source_table": safe_get(rec1, "_source_table")
            },
            "record2": {
                "company_name": safe_get(rec2, "company_name"),
                "industry": safe_get(rec2, "industry"),
                "other_attributes": {
                    "headquarters_address": safe_get(rec1, "headquarters_address"),
                    "headquarters_city": safe_get(rec1, "headquarters_city"),
                    "headquarters_country": safe_get(rec1, "headquarters_country"),
                    "year_founded": safe_get(rec1, "year_founded"),
                    "ownership": safe_get(rec1, "ownership"),
                    "company_number": safe_get(rec1, "company_number"),
                    "employee_count": safe_get(rec1, "employee_count"),
                    "market_cap_usd": safe_get(rec1, "market_cap_usd"),
                    "total_revenue_usd": safe_get(rec1, "total_revenue_usd"),
                    "net_profit_usd": safe_get(rec1, "net_profit_usd"),
                    "total_assets_usd": safe_get(rec1, "total_assets_usd"),
                    "company_website": safe_get(rec1, "company_website"),
                    "social_media_links": safe_get(rec1, "social_media_links"),
                    "representative_name": safe_get(rec1, "representative_name"),
                    "total_raised": safe_get(rec1, "total_raised"),
                    "company_description": safe_get(rec1, "company_description"),
                    "company_stage": safe_get(rec1, "company_stage"),
                    "share_price": safe_get(rec1, "share_price"),
                    "legal_form": safe_get(rec1, "legal_form")
                },
                "_source_table": safe_get(rec2, "_source_table")
            }
        }
        candidate_pairs.append(pair)
    output = {"candidate_pairs": candidate_pairs}
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=4, ensure_ascii=False)
    print(f"File JSON salvato: {output_file}")


# --- GENERAZIONE DEI FILE JSON ---
generate_candidate_pairs_json(candidate_links_A, df, "main_outputs/candidate_pairs_strategy_A.json")
generate_candidate_pairs_json(candidate_links_B, df, "main_outputs/candidate_pairs_strategy_B.json")

# --- NOTA SULL'INPUT PER PAIRWISE MATCHING ---
# Sì, tipicamente l'output del blocking (cioè l'insieme dei candidate pairs) è l'input per la fase di pairwise matching.
# In questa fase, verranno calcolate le similarità tra i record di ciascuna coppia candidate, e il risultato (ad esempio, un punteggio o una classificazione match/non-match)
# verrà utilizzato per definire se due record corrispondono alla stessa entità.

print("Procedura di blocking completata. I file JSON generati possono essere usati come input per il pairwise matching.")
