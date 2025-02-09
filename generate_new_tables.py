import os
import pandas as pd
import re

##########################################
# 1) FUNZIONE PER LEGGERE I FILE RAW
##########################################
def read_files(dir_path: str) -> list[tuple[str, pd.DataFrame]]:
    """
    Legge tutti i file presenti nella cartella dir_path e restituisce
    una lista di tuple (filename, DataFrame).
    Si assume che il nome del file sia del tipo <table_name>.<estensione>.
    Formati supportati: CSV, JSON, JSONL, XLS.
    """
    raw_dir = sorted(os.listdir(dir_path))
    dataframes = []
    for file in raw_dir:
        split_name = file.split(".")
        if len(split_name) < 2:
            continue  # Salta file senza estensione
        filename = split_name[0]
        extension = split_name[1].lower()
            
        file_path = os.path.join(dir_path, file)
        df: pd.DataFrame = None
        # Prova a leggere i formati riconosciuti
        try:
            if extension in ["json", "jsonl"]:
                df = pd.read_json(file_path, lines=True)
            elif extension == "csv":
                df = pd.read_csv(file_path)
            elif extension == "xls":
                df = pd.read_excel(file_path, engine="xlrd")
            else:
                # Formato non riconosciuto, si ignora
                continue
        except ValueError as e:
            print(f"Impossibile caricare {file_path}: {e}")
            continue

        if df is not None:
            # Elimina la colonna "Unnamed: 0" se presente
            df = df.drop(columns=["Unnamed: 0"], errors='ignore')
            dataframes.append((filename, df))
    
    return dataframes


##########################################
# 2) FUNZIONI DI SUPPORTO PER IL MAPPING
##########################################
def parse_source_attr(source_attr: str) -> (str, str):
    """
    Data una stringa tipo "TableName.ColumnName", restituisce (table_name, column_name).
    Se ci sono più punti, il primo è la tabella, il resto è il nome della colonna.
    """
    parts = source_attr.split('.')
    if len(parts) >= 2:
        table = parts[0]
        col = '.'.join(parts[1:])
        return table, col
    else:
        return None, None


def get_transformed_value(table_name: str, row: pd.Series, mapping_entry) -> any:
    """
    Estrae e trasforma il valore della riga 'row' di tabella 'table_name' in base al mapping_entry.
    
    Il mapping_entry può contenere:
      - "sources":  [... elenco di colonne "Tabella.Colonna" ...]
      - "relation": "one-to-one", "many-to-one", "one-to-many" (default "one-to-one")
      - "split_delimiter", "merge_delimiter", "take_first", "take_index", ...
      - "table_rules":  { "AmbitionBox": {...}, "DDD-teamblind-com": {...} }
         => regole specifiche per una certa tabella di provenienza

    Se esiste una regola specifica in "table_rules" per table_name, sovrascrive i parametri
    di "relation", "split_delimiter", ecc. di default.
    """

    # Se mapping_entry è una lista => default one-to-one
    if isinstance(mapping_entry, list):
        sources = mapping_entry
        relation = "one-to-one"
        merge_delimiter = " "
        split_delimiter = None
        take_first = False
        take_index = None
        table_rules = {}
    elif isinstance(mapping_entry, dict):
        # Parametri di default
        sources = mapping_entry.get("sources", [])
        relation = mapping_entry.get("relation", "one-to-one")
        merge_delimiter = mapping_entry.get("merge_delimiter", " ")
        split_delimiter = mapping_entry.get("split_delimiter", None)
        take_first = mapping_entry.get("take_first", False)
        take_index = mapping_entry.get("take_index", None)
        table_rules = mapping_entry.get("table_rules", {})
    else:
        return None  # tipo di mapping non riconosciuto

    # Se esiste una regola specifica per la tabella "table_name" in table_rules,
    # sovrascrive i parametri con quelli definiti lì.
    if table_name in table_rules:
        rule = table_rules[table_name]
        relation = rule.get("relation", relation)
        merge_delimiter = rule.get("merge_delimiter", merge_delimiter)
        split_delimiter = rule.get("split_delimiter", split_delimiter)
        take_first = rule.get("take_first", take_first)
        take_index = rule.get("take_index", take_index)

    # Raccoglie i valori corrispondenti
    values = []
    for src_attr in sources:
        src_tbl, src_col = parse_source_attr(src_attr)
        if src_tbl == table_name:
            # Trova la colonna corrispondente
            matching_cols = [c for c in row.index if c.strip() == src_col]
            if matching_cols:
                actual_col = matching_cols[0]
                val = row[actual_col]
                if isinstance(val, (list, tuple, pd.Series)):
                    if len(val) > 0:
                        values.append(str(val))
                else:
                    if pd.notna(val):
                        values.append(str(val))

    # Applica la logica in base alla relation
    if relation == "one-to-one":
        if len(values) == 0:
            return None
        elif len(values) == 1:
            return values[0]
        else:
            return merge_delimiter.join(values)

    elif relation == "many-to-one":
        if len(values) == 0:
            return None
        else:
            return merge_delimiter.join(values)

    elif relation == "one-to-many":
        if split_delimiter is None:
            # fallback => one-to-one
            if len(values) == 0:
                return None
            elif len(values) == 1:
                return values[0]
            else:
                return merge_delimiter.join(values)
        else:
            # Fa lo split
            split_list = []
            for v in values:
                # normalizziamo spazi multipli in uno singolo
                v_normalized = re.sub(r"\s+", " ", v).strip()
                tokens = [x.strip() for x in v_normalized.split(split_delimiter) if x.strip()]
                split_list.extend(tokens)
            
            # Se c'è take_index, prendo quell'elemento
            if isinstance(take_index, int):
                if 0 <= take_index < len(split_list):
                    return split_list[take_index]
                else:
                    return None
            # Se c'è take_first, prendo solo il primo
            if take_first and len(split_list) > 0:
                return split_list[0]
            return split_list
    else:
        # Fallback
        if len(values) == 0:
            return None
        return values[0]


def transform_table(table_name: str, df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Trasforma una tabella (df) in base al mapping:
    - Per ogni riga => crea un dizionario con le chiavi = attributi del mapping
      e i valori = get_transformed_value(...) in base alla definizione.
    """
    mediated_rows = []
    for _, row in df.iterrows():
        med_row = {}
        for mediated_col, mapping_entry in mapping.items():
            med_row[mediated_col] = get_transformed_value(table_name, row, mapping_entry)
        # Aggiunge info di debug
        med_row['_source_table'] = table_name
        mediated_rows.append(med_row)
    return pd.DataFrame(mediated_rows)


##########################################
# 3) IL MAPPING COMPLETO CON REGOLA SPECIALE
##########################################
mediated_mapping = {

    # 1) company_name
    "company_name": {
        "sources": [
            "AmbitionBox.Name",
            "DDD-cbinsight-com.name",
            "DDD-teamblind-com.name",
            "MalPatSaj-forbes-com.Name",
            "MalPatSaj-wikipedia-org.Name",
            "campaignindia.BRAND NAME",
            "companiesMarketCap_dataset.name",
            "company_social_urls.Company",
            "disfold-com.name",
            "ft-com.name",
            "hitHorizons_dataset.name",
            "output_globaldata.name",
            "output_govukbigsize.name",
            "valueToday_dataset.name",
            "wissel-ariregister.Name",
            "wissel-aziende-info-clipper-com.Name"
        ],
        "relation": "one-to-one",
        "table_rules": {
            "DDD-teamblind-com": {
                "relation": "one-to-one"
            },
            "MalPatSaj-forbes-com": {
                "relation": "one-to-one"
            }
            # Aggiungi altre tabelle se vuoi regole speciali,
            # altrimenti rimane "one-to-one" di default.
        }
    },

    # 2) industry
    "industry": {
        "sources": [
            "AmbitionBox.Industry",
            "DDD-cbinsight-com.industry",
            "DDD-teamblind-com.industry",
            "MalPatSaj-wikipedia-org.Industry",
            "hitHorizons_dataset.industry",
            "output_globaldata.industry",
            "ft-com.industry",
            "wissel-ariregister.Area of Activity"
        ],
        "relation": "one-to-one",
        "table_rules": {
            # Esempio: se voglio che "AmbitionBox" sia splitted su virgola (ipotesi)
            # "AmbitionBox": {
            #   "relation": "one-to-many",
            #   "split_delimiter": ","
            # }
        }
    },

    # 3) sector (many-to-one) + regole speciali per MalPatSaj-wikipedia-org
    "sector": {
        "sources": [
            "MalPatSaj-wikipedia-org.Sector"
        ],
        "relation": "many-to-one",
        "merge_delimiter": " ",
        "table_rules": {
            # Esempio: se "DDD-cbinsight-com" avesse un'altra colonna da unire
            # "DDD-cbinsight-com": {
            #    "sources": [ "DDD-cbinsight-com.mySector" ],
            #    "relation": "many-to-one"
            # }
        }
    },

    # 4) year_founded => es. di regola: di default one-to-one, ma per AmbitionBox => split su spazio
    "year_founded": {
        "sources": [
            "AmbitionBox.Foundation Year",
            "DDD-cbinsight-com.founded",
            "DDD-teamblind-com.founded",
            "MalPatSaj-wikipedia-org.Founded",
            "ft-com.founded",
            "hitHorizons_dataset.est_of_ownership",
            "output_globaldata.company_creation_date",
            "wissel-ariregister.Registration Date"
        ],
        "relation": "one-to-one",
        "table_rules": {
            "AmbitionBox": {
                "relation": "one-to-many",
                "split_delimiter": " ",
                "take_first": True
            },
            "DDD-teamblind-com": {
                # Esempio: se vuoi fare la stessa cosa per DDD-teamblind-com
                "relation": "one-to-many",
                "split_delimiter": " ",
                "take_first": True
            }
        }
    },

    # 6) ownership => many-to-one (es. unisce AmbitionBox.Ownership + hitHorizons_dataset.type)
    "ownership": {
        "sources": [
            "AmbitionBox.Ownership",
            "hitHorizons_dataset.type"
        ],
        "relation": "many-to-one",
        "merge_delimiter": " ",
        "table_rules": {
            # Esempio: se "disfold-com" ha un'altra colonna su "ownership"
            # "disfold-com": {
            #   "sources": [ "disfold-com.ownership_col" ],
            #   "relation": "many-to-one"
            # }
        }
    },

    # 7) headquarters_city => di base splitted su virgola,
    "headquarters_city": {
        "sources": [
            "DDD-cbinsight-com.city",
            "DDD-teamblind-com.locations",
            "disfold-com.headquarters",
            "valueToday_dataset.headquarters_region_city",
            "wissel-aziende-info-clipper-com.City"
        ],
        "relation": "one-to-many",
        "split_delimiter": ",",
        "table_rules": {
            # Se "AmbitionBox" fosse anche qui => "AmbitionBox": {...} 
        }
    },

    # 8) headquarters_country => splitted su virgola, stesse considerazioni
    "headquarters_country": {
        "sources": [
            "DDD-cbinsight-com.country",
            "MalPatSaj-forbes-com.Country",
            "disfold-com.headquarters",
            "ft-com.country",
            "hitHorizons_dataset.nation",
            "valueToday_dataset.headquarters_country",
            "companiesMarketCap_dataset.country",
            "wissel-aziende-info-clipper-com.Country"
        ],
        "relation": "one-to-many",
        "split_delimiter": ","
    },

    # 9) headquarters_full_address => one-to-one (per semplificare)
    "headquarters_full_address": {
        "sources": [
            "hitHorizons_dataset.address",
            "output_globaldata.address",
            "output_govukbigsize.registered_office_address",
            "wissel-ariregister.Address",
            "wissel-aziende-info-clipper-com.Address Name"
        ],
        "relation": "one-to-one"
    },

    # 10) company_website => one-to-one
    "company_website": {
        "sources": [
            "DDD-teamblind-com.website",
            "output_globaldata.website",
            "valueToday_dataset.company_website",
            "wissel-ariregister.URL",
            "wissel-aziende-info-clipper-com.URL"
        ],
        "relation": "one-to-one"
    },

    # 11) total_revenue_usd => one-to-one
    "total_revenue_usd": {
        "sources": [
            "MalPatSaj-forbes-com.Sales",
            "ft-com.revenue",
            "output_globaldata.revenue",
            "valueToday_dataset.annual_revenue_in_usd"
        ],
        "relation": "one-to-one"
    },

    # 12) net_profit_usd => one-to-one
    "net_profit_usd": {
        "sources": [
            "MalPatSaj-forbes-com.Profit",
            "valueToday_dataset.annual_net_income_in_usd"
        ],
        "relation": "one-to-one"
    },

    # 13) total_assets_usd => one-to-one
    "total_assets_usd": {
        "sources": [
            "MalPatSaj-forbes-com.Assets",
            "valueToday_dataset.total_assets_in_usd"
        ],
        "relation": "one-to-one"
    },

    # 14) employee_count => one-to-one
    "employee_count": {
        "sources": [
            "DDD-teamblind-com.size",
            "disfold-com.employees",
            "ft-com.employees",
            "output_globaldata.number_of_employees",
            "valueToday_dataset.number_of_employees"
        ],
        "relation": "one-to-one"
    },

    # 15) market_cap_usd => one-to-one
    "market_cap_usd": {
        "sources": [
            "MalPatSaj-forbes-com.Market Value",
            "companiesMarketCap_dataset.market_cap",
            "disfold-com.market_cap",
            "output_globaldata.market_cap"
        ],
        "relation": "one-to-one"
    },

    # 16) social_media_links => splitted su None => array di link
    "social_media_links": {
        "sources": [
            "company_social_urls.Facebook",
            "company_social_urls.Twitter",
            "company_social_urls.Instagram",
            "company_social_urls.Pinterest"
        ],
        "relation": "one-to-many",
        "split_delimiter": None
    },

    # 17) total_raised => splitted su None
    "total_raised": {
        "sources": [
            "DDD-cbinsight-com.totalRaised"
        ],
        "relation": "one-to-many",
        "split_delimiter": None
    },

    # 18) company_type => one-to-one
    "company_type": {
        "sources": [
            "output_govukbigsize.company_type",
            "wissel-ariregister.Legal form"
        ],
        "relation": "one-to-one"
    },

    # 19) ceo_name => one-to-one
    "ceo_name": {
        "sources": [
            "disfold-com.ceo",
            "valueToday_dataset.ceo"
        ],
        "relation": "one-to-one"
    },

    # 20) representative_name => one-to-one
    "representative_name": {
        "sources": [
            "wissel-ariregister.Representative Name"
        ],
        "relation": "one-to-one"
    }
}






##########################################
# 4) FUNZIONE CHE GENERA UN CSV PER OGNI DATASET
##########################################
def process_each_dataset(mapping: dict, raw_dir: str, output_dir: str):
    """
    1) Legge i file raw dalla cartella raw_dir
    2) Per ognuno, trasforma la tabella secondo 'mapping'
    3) Salva il CSV mediato in output_dir
    """
    os.makedirs(output_dir, exist_ok=True)
    files = read_files(raw_dir)
    for table_name, df in files:
        mediated_df = transform_table(table_name, df, mapping)
        out_csv = os.path.join(output_dir, f"{table_name}_mediated.csv")
        mediated_df.to_csv(out_csv, index=False)
        print(f"Generated CSV: {out_csv}")


##########################################
# 5) MAIN
##########################################
if __name__ == '__main__':
    raw_data_dir = 'data/raw'    # cartella con i dataset
    output_dir = 'new_data'      # cartella di output

    process_each_dataset(mediated_mapping, raw_data_dir, output_dir)
