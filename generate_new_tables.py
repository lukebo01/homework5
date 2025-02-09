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

#==============================================================================
# ESEMPIO COMPLETO DI MAPPING PER IL
# PROGETTO "DATA INTEGRATION".
#==============================================================================
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
        "relation": "one-to-one"
        # Niente table_rules perché non ci serve un comportamento speciale.
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
        "relation": "one-to-one"
    },

    # 3) sector -> many-to-one se vogliamo unire eventuali campi (al momento è solo 1 fonte).
    "sector": {
        "sources": [
            "MalPatSaj-wikipedia-org.Sector"
        ],
        "relation": "many-to-one",
        "merge_delimiter": " "
    },

    # 4) business_category -> splitted su virgola
    "business_category": {
        "sources": [
            "campaignindia.CATEGORY",
            "companiesMarketCap_dataset.categories",
            "valueToday_dataset.company_business",
            "output_govukbigsize.nature_of_business"
        ],
        "relation": "one-to-many",
        "split_delimiter": ","
    },

    # 5) headquarters_city -> splitted su virgola
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
            "DDD-teamblind-com": {
            "relation": "one-to-many",
            "split_delimiter": ",",
            "take_index": 0
            },
            "disfold-com": {
            "relation": "one-to-many",
            "split_delimiter": ",",
            "take_index": 0
            }
        }
    },


    # 6) headquarters_country -> splitted su virgola
    "headquarters_country": {
        "sources": [
            "DDD-cbinsight-com.country",
            "DDD-teamblind-com.locations",
            "MalPatSaj-forbes-com.Country",
            "disfold-com.headquarters",
            "ft-com.country",
            "hitHorizons_dataset.nation",
            "valueToday_dataset.headquarters_country",
            "companiesMarketCap_dataset.country",
            "wissel-aziende-info-clipper-com.Country"
        ],
        "relation": "one-to-many",
        "split_delimiter": ",",
        "table_rules": {
            "DDD-teamblind-com": {
            "relation": "one-to-many",
            "split_delimiter": ",",
            "take_index": 1
            },
            "disfold-com": {
            "relation": "one-to-many",
            "split_delimiter": ",",
            "take_index": 1
            }
        }
    },

    # 7) headquarters_full_address -> one-to-one
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

    # 8) year_founded
    # => regola speciale SOLO per AmbitionBox: split su spazio e prendi il primo token
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
            }
        }
    },

    # 09) ownership (many-to-one: unisce AmbitionBox.Ownership + hitHorizons_dataset.type)
    "ownership": {
        "sources": [
            "AmbitionBox.Ownership",
            "hitHorizons_dataset.type"
        ],
        "relation": "one-to-one"
    },

    # 10) company_number -> one-to-one
    "company_number": {
        "sources": [
            "output_govukbigsize.company_number",
            "wissel-ariregister.Code"
        ],
        "relation": "one-to-one"
    },

    # 11) employee_count -> one-to-one
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

    # 12) market_cap_usd -> one-to-one
    "market_cap_usd": {
        "sources": [
            "MalPatSaj-forbes-com.Market Value",
            "companiesMarketCap_dataset.market_cap",
            "disfold-com.market_cap",
            "output_globaldata.market_cap"
        ],
        "relation": "one-to-one"
    },

    # 13) total_revenue_usd -> one-to-one
    "total_revenue_usd": {
        "sources": [
            "MalPatSaj-forbes-com.Sales",
            "ft-com.revenue",
            "output_globaldata.revenue",
            "valueToday_dataset.annual_revenue_in_usd"
        ],
        "relation": "one-to-one"
    },

    # 14) net_profit_usd -> one-to-one
    "net_profit_usd": {
        "sources": [
            "MalPatSaj-forbes-com.Profit",
            "valueToday_dataset.annual_net_income_in_usd"
        ],
        "relation": "one-to-one"
    },

    # 15) total_assets_usd -> one-to-one
    "total_assets_usd": {
        "sources": [
            "MalPatSaj-forbes-com.Assets",
            "valueToday_dataset.total_assets_in_usd"
        ],
        "relation": "one-to-one"
    },

    # 16) company_website -> one-to-one
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

    # 17) social_media_links -> splitted su None (array di link: FB,Twitter,IG,Pinterest)
    "social_media_links": {
        "sources": [
            "company_social_urls.Facebook",
            "company_social_urls.Twitter",
            "company_social_urls.Instagram",
            "company_social_urls.Pinterest"
        ],
        "relation": "many-to-one",
        "split_delimiter": None
    },

    # 18) total_raised -> splitted su None
    "total_raised": {
        "sources": [
            "DDD-cbinsight-com.totalRaised"
        ],
        "relation": "one-to-one",
    },

    # 19) representative_name -> one-to-one
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






# ===========================================================================================================================================================================================
# =============================================================================
# GUIDA COMPLETA PER LA COSTRUZIONE DI UN MAPPING "MEDIATED" IN PYTHON
# =============================================================================
# Creare un dizionario "mediated_mapping" che
# definisce come unire (o splittare) i campi provenienti da diversi dataset
# in uno schema mediato unico.
# =============================================================================

# Ogni chiave del dizionario "mediated_mapping" rappresenta un attributo
# (colonna) che vogliamo ottenere nel nostro schema finale.
# Il valore associato può essere:
#   - una lista "sources": [] -> relation = "one-to-one" (default)
#   - un dizionario con i campi:
#       - "sources": [ ... ]       (obbligatorio)
#       - "relation": "one-to-one" | "one-to-many" | "many-to-one"
#       - "split_delimiter": ...   (solo per one-to-many)
#       - "merge_delimiter": ...   (solo per many-to-one)
#       - "take_first": true/false (solo per one-to-many)
#       - "take_index": int        (solo per one-to-many)
#       - "table_rules": { ... }   (regole speciali per tabella)

# "split_delimiter": ...
#   - Solo per "one-to-many"
#   - La stringa su cui dividere il valore (es. ",", " ", ";", ecc.)
#   - Esempio: se ho "A,B,C" e split_delimiter="," => ["A","B","C"]

# "merge_delimiter": ...
#   - Solo per "many-to-one"
#   - La stringa da usare per unire più valori in uno (es. spazio " ").
#   - Esempio: se ho 2 valori "Pubblico" e "Spa", => "Pubblico Spa"

# "take_first": true/false
#   - Solo per "one-to-many"
#   - Se = true, dopo lo split prendo solamente il primo token
#     e scarto gli altri.
#   - Esempio: "1968 (55 yrs old)" => split su " " => ["1968","(55","yrs","old)"]
#     con take_first => "1968"

# "take_index": int
#   - Solo per "one-to-many"
#   - Se presente, prendo SOLO il token in posizione "int" (0-based)
#   - Se "take_index": 2 => su ["1968","(55","yrs","old)"] prendo "yrs"

# "table_rules": { <nome_tabella>: { ... } }
#   - Opzionale, serve a definire comportamenti speciali
#     per le righe che provengono da una determinata tabella.
#   - All'interno si possono sovrascrivere:
#       relation, split_delimiter, merge_delimiter, take_first, take_index...
#   - Esempio: se AmbitionBox richiede lo split su " ",
#     mentre tutte le altre tabelle no,
#     definisci:
#       table_rules = {
#         "AmbitionBox": {
#           "relation": "one-to-many",
#           "split_delimiter": " ",
#           "take_first": True
#         }
#       }


# Ecco uno schema di esempio con commenti che spiegano ogni possibile campo.
mediated_mapping_example = {

    # -------------------------------------------------------------------------
    # 1) ESEMPIO DI SEMPLICE one-to-one
    # -------------------------------------------------------------------------
    "company_name": {
        # "sources" è una lista di stringhe in formato "NomeTabella.NomeColonna",
        # cioè <table_name>.<column_name>.
        "sources": [
            "AmbitionBox.Name",
            "MalPatSaj-wikipedia-org.Name"
            # ... e altre tabelle, se vuoi unire i valori che contengono "Name".
        ],
        # "relation": "one-to-one" significa che prendi i valori provenienti
        # dalle diverse tabelle e, se sono presenti più di un valore, li concateni
        # con uno spazio (o con "merge_delimiter" di default).
        "relation": "one-to-one"
        # Se non hai bisogno di nulla di speciale, non definire "table_rules",
        # "split_delimiter", "merge_delimiter", ecc.
    },

    # -------------------------------------------------------------------------
    # 2) ESEMPIO DI MANY-TO-ONE (MERGE)
    # -------------------------------------------------------------------------
    "ownership": {
        # L'esempio: unisce "AmbitionBox.Ownership" + "hitHorizons_dataset.type".
        "sources": [
            "AmbitionBox.Ownership",
            "hitHorizons_dataset.type"
        ],
        # "many-to-one" => unisce i valori trovati in un'unica stringa,
        # separandoli con "merge_delimiter".
        "relation": "many-to-one",
        # "merge_delimiter": " " (spazio) di default, ma puoi anche mettere ", "
        "merge_delimiter": " "
    },

    # -------------------------------------------------------------------------
    # 3) ESEMPIO DI ONE-TO-MANY (SPLIT)
    # -------------------------------------------------------------------------
    "headquarters_city": {
        # Fonti: potresti avere un campo "disfold-com.headquarters" che contiene
        # una stringa "New York, USA" e un campo "DDD-teamblind-com.locations"
        # "Menlo Park, CA".
        "sources": [
            "disfold-com.headquarters",
            "DDD-teamblind-com.locations"
        ],
        "relation": "one-to-many",
        # "split_delimiter": "," => questo indica di splittare la stringa su virgola.
        # Il risultato sarà una lista di token ["New York", " USA"] ad es.
        "split_delimiter": ",",
        # "take_first": True => se vuoi prendere soltanto il primo token.
        # "take_index": 1 => se vuoi prendere il secondo token.
        # Se non li metti, avrai l'intera lista.
    },

    # -------------------------------------------------------------------------
    # 4) "table_rules" (REGOLE CONDIZIONALI PER TABELLA)
    # -------------------------------------------------------------------------
    "year_founded": {
        "sources": [
            "AmbitionBox.Foundation Year",
            "DDD-teamblind-com.founded"
        ],
        # Di default, one-to-one
        "relation": "one-to-one",
        # "table_rules" => se ho bisogno di trattare la tabella "AmbitionBox"
        # in modo diverso (es. split su spazio e prendo solo il primo token),
        # mentre "DDD-teamblind-com" rimane one-to-one.
        "table_rules": {
            "AmbitionBox": {
                "relation": "one-to-many",
                "split_delimiter": " ",
                "take_first": True
            }
            # Se volessi trattare "DDD-teamblind-com" in modo speciale:
            # "DDD-teamblind-com": {...}
        }
    },

    # -------------------------------------------------------------------------
    # 5) SOLO LISTA "sources": CASO SEMPLICE
    # -------------------------------------------------------------------------
    # Se non vuoi definire "relation" né altro (one-to-one di default),
    # puoi semplicemente fare:
    "market_cap_usd": [
        "MalPatSaj-forbes-com.Market Value",
        "companiesMarketCap_dataset.market_cap"
    ],
    # e la logica interna assume "relation": "one-to-one".

    # -------------------------------------------------------------------------
    # ALTRI ESEMPI DI ATTRIBUTI (SENZA COMMENTI)
    # -------------------------------------------------------------------------
    "industry": {
        "sources": [
            "AmbitionBox.Industry",
            "DDD-cbinsight-com.industry",
            "MalPatSaj-wikipedia-org.Industry"
        ],
        "relation": "one-to-one"
    },

    "social_media_links": {
        "sources": [
            "company_social_urls.Facebook",
            "company_social_urls.Twitter",
            "company_social_urls.Instagram",
            "company_social_urls.Pinterest"
        ],
        "relation": "one-to-many",
        # "split_delimiter": None => se vuoi l'intera URL come un token singolo
        "split_delimiter": None
    }
}
# ===========================================================================================================================================================================================