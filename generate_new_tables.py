import os
import pandas as pd

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

        try:
            if extension in ["json", "jsonl"]:
                # Tenta di leggere come JSON (o JSON lines)
                # Se file_name.json in realtà è multiline JSON o mal formattato, 
                # potresti dover gestire diversamente. 
                # lines=True => indica JSON lines solo se sei sicuro che il file lo sia.
                # In questo esempio, assumiamo "jsonl" => lines=True, "json" => lines=False
                use_lines = (extension == "jsonl")
                df = pd.read_json(file_path, lines=use_lines)
            elif extension == "csv":
                df = pd.read_csv(file_path)
            elif extension == "xls":
                df = pd.read_excel(file_path, engine="xlrd")
            else:
                # Formato non riconosciuto, si ignora
                continue
        except ValueError as e:
            print(f"Impossibile caricare {file_path}: {e}")
            # Se vuoi saltare il file problematico senza interrompere l'intero processo
            continue
        
        # Elimina la colonna "Unnamed: 0" se presente
        if df is not None:
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
    Estrae e trasforma il valore della riga 'row' di tabella 'table_name' in base a mapping_entry.
    
    - Se mapping_entry è una lista => default one-to-one (copia diretta, merge se più valori).
    - Se mapping_entry è un dict => può specificare "relation" (one-to-one, many-to-one, one-to-many)
      e altri parametri come "split_delimiter", "merge_delimiter", "take_first", "take_index" ecc.

    RELAZIONI:
      1) one-to-one:
         copia diretta del valore (se più valori trovati => concatenazione con merge_delimiter).
      2) many-to-one:
         unisce i valori trovati da più colonne in uno (separati da merge_delimiter).
      3) one-to-many:
         si fa lo split del testo sullo split_delimiter; se "take_first": True => prendi solo il primo pezzo;
         se "take_index": X => prendi solo l'elemento in posizione X;
         altrimenti restituisci l'intera lista dei pezzi.

    Per ogni source_attr in mapping_entry, se src_table == table_name, si cercano le colonne
    che matchano esattamente (dopo lo strip) src_col.
    """
    # Impostazioni di default
    if isinstance(mapping_entry, list):
        sources = mapping_entry
        relation = "one-to-one"
        merge_delimiter = " "
        split_delimiter = None
        take_first = False
        take_index = None
    elif isinstance(mapping_entry, dict):
        sources = mapping_entry.get("sources", [])
        relation = mapping_entry.get("relation", "one-to-one")  # one-to-one di default
        merge_delimiter = mapping_entry.get("merge_delimiter", " ")
        split_delimiter = mapping_entry.get("split_delimiter")  # di default None
        take_first = mapping_entry.get("take_first", False)
        take_index = mapping_entry.get("take_index", None)
    else:
        return None  # tipo di mapping_entry non riconosciuto

    # Raccoglie i valori corrispondenti
    values = []
    for source_attr in sources:
        src_table, src_col = parse_source_attr(source_attr)
        if src_table == table_name:
            # Cerca la colonna corrispondente (tolgo spazi con strip)
            matching_cols = [c for c in row.index if c.strip() == src_col]
            if matching_cols:
                actual_col = matching_cols[0]
                val = row[actual_col]
                # Se val è list-like
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
        # Se non c'è un delimitatore, fallback => one-to-one
        if split_delimiter is None:
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
                # split su v con split_delimiter
                tokens = [x.strip() for x in v.split(split_delimiter) if x.strip()]
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
            # Altrimenti restituisco l'intero array
            return split_list

    else:
        # Fallback se la relation non è riconosciuta
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
# 3) IL MAPPING COMPLETO
##########################################
mediated_mapping = {
  "company_name": [
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
    "output_govuk_bigsize.name",
    "valueToday_dataset.name",
    "wissel-ariregister.Name",
    "wissel-aziende-info-clipper-com.Name"
  ],
  "industry": [
    "AmbitionBox.Industry",
    "DDD-cbinsight-com.industry",
    "DDD-teamblind-com.industry",
    "MalPatSaj-wikipedia-org.Industry",
    "hitHorizons_dataset.industry",
    "output_globaldata.industry",
    "ft-com.industry",
    "wissel-ariregister.Area of Activity"
  ],
  "sector": [
    "MalPatSaj-wikipedia-org.Sector"
  ],
  "business_category": [
    "campaignindia.CATEGORY",
    "companiesMarketCap_dataset.categories",
    "valueToday_dataset.company_business",
    "output_govuk_bigsize.nature_of_business"
  ],
  "headquarters_city": [
    "DDD-cbinsight-com.city",
    "DDD-teamblind-com.locations",
    "disfold-com.headquarters",
    "valueToday_dataset.headquarters_region_city",
    "wissel-aziende-info-clipper-com.City"
  ],
  "headquarters_country": [
    "DDD-cbinsight-com.country",
    "MalPatSaj-forbes-com.Country",
    "disfold-com.headquarters",
    "ft-com.country",
    "hitHorizons_dataset.nation",
    "valueToday_dataset.headquarters_country",
    "companiesMarketCap_dataset.country",
    "wissel-aziende-info-clipper-com.Country"
  ],
  "headquarters_region": [
    "valueToday_dataset.headquarters_sub_region"
  ],
  "headquarters_continent": [
    "valueToday_dataset.headquarters_continent"
  ],
  "headquarters_full_address": [
    "hitHorizons_dataset.address",
    "output_globaldata.address",
    "output_govuk_bigsize.registered_office_address",
    "wissel-ariregister.Address",
    "wissel-aziende-info-clipper-com.Address Name"
  ],
  "location_type": [
    "wissel-aziende-info-clipper-com.Location type"
  ],
  "year_founded": [
    "AmbitionBox.Foundation Year",
    "DDD-cbinsight-com.founded",
    "DDD-teamblind-com.founded",
    "MalPatSaj-wikipedia-org.Founded",
    "ft-com.founded",
    "hitHorizons_dataset.est_of_ownership",
    "output_govuk_bigsize.company_creation_date",
    "wissel-ariregister.Registration Date"
  ],
  "company_age": [
    "AmbitionBox.Foundation Year"
  ],
  "ownership": [
    "AmbitionBox.Ownership",
    "hitHorizons_dataset.type"
  ],
  "company_status": [
    "output_govuk_bigsize.company_status",
    "wissel-ariregister.Status"
  ],
  "company_type": [
    "output_govuk_bigsize.company_type",
    "wissel-ariregister.Legal form"
  ],
  "company_number": [
    "output_govuk_bigsize.company_number",
    "wissel-ariregister.Code"
  ],
  "ceo_name": [
    "disfold-com.ceo",
    "valueToday_dataset.ceo"
  ],
  "company_founders": [
    "valueToday_dataset.founders"
  ],
  "employee_count": [
    "DDD-teamblind-com.size",
    "disfold-com.employees",
    "ft-com.employees",
    "output_globaldata.number_of_employees",
    "valueToday_dataset.number_of_employees"
  ],
  "market_cap_usd": [
    "MalPatSaj-forbes-com.Market Value",
    "companiesMarketCap_dataset.market_cap",
    "disfold-com.market_cap",
    "output_globaldata.market_cap"
  ],
  "valuation_usd": [
    "DDD-cbinsight-com.valuation"
  ],
  "total_revenue_usd": [
    "MalPatSaj-forbes-com.Sales",
    "ft-com.revenue",
    "output_globaldata.revenue",
    "valueToday_dataset.annual_revenue_in_usd"
  ],
  "net_profit_usd": [
    "MalPatSaj-forbes-com.Profit",
    "valueToday_dataset.annual_net_income_in_usd"
  ],
  "fiscal_year_end": [
    "valueToday_dataset.annual_results_for_year_ending"
  ],
  "total_assets_usd": [
    "MalPatSaj-forbes-com.Assets",
    "valueToday_dataset.total_assets_in_usd"
  ],
  "total_liabilities_usd": [
    "valueToday_dataset.total_liabilities_in_usd"
  ],
  "total_equity_usd": [
    "valueToday_dataset.total_equity_in_usd"
  ],
  "company_website": [
    "DDD-teamblind-com.website",
    "output_globaldata.website",
    "valueToday_dataset.company_website",
    "wissel-ariregister.URL",
    "wissel-aziende-info-clipper-com.URL"
  ],
  "investors": [
    "DDD-cbinsight-com.investors"
  ],
  "total_raised": [
    "DDD-cbinsight-com.totalRaised"
  ],
  "social_media_links": [
    "company_social_urls.Facebook",
    "company_social_urls.Twitter",
    "company_social_urls.Instagram",
    "company_social_urls.Pinterest"
  ],
  "stock_share_price": [
    "companiesMarketCap_dataset.share_price"
  ],
  "stock_change_1_day": [
    "companiesMarketCap_dataset.change_1_day"
  ],
  "stock_change_1_year": [
    "companiesMarketCap_dataset.change_1_year"
  ],
  "representative_name": [
    "wissel-ariregister.Representative Name"
  ],
  "representative_code": [
    "wissel-ariregister.Representative Code"
  ],
  "representative_role": [
    "wissel-ariregister.Representative Role"
  ],
  "representative_start_date": [
    "wissel-ariregister.Representative Start Date"
  ],
  "postal_code": [
    "wissel-aziende-info-clipper-com.Postalcode"
  ],
  "telephone": [
    "output_globaldata.telephone"
  ]
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
# 5) APPLICAZIONE DELLE RELAZIONI LLM
##########################################
def apply_llm_relations(mapping: dict) -> dict:
    """
    Modifica 'mapping' in place, definendo per alcuni attributi le relazioni
    one-to-many o many-to-one, con le regole di merge/split, come indicato nella
    risposta dell'LLM. Restituisce lo stesso dizionario 'mapping' aggiornato.
    """

    # Esempio: company_age è "one-to-many" creato da 'AmbitionBox.Foundation Year'
    # che può contenere stringhe del tipo "1968 (55 yrs old)". Splittiamo su spazio,
    # e prendiamo solo il primo token (es. "1968").
    mapping["company_age"] = {
        "sources": mapping["company_age"],  # riutilizziamo l'elenco di source esistente
        "relation": "one-to-many",
        "split_delimiter": " ",
        "take_first": True
    }

    # headquarters_city: "one-to-many"
    # Spesso sono stringhe come "Tallinn, Estonia" o "City, State"; splittiamo su virgola.
    mapping["headquarters_city"] = {
        "sources": mapping["headquarters_city"],
        "relation": "one-to-many",
        "split_delimiter": ","
        # se volessi prendere solo il primo token, potresti aggiungere "take_first": True
    }

    # business_category: "one-to-many"
    # Esempio: 'companiesMarketCap_dataset.categories' o 'valueToday_dataset.company_business'
    # contengono liste/stringhe separate da virgole, quindi split su virgola
    mapping["business_category"] = {
        "sources": mapping["business_category"],
        "relation": "one-to-many",
        "split_delimiter": ","
    }

    # social_media_links: "one-to-many"
    # Nel testo LLM si dice che 'company_social_urls' fornisce vari link,
    # uno per Facebook, Twitter, ecc. Li mettiamo in un array.
    # Se li vuoi concatenare in un'unica stringa, useresti many-to-one;
    # qui supponiamo di volere un array di link => split_delimiter = None => no split
    mapping["social_media_links"] = {
        "sources": mapping["social_media_links"],
        "relation": "one-to-many",
        "split_delimiter": None
    }

    # investors: "one-to-many"
    # Se la colonna 'DDD-cbinsight-com.investors' è una stringa con più investitori separati da virgole,
    # possiamo fare split su virgola. Se invece è un elenco già pronto, mettiamo 'split_delimiter': None
    mapping["investors"] = {
        "sources": mapping["investors"],
        "relation": "one-to-many",
        "split_delimiter": ","  # o None se è già un array
    }

    # total_raised: "one-to-many"
    # Se nel LLM è marcato come "one-to-many". Se in 'DDD-cbinsight-com.totalRaised' trovi
    # stringhe come "1.907B" "556M" ecc. e vuoi gestirle come array, puoi impostare un delimitatore,
    # altrimenti lo lasci come 'one-to-many' con split_delimiter=None (=> no split).
    mapping["total_raised"] = {
        "sources": mapping["total_raised"],
        "relation": "one-to-many",
        "split_delimiter": None
    }

    # ---------------------
    # Many-to-One
    # ---------------------

    # sector
    mapping["sector"] = {
        "sources": mapping["sector"],
        "relation": "many-to-one",
        "merge_delimiter": " "  # unisce eventuali valori con uno spazio
    }

    # ownership: unisce ad esempio AmbitionBox.Ownership e hitHorizons_dataset.type in un'unica stringa
    mapping["ownership"] = {
        "sources": mapping["ownership"],
        "relation": "many-to-one",
        "merge_delimiter": " "
    }

    # fiscal_year_end
    mapping["fiscal_year_end"] = {
        "sources": mapping["fiscal_year_end"],
        "relation": "many-to-one",
        "merge_delimiter": " "
    }

    # valuation_usd
    mapping["valuation_usd"] = {
        "sources": mapping["valuation_usd"],
        "relation": "many-to-one",
        "merge_delimiter": " "
    }

    # Restituisce il mapping modificato
    return mapping


##########################################
# MAIN
##########################################
if __name__ == '__main__':
    raw_data_dir = 'data/raw'  # cartella con i dataset
    output_dir = 'new_data'    # cartella di output
    
    mediated_mapping = apply_llm_relations(mediated_mapping)
    # ... se vuoi ottenere solo la prima parte (es. "1968" da "1968 (55 yrs old)").
    #
    process_each_dataset(mediated_mapping, raw_data_dir, output_dir)
