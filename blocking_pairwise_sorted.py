import pandas as pd
import recordlinkage
import time

start_time = time.time()
# Caricamento del file CSV
schema_mediato = pd.read_csv('C:\\Users\\franc\\Desktop\\homeworkFRA5\\schema_mediato_popolato.csv')

# Gestione dei valori mancanti (sostituzione con stringa vuota)
schema_mediato.fillna('', inplace=True)

# Normalizza tutti gli attributi: rimozione degli spazi e conversione in minuscolo
schema_mediato = schema_mediato.apply(lambda x: x.str.strip().str.lower() if x.dtypes == 'object' else x)

# Filtraggio dei record indesiderati: rimuove righe con 'Name' vuoto o contenente solo '\n'
schema_mediato = schema_mediato[~(schema_mediato['Name'].str.strip().isin(['', '\n']))]

# Creazione dell'indice per il blocking con sorted neighborhood (basato su 'Name')
indexer = recordlinkage.Index()
indexer.sortedneighbourhood(left_on=['Name'], window=5)

# Generazione delle coppie candidate
candidate_links = indexer.index(schema_mediato)

# Pairwise matching con Jaro-Winkler per tutti gli attributi
compare = recordlinkage.Compare()

# Itera su tutte le colonne e aggiunge il confronto con Jaro-Winkler
for col in schema_mediato.columns:
    compare.string(col, col, method='jarowinkler', label=f'{col}_similarity')

# Confronto delle coppie candidate
features = compare.compute(candidate_links, schema_mediato)

# Imposta la similarità a zero se entrambe le celle sono vuote per lo stesso attributo
for col in schema_mediato.columns:
    empty_cells = (schema_mediato[col] == '')  # Celle vuote
    pairs_with_empty_cells = candidate_links[
        empty_cells.loc[candidate_links.get_level_values(0)].values &
        empty_cells.loc[candidate_links.get_level_values(1)].values
    ]
    features.loc[pairs_with_empty_cells, f'{col}_similarity'] = 0

# Filtraggio delle coppie:
# 1. La similarità di 'Name' deve essere > 0.7.
# 2. Almeno 3 attributi devono avere una similarità >= threshold.
threshold = 0.75  # Soglia generale di similarità
matches = features[
    (features['Name_similarity'] > 0.7) & (features['country_similarity'] > 0.75) & # Condizione su 'Name'
    ((features >= threshold).sum(axis=1) >= 2)  # Almeno 2 attributi sopra soglia
]

# Salvataggio delle coppie abbinate su file CSV
matches.reset_index()[['level_0', 'level_1']].to_csv(
    'C:\\Users\\franc\\Desktop\\homeworkFRA5\\blocking_pairwise_sorted_time.csv',
    header=['Record_1', 'Record_2'],  # Solo le colonne richieste
    index=False
)


# Calcola e stampa il tempo di esecuzione
end_time = time.time()
execution_time = end_time - start_time



print(f"Processo completato in {execution_time:.2f} secondi.")