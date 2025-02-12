import os
import pandas as pd

def populate_final_mediated_schema(input_dir: str, output_csv: str) -> None:
    """
    Legge tutti i file CSV presenti in 'input_dir' (la cartella new_data)
    e li unisce in un unico DataFrame, generando un file CSV finale
    chiamato 'output_csv'.
    """
    csv_files = sorted(f for f in os.listdir(input_dir) if f.lower().endswith(".csv"))

    all_dfs = []
    for csv_file in csv_files:
        file_path = os.path.join(input_dir, csv_file)
        df = pd.read_csv(file_path)
        all_dfs.append(df)

    if not all_dfs:
        print("Nessun CSV trovato in:", input_dir)
        return

    # Unisce tutti i CSV in un unico DataFrame
    final_df = pd.concat(all_dfs, ignore_index=True)

    # Salva il risultato finale in un unico CSV
    final_df.to_csv(output_csv, index=False)
    print(f"Schema mediato finale creato e salvato in: {output_csv}")


if __name__ == "__main__":
    # ESEMPIO DI UTILIZZO:
    # Cartella che contiene i CSV mediati (generati in precedenza)
    new_data_dir = "new_data"
    # Nome del file CSV finale
    final_output = "main_outputs/final_mediated_schema.csv"

    populate_final_mediated_schema(new_data_dir, final_output)
