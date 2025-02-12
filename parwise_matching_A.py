import json
import pandas as pd
import re
import recordlinkage

# --------------------------
# FUNZIONI DI SUPPORTO
# --------------------------

def safe_str(s):
    """Restituisce una stringa anche se s è NaN."""
    if pd.isnull(s):
        return ""
    return str(s)

def canonical_pair(rec_left, rec_right):
    """
    Data una coppia di record (dizionari), restituisce una rappresentazione canonica (frozenset)
    basata su (company_name_normalized, _source_table).
    La normalizzazione converte in lowercase, elimina spazi multipli e fa strip.
    """
    def norm(rec):
        name = safe_str(rec.get("company_name", "")).lower().strip()
        name = re.sub(r'\s+', ' ', name)
        source = safe_str(rec.get("_source_table", "")).lower().strip()
        return (name, source)
    return frozenset([norm(rec_left), norm(rec_right)])

def load_candidate_pairs_list(json_file):
    """Carica il file JSON dei candidate pairs e restituisce la lista di coppie."""
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("candidate_pairs", [])

def compute_raw_similarity(record1, record2):
    """
    Calcola la similarità Jaro-Winkler tra i campi 'company_name' usando recordlinkage.
    """
    compare = recordlinkage.Compare()
    compare.string("company_name", "company_name", method="jarowinkler", label="name_sim")

    df = pd.DataFrame([record1, record2])
    pairs = recordlinkage.Index().full().index(df)

    similarity_df = compare.compute(pairs, df, df)
    
    return similarity_df.iloc[0]["name_sim"]  # Restituisce il punteggio di similarità

def filter_candidate_pairs(candidate_pairs, min_sim=0.6, max_sim=0.7):
    """
    Filtra le candidate pairs mantenendo solo quelle con similarità tra min_sim e max_sim.
    """
    filtered = []
    for pair in candidate_pairs:
        rec1 = pair.get("record1", {})
        rec2 = pair.get("record2", {})
        sim_score = compute_raw_similarity(rec1, rec2)

        source1 = safe_str(rec1.get("_source_table", "")).strip().lower()
        source2 = safe_str(rec2.get("_source_table", "")).strip().lower()

        if (min_sim <= sim_score <= max_sim) and (source1 != source2):
            filtered.append(pair)

    return filtered

def load_and_filter_candidate_pairs_df(json_file, min_sim=0.6, max_sim=0.7):
    """
    Carica i candidate pairs, filtra le coppie e restituisce un DataFrame.
    """
    candidate_pairs = load_candidate_pairs_list(json_file)
    filtered_pairs = filter_candidate_pairs(candidate_pairs, min_sim, max_sim)

    rows = []
    for pair in filtered_pairs:
        rec1 = pair.get("record1", {})
        rec2 = pair.get("record2", {})

        row = {
            "company_name_left": safe_str(rec1.get("company_name", "")),
            "industry_left": safe_str(rec1.get("industry", "")),
            "_source_table_left": safe_str(rec1.get("_source_table", "")),
            "company_name_right": safe_str(rec2.get("company_name", "")),
            "industry_right": safe_str(rec2.get("industry", "")),
            "_source_table_right": safe_str(rec2.get("_source_table", ""))
        }
        rows.append(row)

    return pd.DataFrame(rows)

def load_ground_truth(json_file):
    """
    Carica la ground truth e restituisce un set di coppie canoniche.
    """
    with open(json_file, "r", encoding="utf-8") as f:
        gt_data = json.load(f)
    
    gt_pairs = set()
    for pair in gt_data.get("ground_truth", []):
        records = []
        for company_name, source in pair.items():
            name = safe_str(company_name).lower().strip()
            name = re.sub(r'\s+', ' ', name)
            src = safe_str(source).lower().strip()
            records.append((name, src))
        if len(records) == 2:
            gt_pairs.add(frozenset(records))
    
    return gt_pairs

def perform_pairwise_matching(candidate_df, similarity_threshold=0.7):
    """
    Esegue il pairwise matching usando recordlinkage.Compare().
    """
    left = candidate_df[["company_name_left", "industry_left", "_source_table_left"]].copy()
    right = candidate_df[["company_name_right", "industry_right", "_source_table_right"]].copy()

    left = left.rename(columns={
        "company_name_left": "company_name",
        "industry_left": "industry",
        "_source_table_left": "_source_table"
    })
    right = right.rename(columns={
        "company_name_right": "company_name",
        "industry_right": "industry",
        "_source_table_right": "_source_table"
    })

    compare = recordlinkage.Compare()
    compare.string("company_name", "company_name", method="jarowinkler",
                   threshold=similarity_threshold, label="name_sim")

    pairs = recordlinkage.Index().full().index(left)
    features = compare.compute(pairs, left, right)

    matched_idx = features[features["name_sim"] == 1].index
    predicted_matches = set()

    for (i, j) in matched_idx:
        rec_left = left.iloc[i].to_dict()
        rec_right = right.iloc[j].to_dict()
        predicted_matches.add(canonical_pair(rec_left, rec_right))

    return predicted_matches, features

def evaluate_matching(predicted_matches, ground_truth_pairs):
    """
    Confronta i match predetti con la ground truth e calcola precision, recall e F1-score.
    """
    true_positives = predicted_matches.intersection(ground_truth_pairs)

    num_tp = len(true_positives)
    num_pred = len(predicted_matches)
    num_gt = len(ground_truth_pairs)

    precision = num_tp / num_pred if num_pred > 0 else 0
    recall = num_tp / num_gt if num_gt > 0 else 0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0

    return {
        "true_positives": num_tp,
        "predicted_matches": num_pred,
        "ground_truth_matches": num_gt,
        "precision": precision,
        "recall": recall,
        "f1_score": f1
    }

# --------------------------
# SCRIPT PRINCIPALE
# --------------------------
if __name__ == "__main__":
    ground_truth_file = "data/ground_truth.json"
    
    candidate_pairs_file_A = "main_outputs/candidate_pairs_strategy_A.json"
    candidate_df_A = load_and_filter_candidate_pairs_df(candidate_pairs_file_A)
    predicted_matches_A, _ = perform_pairwise_matching(candidate_df_A)

    ground_truth_pairs = load_ground_truth(ground_truth_file)
    metrics_A = evaluate_matching(predicted_matches_A, ground_truth_pairs)

    print("\n--- Metriche per Strategia A ---")
    for key, value in metrics_A.items():
        print(f"{key}: {value}")