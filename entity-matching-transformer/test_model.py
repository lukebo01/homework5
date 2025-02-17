import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# 1️⃣ Carichiamo il modello addestrato
model_path = "experiments/DATA/DIRTY_AMAZON_ITUNES/DEEP_MATCHER__DISTILBERT__20250216_211628"  # Sostituisci con il nome corretto della tua cartella di esperimenti
tokenizer = AutoTokenizer.from_pretrained(model_path)
model = AutoModelForSequenceClassification.from_pretrained(model_path)
model.eval()  # Mettiamo il modello in modalità di valutazione

# 2️⃣ Carichiamo le coppie da testare
test_file = "filtered_pairs.tsv"
df = pd.read_csv(test_file, sep="\t")

# 3️⃣ Funzione per fare le predizioni
def predict_match(sentence1, sentence2):
    inputs = tokenizer(sentence1, sentence2, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        outputs = model(**inputs)
    scores = outputs.logits.softmax(dim=1)
    return scores[0][1].item()  # Restituisce la probabilità che sia un match

# 4️⃣ Applichiamo il modello a ogni coppia
df["match_score"] = df.apply(lambda row: predict_match(row["sentence1"], row["sentence2"]), axis=1)

# 5️⃣ Aggiungiamo una colonna con il risultato (1 = Match, 0 = No Match)
df["predicted_label"] = df["match_score"].apply(lambda x: 1 if x > 0.5 else 0)

# 6️⃣ Salviamo i risultati
df.to_csv("matching_results.tsv", sep="\t", index=False)

print("✅ Predizioni completate! I risultati sono salvati in `matching_results.tsv`")
