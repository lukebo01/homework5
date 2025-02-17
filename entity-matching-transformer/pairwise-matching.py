import pandas as pd
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from safetensors.torch import load_file

# Carica il modello
model_path = "experiments/DATA/DIRTY_AMAZON_ITUNES/DEEP_MATCHER__DISTILBERT__20250216_211628"
tokenizer = AutoTokenizer.from_pretrained(model_path)
model = AutoModelForSequenceClassification.from_pretrained(model_path)
state_dict = load_file(f"{model_path}/model.safetensors")
model.load_state_dict(state_dict)
model.eval()

def predict(sentence1, sentence2):
    inputs = tokenizer(sentence1, sentence2, return_tensors="pt", truncation=True, padding=True, max_length=180)
    with torch.no_grad():
        outputs = model(**inputs)
        predictions = torch.argmax(outputs.logits, dim=-1)
    return predictions.item()

# Carica il dataset con le coppie bloccate
df = pd.read_csv("blocked_pairs.tsv", sep="\t")

# Predice il match per ogni coppia
df["label"] = df.apply(lambda row: predict(row["sentence1"], row["sentence2"]), axis=1)

# Salva il dataset con le predizioni
df.to_csv("predicted_matches.tsv", sep="\t", index=False)

print("✅ Predizioni completate! Controlla predicted_matches.tsv")
