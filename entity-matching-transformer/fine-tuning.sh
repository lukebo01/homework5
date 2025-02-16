#!/bin/bash
# Impostiamo la variabile PYTHONPATH per il corretto funzionamento dei modelli
PYTHONPATH=$(pwd)

# Funzione di stampa a colori
cecho(){
    RED="\033[0;31m"
    GREEN="\033[0;32m"
    YELLOW="\033[1;33m"
    NC="\033[0m" # Nessun colore
    printf "${!1}${2} ${NC}\n"
}

# Impostiamo il seme per la riproducibilità
SEED=22

# Fine-tuning sul dataset di GT (ad esempio con la tua ground truth)
cecho "YELLOW" "Start fine-tuning with DistilBERT on your GT"
python ~/PA2/src/run_training.py --model_type=distilbert --model_name_or_path=distilbert-base-uncased --data_processor=DeepMatcherProcessor --data_dir=/path/to/your/gt_data --train_batch_size=16 --eval_batch_size=16 --max_seq_length=180 --num_epochs=5 --seed=${SEED}
