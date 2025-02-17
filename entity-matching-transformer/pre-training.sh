#!/bin/bash

DATA_PATH=data/dirty_amazon_itunes/deep_matcher

PYTHON_EXE=python3  # Usa Python del container

RUN_PY_PATH=src/run_training.py

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

# Addestramento su un dataset simile (ad esempio dirty_amazon_itunes)
cecho "YELLOW" "Start pre-training with DistilBERT on dirty_amazon_itunes"
$PYTHON_EXE $RUN_PY_PATH --model_type=distilbert --model_name_or_path=distilbert-base-uncased --data_processor=DeepMatcherProcessor --data_dir=$DATA_PATH --train_batch_size=16 --eval_batch_size=16 --max_seq_length=180 --num_epochs=10 --seed=${SEED}
