import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity


def ordina_per_terzo_elemento(lista):
    return sorted(lista, key=lambda item: item[2], reverse=True)

# 🔹 1️⃣ Carichiamo il modello di embedding
model = SentenceTransformer('all-MiniLM-L6-v2')

# 🔹 2️⃣ Leggiamo il dataset
def read_schema(path: str):
    df = pd.read_csv(path, low_memory=False)
    df["text"] = df["company_name"].astype(str) + "|" + df["headquarters_country"].astype(str)
    return df

df = read_schema("./final_mediated_schema.csv")

# 🔹 3️⃣ Creiamo gli embeddings con pulizia dei dati
df["embedding"] = df["text"].apply(lambda x: model.encode(str(x)) if pd.notna(x) else np.zeros(384))

# 🔹 4️⃣ Convertiamo in matrice numpy
X = np.vstack(df["embedding"].values)

# 🔹 5️⃣ Blocking con KMeans
num_clusters = 50  # Regola questo valore in base ai tuoi dati
kmeans = KMeans(n_clusters=num_clusters, random_state=42)
df["cluster"] = kmeans.fit_predict(X)

# 🔹 6️⃣ Pairwise Matching nei blocchi con Cosine Similarity
similar_pairs = []

for cluster_id in df["cluster"].unique():
    subset = df[df["cluster"] == cluster_id]  # Prendiamo solo aziende dello stesso cluster
    
    if len(subset) > 1:  # Se c'è più di un'azienda nel blocco
        embeddings_matrix = np.vstack(subset["embedding"])
        similarity_matrix = cosine_similarity(embeddings_matrix)

        for i in range(len(subset)):
            for j in range(i+1, len(subset)):  # Evitiamo confronti duplicati
                if similarity_matrix[i, j] > 0.8:  # Soglia di similarità
                    similar_pairs.append((subset.iloc[i]["company_name"], 
                                          subset.iloc[j]["company_name"], 
                                          similarity_matrix[i, j]))

file = open("result.txt", "w")
similar_pairs = ordina_per_terzo_elemento(similar_pairs)

for i,pair in enumerate(similar_pairs):
    #print(f"{pair[0]} <-> {pair[1]} (similarità: {pair[2]:.2f})")
    file.write(f"{pair[0]} | {pair[1]} | {pair[2]:.2f}\n")
    if i == 50000:
        break
file.close()