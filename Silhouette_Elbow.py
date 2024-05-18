import pandas as pd
from sklearn.cluster import KMeans, MiniBatchKMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from sklearn.metrics import silhouette_score
from tqdm import tqdm
from joblib import Parallel, delayed
import numpy as np

file_path = 'E:/TXDATA/2024-05-15-snapshot1_transactions.csv'

chunksize = 10 ** 5
reader = pd.read_csv(file_path, chunksize=chunksize)

df = next(reader)

df_selected = df[['NATIVE_DROP_USD', 'STARGATE_SWAP_USD']].fillna(0)

sample_size = 10000
df_sampled = df_selected.sample(n=sample_size, random_state=42)

scaler = StandardScaler()
df_scaled = scaler.fit_transform(df_sampled)

def compute_kmeans(k):
    kmeans = MiniBatchKMeans(n_clusters=k, random_state=42, batch_size=10000)
    labels = kmeans.fit_predict(df_scaled)
    sse = kmeans.inertia_
    silhouette_avg = silhouette_score(df_scaled, labels)
    return sse, silhouette_avg

k_range = range(2, 20)
results = Parallel(n_jobs=-1)(delayed(compute_kmeans)(k) for k in tqdm(k_range, desc="Finding optimal k"))

sse, silhouette_scores = zip(*results)

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

ax1.plot(k_range, sse, 'bo-')
ax1.set_xlabel('Number of clusters k')
ax1.set_ylabel('Sum of squared errors (SSE)')
ax1.set_title('Elbow method for determining optimal k')

ax2.plot(k_range, silhouette_scores, 'bo-')
ax2.set_xlabel('Number of clusters k')
ax2.set_ylabel('Silhouette coefficient')
ax2.set_title('Silhouette method for determining optimal k')

plt.show()

optimal_k = 3
kmeans = MiniBatchKMeans(n_clusters=optimal_k, random_state=42, batch_size=10000)
df['Cluster'] = kmeans.fit_predict(scaler.transform(df_selected.fillna(0)))

print(df[['SOURCE_CHAIN', 'SOURCE_TRANSACTION_HASH', 'Cluster']].head())

for cluster in range(optimal_k):
    print(f"\nCluster {cluster}:")
    print(df[df['Cluster'] == cluster].head())
