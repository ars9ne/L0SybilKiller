import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from datetime import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', 10)

file_path = 'E:/TXDATA/2024-05-15-snapshot1_transactions.csv'

chunksize = 10 ** 6
reader = pd.read_csv(file_path, chunksize=chunksize)

df = next(reader)

df[['NATIVE_DROP_USD', 'STARGATE_SWAP_USD']] = df[['NATIVE_DROP_USD', 'STARGATE_SWAP_USD']].fillna(0)

df['SOURCE_TIMESTAMP_UTC'] = pd.to_datetime(df['SOURCE_TIMESTAMP_UTC'])
df['SOURCE_TIMESTAMP_UTC'] = df['SOURCE_TIMESTAMP_UTC'].map(datetime.timestamp)

df['NATIVE_DROP_USD'] *= 2
df['SOURCE_TIMESTAMP_UTC'] /= 2

categorical_features = ['PROJECT', 'SOURCE_CHAIN', 'SOURCE_CONTRACT', 'DESTINATION_CHAIN', 'DESTINATION_CONTRACT']
numerical_features = ['NATIVE_DROP_USD', 'STARGATE_SWAP_USD', 'SOURCE_TIMESTAMP_UTC']

preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), numerical_features),
        ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_features)
    ])

kmeans = Pipeline(steps=[('preprocessor', preprocessor),
                         ('clusterer', KMeans(n_clusters=12, random_state=42, n_init='auto'))])

df['Cluster'] = kmeans.fit_predict(df)

output_file_path = 'E:/TXDATA/clustered_transactions.csv'
df.to_csv(output_file_path, index=False)

print(df[['SOURCE_CHAIN', 'SOURCE_TRANSACTION_HASH', 'Cluster']].head())

for cluster in range(12):
    print(f"\nCluster {cluster}:")
    print(df[df['Cluster'] == cluster].head())
