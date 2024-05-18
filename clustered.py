import pandas as pd

input_file_path = 'E:/TXDATA/clustered_transactions.csv'
df = pd.read_csv(input_file_path)
df = df[df['NATIVE_DROP_USD'] != 0]
cluster_data = {cluster: [] for cluster in df['Cluster'].unique()}
grouped = df.groupby(['Cluster', 'NATIVE_DROP_USD'])

for (cluster, native_drop), group in grouped:
    if len(group) > 1:
        if group['SENDER_WALLET'].duplicated().any():
            continue
        wallets_transactions = group.apply(lambda row: f"{row['SENDER_WALLET']}:{row['SOURCE_TRANSACTION_HASH']}", axis=1)
        cluster_data[cluster].append({'NATIVE_DROP_USD': native_drop, 'SENDER_WALLETS:SOURCE_TRANSACTION_HASH': list(wallets_transactions)})
for cluster, data in cluster_data.items():
    if data:
        output_file_path = f'E:/TXDATA/cluster_{cluster}_wallets_transactions.csv'
        cluster_df = pd.DataFrame(data)
        cluster_df.to_csv(output_file_path, index=False)
        print(f'Data for cluster {cluster} saved to file {output_file_path}')
