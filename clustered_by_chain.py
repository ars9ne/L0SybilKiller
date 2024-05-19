import pandas as pd


input_file_path = 'E:/TXDATA/clustered_transactions.csv'

df = pd.read_csv(input_file_path)

# Filter data by SOURCE_CHAIN "Arbitrum" and exclude records with NATIVE_DROP_USD equal to 0
df = df[(df['SOURCE_CHAIN'] == 'Arbitrum') & (df['NATIVE_DROP_USD'] != 0)]

# Create a dictionary to store data by clusters
cluster_data = {cluster: [] for cluster in df['Cluster'].unique()}

# Group data by clusters and NATIVE_DROP_USD
grouped = df.groupby(['Cluster', 'NATIVE_DROP_USD'])

# Fill the dictionary with data
for (cluster, native_drop), group in grouped:
    if len(group) > 1:
        # Check for duplication of SENDER_WALLET
        if group['SENDER_WALLET'].duplicated().any():
            continue
        wallets_transactions = group.apply(lambda row: f"{row['SENDER_WALLET']}:{row['SOURCE_TRANSACTION_HASH']}", axis=1)
        cluster_data[cluster].append({'NATIVE_DROP_USD': native_drop, 'SENDER_WALLETS:SOURCE_TRANSACTION_HASH': list(wallets_transactions)})

# Save data to separate files for each cluster
for cluster, data in cluster_data.items():
    if data:  # Save the file only if there is data for the cluster
        output_file_path = f'C:/Users/RYZEN9/PycharmProjects/L0/mnt/data/cluster_{cluster}_wallets_transactions.csv'
        cluster_df = pd.DataFrame(data)
        cluster_df.to_csv(output_file_path, index=False)
        print(f'Data for cluster {cluster} saved to file {output_file_path}')
