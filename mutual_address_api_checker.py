import pandas as pd
import requests
import ast
from ratelimit import limits, sleep_and_retry

CALLS = 5
RATE_LIMIT_PERIOD = 1


@sleep_and_retry
@limits(calls=CALLS, period=RATE_LIMIT_PERIOD)
def get_transaction_receipt_status(txhash, api_key):
    url = f"https://api.ftmscan.com/api?module=transaction&action=gettxreceiptstatus&txhash={txhash}&apikey={api_key}"
    response = requests.get(url)
    data = response.json()

    if data['status'] != '1':
        print(f"Error retrieving transaction status {txhash}: {data['message']}. Full message: {data}")
        return None

    return data['result']


@sleep_and_retry
@limits(calls=CALLS, period=RATE_LIMIT_PERIOD)
def get_transactions(address, api_key):
    url = f"https://api.ftmscan.com/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={api_key}"
    response = requests.get(url)
    data = response.json()

    if data['status'] != '1':
        print(f"Error retrieving data for address {address}: {data['message']}. Full message: {data}")
        return []

    return data['result']


def find_mutual_transactions(address1, address2, api_key):
    txs1 = get_transactions(address1, api_key)
    txs2 = get_transactions(address2, api_key)

    mutual_tx_hashes = set()

    for tx in txs1:
        if tx.get('to') and tx['to'].lower() == address2.lower():
            status = get_transaction_receipt_status(tx['hash'], api_key)
            if status and status['status'] == '1':
                mutual_tx_hashes.add(tx['hash'])

    for tx in txs2:
        if tx.get('to') and tx['to'].lower() == address1.lower():
            status = get_transaction_receipt_status(tx['hash'], api_key)
            if status and status['status'] == '1':
                mutual_tx_hashes.add(tx['hash'])

    return mutual_tx_hashes


api_key = '4VH9VINZEYA93IJTYEGZPH38PXAH59PRPK'

file_paths = [
    'C:/Users/RYZEN9/PycharmProjects/L0/mnt/data/cluster_8_wallets_transactions.csv',
    'C:/Users/RYZEN9/PycharmProjects/L0/mnt/data/cluster_1_wallets_transactions.csv',
    'C:/Users/RYZEN9/PycharmProjects/L0/mnt/data/cluster_0_wallets_transactions.csv',
    'C:/Users/RYZEN9/PycharmProjects/L0/mnt/data/cluster_2_wallets_transactions.csv'
]

for file_path in file_paths:
    print(f"Processing file: {file_path}")

    df = pd.read_csv(file_path)

    results = []

    for index, row in df.iterrows():

        wallets_transactions = row['SENDER_WALLETS:SOURCE_TRANSACTION_HASH']
        wallets_transactions = ast.literal_eval(wallets_transactions)

        wallets = [wt.split(':')[0] for wt in wallets_transactions]

        unique_wallets = list(set(wallets))
        mutual_wallets = set()
        for i, address1 in enumerate(unique_wallets):
            for j, address2 in enumerate(unique_wallets):
                if i >= j:
                    continue
                mutual_tx_hashes = find_mutual_transactions(address1, address2, api_key)
                if mutual_tx_hashes:
                    mutual_wallets.update([address1, address2])
                    print(
                        f"Found mutual connection between {address1} and {address2} in record with NATIVE_DROP_USD {row['NATIVE_DROP_USD']}. Transactions: {mutual_tx_hashes}")

        if mutual_wallets:
            results.append({
                'NATIVE_DROP_USD': row['NATIVE_DROP_USD'],
                'SENDER_WALLETS': ','.join(mutual_wallets),
                'MUTUAL_ADDRESSES': list(mutual_wallets)
            })

    if results:
        output_file_path = file_path.replace('.csv', '_checked.csv')
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_file_path, index=False)
        print(f'Data saved to file {output_file_path}')
