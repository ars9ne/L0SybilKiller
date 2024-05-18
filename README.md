Overview

This SybilKiller focuses on clustering blockchain transaction data to identify patterns and potential Sybil addresses. The process involves data collection, preprocessing, feature scaling, clustering using the K-Means algorithm, and analysis of the resulting clusters.

Features

Data Preprocessing: Cleans and prepares data for clustering.
Feature Scaling and Encoding: Adjusts the significance of different features for clustering.
Clustering: Uses K-Means algorithm to group transactions into clusters.
Analysis: Identifies unique patterns and potential Sybil addresses.
Visualization: Uses the Elbow and Silhouette methods to determine the optimal number of clusters.

Installation

To use this project, ensure you have the following dependencies installed:

pandas
scikit-learn
matplotlib
tqdm
joblib
You can install the required packages using pip:
bash
Copy code
pip install pandas scikit-learn matplotlib tqdm joblib


Usage

Step 1: Determine Optimal Number of Clusters

Load the transaction data.
Sample a subset of the data for efficient processing.
Scale the data and compute K-Means for a range of cluster numbers.
Use the Elbow and Silhouette methods to determine the optimal number of clusters.
Apply the optimal number of clusters and print the results.

Step 2: Preprocessing and Clustering

Load and preprocess the transaction data.
Fill missing values and transform timestamps to numerical values.
Scale the features, giving more weight to NATIVE_DROP_USD and less to timestamps.
Define categorical and numerical features for clustering.
Use K-Means clustering to group the transaction data into 12 clusters.
Save the clustered data to a CSV file.
Print the results and analyze the clusters.

Step 3: Identify Unique Wallets

Load the clustered data.
Exclude records with NATIVE_DROP_USD equal to 0.
Group the data by clusters and NATIVE_DROP_USD.
Identify unique wallets for each group and ensure there are no duplicate SENDER_WALLET addresses within the same NATIVE_DROP_USD group.
Save the unique wallet data to separate CSV files for each cluster.
