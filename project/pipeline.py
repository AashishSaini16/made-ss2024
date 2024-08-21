import os
import requests
import pandas as pd
import sqlite3

# Use relative paths
data_path = 'data'
data_directory = 'data'

# Function to download and save data
def download_and_save_data(file, url):
    print(f"Starting data download for {file} from {url}...")
    os.makedirs(data_directory, exist_ok=True)
    response = requests.get(url)
    
    if response.status_code == 200:
        with open(file, 'wb') as f:
            f.write(response.content)
        print(f"Data download completed successfully for {file}")
    else:
        print(f"Failed to download data from {url}. Status code: {response.status_code}")

# Function to rename columns
def rename_columns(file1, file2, rename_map1, rename_map2):
    print("Renaming columns...")
    df1 = pd.read_csv(file1, sep=';')
    df2 = pd.read_csv(file2, sep=';')

    df1.rename(columns=rename_map1, inplace=True)
    df2.rename(columns=rename_map2, inplace=True)
    print("Column renaming completed successfully")
    return [df1, df2]

# URLs of the datasets
first_url = 'https://www.data.gouv.fr/fr/datasets/r/6f5cc4ea-ef1c-4a1d-b7c4-ad49f3970933'
second_url = 'https://www.data.gouv.fr/fr/datasets/r/670bf487-69dc-4b38-9255-c71a0dc1c41f'

# File paths (relative)
first_file = os.path.join(data_path, 'gas-storage.csv')
second_file = os.path.join(data_path, 'gas-flow.csv')
db_file = os.path.join(data_path, 'combined_data.sqlite')

# Download the data
print('Downloading data...')
download_and_save_data(first_file, first_url)
download_and_save_data(second_file, second_url)
print('Data downloaded and saved successfully to directory: {}'.format(data_path))

# Load the data
print('Loading data...')
data1 = pd.read_csv(first_file, sep=';')
data2 = pd.read_csv(second_file, sep=';')

# Define the column renaming dictionary for both datasets
rename_dict1 = {
    'date': 'date',
    'stock_fin_de_journee': 'stock_end_of_day'
}

rename_dict2 = {
    'date': 'date',
    'debit_fin_de_journee': 'debit_end_of_day'
}

# Rename the columns in data1 and data2
data1.rename(columns=rename_dict1, inplace=True)
data2.rename(columns=rename_dict2, inplace=True)

# Ensure the date columns are in datetime format for accurate merging
data1['date'] = pd.to_datetime(data1['date'])
data2['date'] = pd.to_datetime(data2['date'])

# Identify common sources and pits
common_sources = set(data1['source'].unique()).intersection(set(data2['source'].unique()))
common_pits = set(data1['pits'].unique()).intersection(set(data2['pits'].unique()))

# Filter datasets to only include common sources and pits
data1_filtered = data1[data1['source'].isin(common_sources) & data1['pits'].isin(common_pits)]
data2_filtered = data2[data2['source'].isin(common_sources) & data2['pits'].isin(common_pits)]

# Merge the data on the common dates column, keeping only intersecting dates
print('Merging data...')
merged_data = pd.merge(data1_filtered, data2_filtered, on=['date', 'source', 'pits'], how='inner')

# Save the merged data to a single SQLite database
print('Saving merged data to SQLite database...')
conn = sqlite3.connect(db_file)
merged_data.to_sql('merged_data', conn, if_exists='replace', index=False)
conn.close()

print('Merged data saved to SQLite database successfully')
