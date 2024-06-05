import requests
import os
import pandas as pd
import sqlite3

data_path = 'data'

# Create a directory to store the data
os.makedirs(data_path, exist_ok=True)

# URLs of the datasets
first_url = 'https://www.data.gouv.fr/fr/datasets/r/6f5cc4ea-ef1c-4a1d-b7c4-ad49f3970933'
second_url = 'https://www.data.gouv.fr/fr/datasets/r/670bf487-69dc-4b38-9255-c71a0dc1c41f'

# File paths
first_file = os.path.join(data_path, 'gas-storage.csv')
second_file = os.path.join(data_path, 'gas-flow.csv')
db_file = os.path.join(data_path, 'combined_data.sqlite')

# Download the data
print('Downloading data...')

# Download the first file
response = requests.get(first_url)
with open(first_file, 'wb') as f:
    f.write(response.content)

# Download the second file
response = requests.get(second_url)
with open(second_file, 'wb') as f:
    f.write(response.content)

print('Data downloaded and saved successfully to directory: {}'.format(data_path))

# Load the data
print('Loading data...')
data1 = pd.read_csv(first_file, sep=';')
data2 = pd.read_csv(second_file, sep=';')

# Ensure the date columns are in datetime format for accurate merging
data1['date'] = pd.to_datetime(data1['date'])
data2['date'] = pd.to_datetime(data2['date'])

# Merge the data on the common dates column, keeping only intersecting dates
print('Merging data...')
merged_data = pd.merge(data1, data2, on='date', how='inner')

# Save the merged data to a single SQLite database
print('Saving merged data to sqlite database...')
conn = sqlite3.connect(db_file)
merged_data.to_sql('merged_data', conn, if_exists='replace', index=False)
conn.close()

print('Merged data saved to sqlite database successfully')