import os
import requests
import pandas as pd
import sqlite3
from pipeline import download_and_save_data, rename_columns

# Define paths and URLs
url1 = 'https://www.data.gouv.fr/fr/datasets/r/6f5cc4ea-ef1c-4a1d-b7c4-ad49f3970933'
url2 = 'https://www.data.gouv.fr/fr/datasets/r/670bf487-69dc-4b38-9255-c71a0dc1c41f'
data_directory = 'data'
file1 = os.path.join(data_directory, 'gas-storage.csv')
file2 = os.path.join(data_directory, 'gas-flow.csv')
# database_path = os.path.join(data_directory, 'combined_data.sqlite')


# Function to load data into DataFrames
""" def load_data_into_dataframes():
    print("Loading data into DataFrames...")
    df1 = pd.read_csv(file1, sep=';')
    df2 = pd.read_csv(file2, sep=';')

    assert not df1.empty, "First DataFrame is empty."
    assert not df2.empty, "Second DataFrame is empty."

    print("Data loaded successfully into DataFrames")

# Function to merge data
def merge_data():
    print("Merging data...")
    df1 = pd.read_csv(file1, sep=';')
    df2 = pd.read_csv(file2, sep=';')

    rename_map1 = {
        'date': 'date',
        'stock_fin_de_journee': 'stock_end_of_day'
    }

    rename_map2 = {
        'date': 'date',
        'debit_fin_de_journee': 'debit_end_of_day'
    }

    df1.rename(columns=rename_map1, inplace=True)
    df2.rename(columns=rename_map2, inplace=True)

    df1['date'] = pd.to_datetime(df1['date'])
    df2['date'] = pd.to_datetime(df2['date'])

    shared_sources = set(df1['source'].unique()).intersection(df2['source'].unique())
    shared_pits = set(df1['pits'].unique()).intersection(df2['pits'].unique())

    df1_filtered = df1[df1['source'].isin(shared_sources) & df1['pits'].isin(shared_pits)]
    df2_filtered = df2[df2['source'].isin(shared_sources) & df2['pits'].isin(shared_pits)]

    merged_df = pd.merge(df1_filtered, df2_filtered, on=['date', 'source', 'pits'], how='inner')

    assert not merged_df.empty, "Merged DataFrame is empty."
    assert 'stock_end_of_day' in merged_df.columns and 'debit_end_of_day' in merged_df.columns, "Merged DataFrame columns are incorrect."

    print("Data merging completed successfully")

# Function to save merged data to SQLite
def save_merged_data_to_sqlite():
    print("Saving merged data to SQLite database...")
    df1 = pd.read_csv(file1, sep=';')
    df2 = pd.read_csv(file2, sep=';')

    rename_map1 = {
        'date': 'date',
        'stock_fin_de_journee': 'stock_end_of_day'
    }

    rename_map2 = {
        'date': 'date',
        'debit_fin_de_journee': 'debit_end_of_day'
    }

    df1.rename(columns=rename_map1, inplace=True)
    df2.rename(columns=rename_map2, inplace=True)

    df1['date'] = pd.to_datetime(df1['date'])
    df2['date'] = pd.to_datetime(df2['date'])

    shared_sources = set(df1['source'].unique()).intersection(df2['source'].unique())
    shared_pits = set(df1['pits'].unique()).intersection(df2['pits'].unique())

    df1_filtered = df1[df1['source'].isin(shared_sources) & df1['pits'].isin(shared_pits)]
    df2_filtered = df2[df2['source'].isin(shared_sources) & df2['pits'].isin(shared_pits)]

    merged_df = pd.merge(df1_filtered, df2_filtered, on=['date', 'source', 'pits'], how='inner')

    conn = sqlite3.connect(database_path)
    merged_df.to_sql('merged_data', conn, if_exists='replace', index=False)
    conn.close()

    assert os.path.exists(database_path), "Failed to create SQLite database file."

    print("Data saved to SQLite database successfully")
 """
def test_download_and_save_data():
    download_and_save_data(file=file1, url=url1)
    assert os.path.isfile(file1)
    
    
def test_rename_columns():
    # test the rename_columns function
    rename_map1 = {
        'date': 'date',
        'stock_fin_de_journee': 'stock_end_of_day'
    }

    rename_map2 = {
        'date': 'date',
        'debit_fin_de_journee': 'debit_end_of_day'
    }
    
    df1, df2  = rename_columns(file1, file2, rename_map1, rename_map2)
    assert 'stock_end_of_day' in df1.columns, "First DataFrame columns were not renamed properly."
    assert 'debit_end_of_day' in df2.columns, "Second DataFrame columns were not renamed properly."


    print("============= Rename columns test passed successfully =============")


if __name__ == '__main__':
    test_rename_columns()