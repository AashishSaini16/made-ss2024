import os
import pandas as pd
from pipeline import download_and_save_data, rename_columns

# Define paths and URLs
data_path = 'C:/Users/aashi/Desktop/made-ss2024/project/data'
url1 = 'https://www.data.gouv.fr/fr/datasets/r/6f5cc4ea-ef1c-4a1d-b7c4-ad49f3970933'
url2 = 'https://www.data.gouv.fr/fr/datasets/r/670bf487-69dc-4b38-9255-c71a0dc1c41f'
file1 = os.path.join(data_path, 'gas-storage.csv')
file2 = os.path.join(data_path, 'gas-flow.csv')

# Test data download function
def test_download_and_save_data():
    # Test download for the first file
    download_and_save_data(file1, url1)
    assert os.path.isfile(file1), f"{file1} does not exist."

    # Test download for the second file
    download_and_save_data(file2, url2)
    assert os.path.isfile(file2), f"{file2} does not exist."

    print("============= Data download test passed successfully =============")

# Test renaming columns function
def test_rename_columns():
    # Define renaming mappings
    rename_map1 = {
        'date': 'date',
        'stock_fin_de_journee': 'stock_end_of_day'
    }
    rename_map2 = {
        'date': 'date',
        'debit_fin_de_journee': 'debit_end_of_day'
    }

    # Perform renaming
    df1, df2 = rename_columns(file1, file2, rename_map1, rename_map2)

    # Check if columns are renamed properly
    assert 'stock_end_of_day' in df1.columns, "First DataFrame columns were not renamed properly."
    assert 'debit_end_of_day' in df2.columns, "Second DataFrame columns were not renamed properly."

    print("============= Rename columns test passed successfully =============")

if __name__ == '__main__':
    test_download_and_save_data()
    test_rename_columns()