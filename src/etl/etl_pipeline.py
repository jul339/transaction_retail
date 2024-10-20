from datetime import datetime
from typing import List, Tuple
import sqlite3
import os
import pandas as pd
import logging as log
import gzip
from src.retail import ESretail
log.basicConfig(filename='etl_log.log', 
                    level=log.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
class Cols :
    id = 'id'
    category = 'category'
    description = 'description'
    quantity = 'quantity'
    amount_excl_tax = 'amount_excl_tax'
    amount_inc_tax = 'amount_inc_tax'

def find_csv(folder_path: str) -> str:
    """
    Finds the CSV file in the specified folder path.

    Args:
        folder_path (str): The path to the folder containing the CSV file.

    Returns:
        str: The name of the CSV file found in the folder.

    Raises:
        FileExistsError: If there are multiple CSV files in the folder.
        FileNotFoundError: If no CSV files are found in the folder.
    """
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    if len(csv_files) > 1:
        raise FileExistsError("Data folder should have only one file")
    if len(csv_files) == 0:
        raise FileNotFoundError("Data folder should have one file")
    return csv_files[0]

def extract() -> tuple[str, str]:
    """
    Extracts data from a CSV file located in a specified folder and saves it in a structured format in the datalake.

    Returns:
        tuple: A tuple containing:
            - (str) raw_data_folder: The path of the folder where the raw data is saved.
            - (str) csv_file_name: The name of the CSV file extracted.
    
    Raises:
        FileNotFoundError: If the data folder does not contain exactly one CSV file.
        OSError: If there is an issue creating the raw data folder.
    """
    current_path = os.path.dirname(os.path.abspath(__file__))
    datalake_path = os.path.abspath(os.path.join(current_path, '..', 'datalake'))
    csv_file_name = find_csv(os.path.abspath(os.path.join(current_path, '..', 'data')))
    csv_file_path = os.path.join('data', csv_file_name)
    raw_data_folder = os.path.join(
        datalake_path,
        csv_file_name.split("_")[3][0:4],
        csv_file_name.split("_")[2],
        csv_file_name.split("_")[1],
    )
    incoming_file_path = os.path.join(raw_data_folder, csv_file_name)

    # Save raw data in datalake 
    if not os.path.exists(raw_data_folder):
        os.makedirs(raw_data_folder)
        log.info(f"The folder: {raw_data_folder} has been created")
    else:
        log.warning(f"The folder: {raw_data_folder} already exists")

    with open(csv_file_path, 'rb') as source_file:
        with open(incoming_file_path, 'wb') as target_file:
            target_file.write(source_file.read())
        
    return raw_data_folder, csv_file_name


def read_transaction_file(df:pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Reads a transaction DataFrame and validates its contents.

    Args:
        df (pd.DataFrame): The DataFrame containing transaction data.

    Returns:
        tuple: A tuple containing:
            - (pd.DataFrame) clean_df: A DataFrame with valid transaction entries.
            - (List[str]) bad_lines: A list of IDs for entries that could not be processed.

    Raises:
        KeyError: If the DataFrame does not contain the required columns or has extra columns.
    """
    required_columns = {Cols.id, Cols.category, Cols.description, Cols.quantity, Cols.amount_excl_tax, Cols.amount_inc_tax}
    if not required_columns.issubset(df.columns) or len(df.columns) != len(required_columns):
        raise KeyError("The columns of the DataFrame don't correspond to the columns of the database.")

    bad_lines = []
    clean_lines = []

    for i in range(df.shape[0]):
        try:
            id = str(df[Cols.id].iloc[i])
        except ValueError:
            continue  # Skip the entry if ID cannot be converted to string.

        try:
            category = str(df[Cols.category].iloc[i])
            description = str(df[Cols.description].iloc[i])
            quantity = int(df[Cols.quantity].iloc[i])
            amount_excl_tax = round(float(df[Cols.amount_excl_tax].iloc[i]), 2)
            amount_inc_tax = round(float(df[Cols.amount_inc_tax].iloc[i]), 2)
        except ValueError:
            bad_lines.append(id)  # Add the ID to bad_lines if any conversion fails.
            continue  # Skip the entry if any field cannot be converted.

        transaction_dict = {
            Cols.id: id,
            Cols.category: category,
            Cols.description: description,
            Cols.quantity: quantity,
            Cols.amount_excl_tax: amount_excl_tax,
            Cols.amount_inc_tax: amount_inc_tax
        }
        clean_lines.append(transaction_dict)

    clean_df = pd.DataFrame(
        clean_lines,
        columns=[
            Cols.id,
            Cols.category,
            Cols.description,
            Cols.quantity,
            Cols.amount_excl_tax,
            Cols.amount_inc_tax
        ]
    )

    return clean_df, bad_lines

    
def transforme_transactions(incoming_file_path: str, file_name: str) -> pd.DataFrame:
    """
    Transforms transaction data from a CSV file into a cleaned DataFrame and saves it as a Parquet file.

    Args:
        incoming_file_path (str): The path to the folder containing the incoming CSV file.
        file_name (str): The name of the CSV file to be transformed.

    Returns:
        pd.DataFrame: A DataFrame containing unique transaction entries with a transaction date.

    Raises:
        FileNotFoundError: If the incoming file path does not exist.
        pd.errors.EmptyDataError: If the CSV file is empty or cannot be read.
    """
    df = pd.read_csv(os.path.join(incoming_file_path, file_name))
    parquet_retail_file_name = f"retail_data{file_name[7:0]}.parquet"
    parquet_retail_path = os.path.join(incoming_file_path, parquet_retail_file_name)

    clean_df, bad_lines = read_transaction_file(df)

    unique_df = clean_df[~clean_df['id'].duplicated(keep=False)]

    file_year = file_name.split("_")[3][0:4]
    file_month = file_name.split("_")[2]
    file_day = file_name.split("_")[1]
    unique_df['transaction_date'] = f"{file_year}-{file_month}-{file_day}"

    unique_df.rename(columns={'description': 'name'}, inplace=True)

    if not os.path.exists(parquet_retail_path):
        if bad_lines:
            log.warning(f"Bad line(s) in the file: {incoming_file_path}\nID of the bad lines: {bad_lines[0]}")
            for line in bad_lines[1:0]:
                log.warning(line)
        clean_df.to_parquet(parquet_retail_path, index=False)

    return unique_df


def load_data(df: pd.DataFrame, db_file_name: str) -> None:
    """
    Loads transaction data into a SQLite database.

    Args:
        df (pd.DataFrame): The DataFrame containing transaction data to be loaded.
        db_file_name (str): The name of the SQLite database file.

    Returns:
        None

    Raises:
        Exception: If there is an error during the bulk import of data.
    """
    retail = ESretail(db_file_name)
    try:
        retail.bulk_import(df)
    except Exception as e:
        log.error(f"Error during bulk import: {e}")
        raise
    finally:
        retail.conn.close()



# transforme_transactions(extract())
def run_etl():
    incoming_file_path, file_name = extract()
    clean_df = transforme_transactions(incoming_file_path, file_name)
    load_data(clean_df)

if __name__ == "__main__":
    extract()