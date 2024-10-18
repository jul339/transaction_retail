from datetime import datetime
import sqlite3
import os
import pandas as pd
import logging as log
import gzip
CSV_FILE_PATH = "D:\\perso\\prog\\retail_transaction\\retail_15_01_2022.csv"
DATALAKE_PATH = "D:\\perso\\prog\\retail_transaction\\datalake"
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

def extract():
    file_name = CSV_FILE_PATH.split("\\")[-1]
    raw_data_folder = os.path.join(
    DATALAKE_PATH,
    file_name.split("_")[3][0:4],
    file_name.split("_")[2],
    file_name.split("_")[1],
    )
    incoming_file_path = os.path.join(raw_data_folder, file_name)

    # Save brute data in datalake 
    if not os.path.exists(raw_data_folder):
        os.makedirs(raw_data_folder)
        log.info(f"The folder : {raw_data_folder} has been created")
    else:
        log.warning(f"The folder : {raw_data_folder} already exist")

    with open(CSV_FILE_PATH, 'rb') as source_file:
        with open(incoming_file_path, 'wb') as target_file:
            target_file.write(source_file.read())
        
    return raw_data_folder, file_name

def read_transaction_file(df: pd.DataFrame):
    required_columns = {Cols.id, Cols.category, Cols.description, Cols.quantity, Cols.amount_excl_tax, Cols.amount_inc_tax}
    if not required_columns.issubset(df.columns) or len(df.columns) != len(required_columns):
        raise KeyError("The colomn of the dataframe doesn't correspond the colomn of the database")
    bad_lines = []
    clean_lines = []
    for i in range(df.shape[0]):
        try :
            id = str(df[Cols.id].iloc[i])
            log.info("Converted 'id' column to str.")
        except ValueError:
            continue

        try:
            category = str(df[Cols.category].iloc[i])
            description = str(df[Cols.description].iloc[i])
            quantity = int(df[Cols.quantity].iloc[i])
            amount_excl_tax = float(df[Cols.amount_excl_tax].iloc[i])
            amount_inc_tax = float(df[Cols.amount_inc_tax].iloc[i])

        except ValueError as e:
            bad_lines.append(id)
            continue
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
    
def transforme_transactions(incoming_file_path, file_name):
    df = pd.read_csv(os.path.join(incoming_file_path, file_name))
    clean_df, bad_lines = read_transaction_file(df)
    unique_df = clean_df[~clean_df['id'].duplicated(keep=False)]
    parquet_retail_file_name = f"retail_data{file_name[7:0]}.parquet"
    parquet_retail_path = os.path.join(incoming_file_path, parquet_retail_file_name)
    file_year = file_name.split("_")[3][0:4]
    file_month = file_name.split("_")[2]
    file_day = file_name.split("_")[1]
    df['tansaction_date'] = f"{file_year}-{file_month}-{file_day}"
 
    if not os.path.exists(parquet_retail_path):
        if bad_lines:
            log.warning(f"Bad line(s) in the file : {incoming_file_path}\n ID of the bad lines : {bad_lines[0]}")
            for line in bad_lines[1:0]:
                log.warning(line)
        clean_df.to_parquet(parquet_retail_path, index=False)


def load(**kwargs):
    # Get transformed data
    return

# transforme_transactions(extract())
def main():
    inco, fil = extract()
    transforme_transactions(inco, fil)

if __name__ == "__main__":
    main()
