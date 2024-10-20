import pytest
import pandas as pd
import sqlite3
import os
from src.retail import ESretail
from src.etl_pipeline import transforme_transactions
import unittest

class TransactionTest(unittest.TestCase):

    def setUp(self):
        """
        Creates a new SQLite database for testing.

        """
        test_db_path = 'tests/retail_test.db'

        self.retail = ESretail(test_db_path)
        self.retail.cursor.execute('''

        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT,
            category TEXT,
            name TEXT,
            quantity BIGINT,
            amount_excl_tax FLOAT,
            amount_inc_tax FLOAT, 
            transaction_date TEXT
        )
        ''')
        self.retail.cursor.execute('DELETE FROM transactions')
        self.retail.conn.commit()

    def tearDown(self):
        self.retail.conn.close()


################    TEST BULK IMPORT    #################
    def test_bulk_empty(self):
        new_retail = self.retail
        self.assertEqual(new_retail.count_total_id(), 0)
        data_valid = {
            'id': [],
            'transaction_date': [],
            'category' : [],
            'name': [],
            'quantity': [],
            'amount_excl_tax': [],
            'amount_inc_tax': []}
        df = pd.DataFrame(data_valid)
        new_retail.bulk_import(df)
        self.assertEqual(new_retail.count_total_id(), 0)

    
    def test_bulk_valid(self):
        new_retail = self.retail
        self.assertEqual(new_retail.count_total_id(), 0)
        data_valid = {
            'id': ["94ca3d4f","9a348783","9e8e3262","aad54a55","ac82915d"],
            'transaction_date': ['2001_01_01','2001_01_01', '2001_01_01', '2001_01_01', '2001_01_01',],
            'category' : ["SELL","BUY","BUY","BUY","SELL"],
            'name': ["Fitbit Charge","Apple iPhone","Ray-Ban","Levis Jeans","Fitbit Charge"],
            'quantity': [4, 5, 5, 5, 4] ,
            'amount_excl_tax': [399.95,449.95,799.95,269.97,2199.98],
            'amount_inc_tax': [479.94,539.94,959.94,323.96,2639.98]}
        df = pd.DataFrame(data_valid)
        new_retail.bulk_import(df)
        self.assertEqual(new_retail.count_total_id(), 5)


    def test_bulk_duplicate(self):
        new_retail = self.retail
        self.assertEqual(new_retail.count_total_id(), 0)
        data_valid = {
            'id': ["94ca3d4f","9a348783","9e8e3262","aad54a55","ac82915d",],
            'transaction_date': ['2001_01_01','2001_01_01', '2001_01_01', '2001_01_01', '2001_01_01'],
            'category' : ["SELL","BUY","BUY","BUY","SELL"],
            'name': ["Fitbit Charge","Apple iPhone","Ray-Ban","Levis Jeans","Fitbit Charge"],
            'quantity': [4, 5, 5, 5, 4] ,
            'amount_excl_tax': [399.95,449.95,799.95,269.97,2199.98],
            'amount_inc_tax': [479.94,539.94,959.94,323.96,2639.98]}
        df = pd.DataFrame(data_valid)
        new_retail.bulk_import(df)
        self.assertEqual(new_retail.count_total_id(), 5)
        new_retail.bulk_import(df.head(2))
        self.assertEqual(new_retail.count_total_id(), 5)



################    TEST count transaction by date    #################
    def test_count_transactions_by_date_no_data(self):
        new_retail = self.retail
        self.assertEqual(new_retail.count_transactions_by_date('2001-01-01'), 0)


    def test_count_transactions_by_date_valid_data(self):
        new_retail = self.retail
        data_valid = {
            'id': ["94ca3d4f","9a348783"],
            'transaction_date': ['2001-01-01','2001-01-01'],
            'category': ["SELL","BUY"],
            'name': ["Fitbit Charge","Apple iPhone"],
            'quantity': [4, 5],
            'amount_excl_tax': [399.95, 449.95],
            'amount_inc_tax': [479.94, 539.94]
        }
        df = pd.DataFrame(data_valid)
        new_retail.bulk_import(df)
        self.assertEqual(new_retail.count_transactions_by_date('2001-01-01'), 2)


    def test_count_transactions_by_date_invalid_date(self):
        new_retail = self.retail
        data_valid = {
            'id': ["94ca3d4f"],
            'transaction_date': ['2001-01-01'],
            'category': ["SELL"],
            'name': ["Fitbit Charge"],
            'quantity': [4],
            'amount_excl_tax': [399.95],
            'amount_inc_tax': [479.94]
        }
        df = pd.DataFrame(data_valid)
        new_retail.bulk_import(df)
        self.assertEqual(new_retail.count_transactions_by_date('2000-01-01'), 0)


    def test_count_transactions_by_date_multiple_dates(self):
        new_retail = self.retail
        data_valid = {
            'id': ["94ca3d4f", "9a348783", "9e8e3262"],
            'transaction_date': ['2001-01-01', '2001-02-01', '2001-01-01'],
            'category': ["SELL", "BUY", "SELL"],
            'name': ["Fitbit Charge", "Apple iPhone", "Ray-Ban"],
            'quantity': [4, 5, 5],
            'amount_excl_tax': [399.95, 449.95, 799.95],
            'amount_inc_tax': [479.94, 539.94, 959.94]
        }
        df = pd.DataFrame(data_valid)
        new_retail.bulk_import(df)
        self.assertEqual(new_retail.count_transactions_by_date('2001-01-01'), 2)
        self.assertEqual(new_retail.count_transactions_by_date('2001-02-01'), 1)


        
################    TEST SUM TRANSACTION    ################
    def test_sum_total_transaction_no_data(self):
        new_retail = self.retail
        self.assertEqual(new_retail.sum_total_transaction(), 0)


    def test_sum_total_transaction_valid_data(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f","9a348783"],
            'transaction_date': ['2001-01-01','2001-01-01'],
            'category': ["SELL","BUY"],
            'name': ["Fitbit Charge","Apple iPhone"],
            'quantity': [4, 5],
            'amount_excl_tax': [399.95, 449.95],
            'amount_inc_tax': [479.94, 539.94]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        self.assertEqual(round(new_retail.sum_total_transaction(), 2), 1019.88)


    def test_sum_total_transaction_with_null(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f","9a348783"],
            'transaction_date': ['2001-01-01','2001-01-01'],
            'category': ["SELL","BUY"],
            'name': ["Fitbit Charge","Apple iPhone"],
            'quantity': [4, 5],
            'amount_excl_tax': [399.95, 449.95],
            'amount_inc_tax': [None, 539.94]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        self.assertEqual(new_retail.sum_total_transaction(), 539.94)


    def test_sum_total_transaction_multiple_entries(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783", "9e8e3262"],
            'transaction_date': ['2001-01-01', '2001-02-01', '2001-01-01'],
            'category': ["SELL", "BUY", "SELL"],
            'name': ["Fitbit Charge", "Apple iPhone", "Ray-Ban"],
            'quantity': [4, 5, 5],
            'amount_excl_tax': [399.95, 449.95, 799.95],
            'amount_inc_tax': [479.94, 539.94, 959.94]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        self.assertEqual(round(new_retail.sum_total_transaction(), 2), 1979.82)



################    TEST Get balance by date    #################
    def test_get_balance_by_date_sql_no_data(self):
        new_retail = self.retail
        result = new_retail.get_balance_by_date_sql("Amazon Echo Dot")
        self.assertTrue(result.empty)


    def test_get_balance_by_date_sql_valid_data(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783"],
            'transaction_date': ['2001-01-01', '2001-01-01'],
            'category': ["SELL", "BUY"],
            'name': ["Amazon Echo Dot", "Amazon Echo Dot"],
            'quantity': [10, 5],
            'amount_excl_tax': [100.00, 50.00],
            'amount_inc_tax': [120.00, 60.00]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        result = new_retail.get_balance_by_date_sql("Amazon Echo Dot")
        self.assertEqual(result.loc[0, 'balance'], 60)
        self.assertEqual(len(result), 1)


    def test_get_balance_by_date_sql_multiple_dates(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783", "9e8e3262"],
            'transaction_date': ['2001-01-01', '2001-02-01', '2001-01-01'],
            'category': ["SELL", "BUY", "SELL"],
            'name': ["Amazon Echo Dot", "Amazon Echo Dot", "Amazon Echo Dot"],
            'quantity': [10, 5, 3],
            'amount_excl_tax': [100.00, 50.00, 30.00],
            'amount_inc_tax': [120.00, 60.00, 36.00]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        result = new_retail.get_balance_by_date_sql("Amazon Echo Dot")
        self.assertEqual(result.loc[0, 'balance'], 156)
        self.assertEqual(result.loc[1, 'balance'], -60)
        self.assertEqual(len(result), 2)


    def test_get_balance_by_date_sql_no_matching_product(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783"],
            'transaction_date': ['2001-01-01', '2001-01-01'],
            'category': ["SELL", "BUY"],
            'name': ["Fitbit Charge", "Fitbit Charge"],
            'quantity': [10, 5],
            'amount_excl_tax': [100.00, 50.00],
            'amount_inc_tax': [120.00, 60.00]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        result = new_retail.get_balance_by_date_sql("Amazon Echo Dot")
        self.assertTrue(result.empty)


    def test_get_balance_by_date_sql_different_products(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783", "9e8e3262"],
            'transaction_date': ['2001-01-01', '2001-02-01', '2001-01-01'],
            'category': ["SELL", "BUY", "SELL"],
            'name': ["Amazon Echo Dot", "Fitbit Charge", "Amazon Echo Dot"],
            'quantity': [10, 5, 3],
            'amount_excl_tax': [100.00, 50.00, 30.00],
            'amount_inc_tax': [120.00, 60.00, 36.00]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        result = new_retail.get_balance_by_date_sql("Amazon Echo Dot")
        self.assertEqual(result.loc[0, 'balance'], 156)
        self.assertEqual(len(result), 1)


################    TEST Get cumulated balance by date    #################
    def test_get_cumulated_balance_by_date_no_data(self):
        new_retail = self.retail
        result = new_retail.get_cumulated_balance_by_date("Amazon Echo Dot")
        self.assertIsNone(result)


    def test_get_cumulated_balance_by_date_valid_data(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783"],
            'transaction_date': ['2001-01-01', '2001-01-01'],
            'category': ["SELL", "BUY"],
            'name': ["Amazon Echo Dot", "Amazon Echo Dot"],
            'quantity': [10, 5],
            'amount_excl_tax': [100.00, 50.00],
            'amount_inc_tax': [120.00, 60.00]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        result = new_retail.get_cumulated_balance_by_date("Amazon Echo Dot")
        self.assertEqual(result.loc[0, 'balance'], 60.00)
        self.assertEqual(result.loc[0, 'cumulated_balance'], 60.00)
        self.assertEqual(len(result), 1)


    def test_get_cumulated_balance_by_date_multiple_dates(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783", "9e8e3262"],
            'transaction_date': ['2001-01-01', '2001-02-01', '2001-01-01'],
            'category': ["SELL", "BUY", "SELL"],
            'name': ["Amazon Echo Dot", "Amazon Echo Dot", "Amazon Echo Dot"],
            'quantity': [10, 5, 3],
            'amount_excl_tax': [100.00, 50.00, 30.00],
            'amount_inc_tax': [120.00, 60.00, 36.00]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        result = new_retail.get_cumulated_balance_by_date("Amazon Echo Dot")
        self.assertEqual(result.loc[0, 'balance'], 156.00)
        self.assertEqual(result.loc[0, 'cumulated_balance'], 156.00)
        self.assertEqual(result.loc[1, 'balance'], -60.00)
        self.assertEqual(result.loc[1, 'cumulated_balance'], 96.00)


    def test_get_cumulated_balance_by_date_no_matching_product(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783"],
            'transaction_date': ['2001-01-01', '2001-01-01'],
            'category': ["SELL", "BUY"],
            'name': ["Fitbit Charge", "Fitbit Charge"],
            'quantity': [10, 5],
            'amount_excl_tax': [100.00, 50.00],
            'amount_inc_tax': [120.00, 60.00]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        result = new_retail.get_cumulated_balance_by_date("Amazon Echo Dot")
        self.assertIsNone(result)


    def test_get_cumulated_balance_by_date_different_products(self):
        new_retail = self.retail
        data = {
            'id': ["94ca3d4f", "9a348783", "9e8e3262"],
            'transaction_date': ['2001-01-01', '2001-02-01', '2001-01-01'],
            'category': ["SELL", "BUY", "SELL"],
            'name': ["Amazon Echo Dot", "Fitbit Charge", "Amazon Echo Dot"],
            'quantity': [10, 5, 3],
            'amount_excl_tax': [100.00, 50.00, 30.00],
            'amount_inc_tax': [120.00, 60.00, 36.00]
        }
        df = pd.DataFrame(data)
        new_retail.bulk_import(df)
        result = new_retail.get_cumulated_balance_by_date("Amazon Echo Dot")
        self.assertEqual(result.loc[0, 'balance'], 156.00)
        self.assertEqual(result.loc[0, 'cumulated_balance'], 156.00)
        self.assertEqual(len(result), 1)




# def test_full_etl_pipeline(test_db):
#     # Exécution du pipeline ETL
#     run_etl()

#     # Validation des données chargées
#     cursor = test_db.cursor()
#     cursor.execute("SELECT COUNT(*) FROM transactions")
#     result = cursor.fetchone()
    
#     # On s'attend à ce qu'il y ait 5 transactions dans la table
#     assert result[0] == 5

