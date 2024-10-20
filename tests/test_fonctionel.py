import pytest
import pandas as pd
import sqlite3
import os
from src.retail import ESretail
from src.etl_pipeline import *
import unittest

class TransactionTest(unittest.TestCase):

    def setUp(self):
        """
        Creates a new SQLite database for testing.

        """
        self.test_db_path = 'tests/retail_test.db'

        self.retail = ESretail(self.test_db_path)
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

    def test_etl_valid(self):
        df = transforme_transactions("tests", "retail_15_01_2022.csv")
        load_data(df, self.test_db_path)
        self.assertEqual(self.retail.count_total_id(), 10)

        df = transforme_transactions("tests", "retail_15_01_2022.csv")
        load_data(df, self.test_db_path)
        self.assertEqual(self.retail.count_total_id(), 10)
            
