import pandas as pd
import numpy as np
import unittest
from src.etl_pipeline import read_transaction_file


class TransactionTest(unittest.TestCase):
    

    def test_transforme_error(self):
        """Test case where DataFrame is empty. Expecte raising ValueError."""
        data_None = {}
        df = pd.DataFrame(data_None)
        self.assertRaises(KeyError,read_transaction_file,df)


    def test_transforme_empty(self):
        """Test case where DataFrame is empty. Excepted empty df and error lines"""
        data_empty = [
            'id',
            'category',
            'description',
            'quantity',
            'amount_excl_tax',
            'amount_inc_tax'
            ]
        df = pd.DataFrame(columns=data_empty)
        res_df, res_badlines = read_transaction_file(df)
        self.assertTrue(res_df.empty)
        self.assertEqual(len(res_badlines), 0)

    def test_transforme_no_col(self):
        """Test case where DataFrame does have id column. Expecte raising ValueError"""
        data_no_id = {
            'category' : ["SELL","BUY","BUY","BUY","SELL"],
            'description': ["Fitbit Charge","Apple iPhone","Ray-Ban","Levis Jeans","Fitbit Charge"],
            'quantity': [4, 5, 5, 5, 4] ,
            'amount_excl_tax': [399.95,449.95,799.95,269.97,2199.98],
            'amount_inc_tax': [479.94,539.94,959.94,323.96,2639.98]}
        df = pd.DataFrame(data_no_id)
        self.assertRaises(KeyError,read_transaction_file,df)

    
    def test_transform_valid(self):
        data_valid = {
            'id': ["94ca3d4f","9a348783","9e8e3262","aad54a55","ac82915d"],
            'category' : ["SELL","BUY","BUY","BUY","SELL"],
            'description': ["Fitbit Charge","Apple iPhone","Ray-Ban","Levis Jeans","Fitbit Charge"],
            'quantity': [4, 5, 5, 5, 4] ,
            'amount_excl_tax': [399.95,449.95,799.95,269.97,2199.98],
            'amount_inc_tax': [479.94,539.94,959.94,323.96,2639.98]}
        df = pd.DataFrame(data_valid)
        res_df, res_badlines = read_transaction_file(df)
        res_types_cols = [str(x) for x in res_df.dtypes]
        expected_types = ['object', 'object', 'object', 'int64', 'float64', 'float64']
        self.assertEqual(len(res_df), 5)
        self.assertEqual(res_types_cols, expected_types)
        self.assertEqual(res_df.loc[res_df['id'] == '94ca3d4f', 'amount_inc_tax'].values[0], float(479.94))
        self.assertEqual(res_df.loc[res_df['id'] == '9a348783', 'quantity'].values[0], int(5))
        self.assertEqual(len(res_badlines), 0)
        
    def test_transform_invalid_lines(self):
        data_invalid = {
            'id': ["94ca3d4f","9a348783","9e8e3262","aad54a55","ac82915d"],
            'category' : ["SELL",10,"BUY","BUY","SELL"],
            'description': ["Fitbit Charge","Apple iPhone","Ray-Ban","Levis Jeans","Fitbit Charge"],
            'quantity': ["ERROR", 5, 5, 5, 4] ,
            'amount_excl_tax': [399.95,449.95,799.95,269.97,"ERROR"],
            'amount_inc_tax': [479.94,539.94,959.94,323.96,2639.98]}
        df = pd.DataFrame(data_invalid)
        res_df, res_badlines = read_transaction_file(df)
        res_types_cols = [str(x) for x in res_df.dtypes]
        expected_types = ['object', 'object', 'object', 'int64', 'float64', 'float64']
        self.assertEqual(len(res_df), 3)
        self.assertEqual(res_types_cols, expected_types)
        self.assertEqual(res_df.loc[res_df['id'] == '9e8e3262', 'amount_inc_tax'].values[0], float(959.94))
        self.assertEqual(res_df.loc[res_df['id'] == 'aad54a55', 'quantity'].values[0], int(5))
        self.assertEqual(len(res_badlines), 2)
        self.assertIn("94ca3d4f", res_badlines)
        self.assertIn("ac82915d", res_badlines)

    # @pytest.fixture
    # def test_db():
    #     # Setup: Créer une base de données de test
    #     test_db_path = 'tests/test_database.db'
    #     conn = sqlite3.connect(test_db_path)
    #     yield conn
    #     conn.close()
    #     os.remove(test_db_path)

if __name__ == '__main__':
    unittest.main()