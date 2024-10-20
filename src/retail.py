import sqlite3
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ESretail:
    def __init__(self, db_filename='retail.db'):
        # Path to the SQLite database at the project root
        self.db_path = os.path.join(os.path.dirname(__file__), '..', db_filename)

        try:
            # Connect to the SQLite database
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logging.info(f"Successfully connected to the database {self.db_path}.")
        except sqlite3.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise
        
    def close_connection(self):
        """Method to close the database connection"""
        if self.conn:
            self.conn.close()
            logging.info("Connection closed.")

    def bulk_import(self, df: pd.DataFrame, batch_size=20):
        """
        Insère les données dans la base de données SQLite par batch.

        :param df: DataFrame contenant les colonnes id, transaction_date, name, quantity, amount_excl_tax, amount_inc_tax
        :param batch_size: La taille du batch pour les insertions
        """
        # Log the process
        logging.info("Starting bulk import process")

        list_dict = df.to_dict(orient="records")
        sql_query = """
        INSERT OR IGNORE INTO transactions (id, category, name, quantity, amount_excl_tax, amount_inc_tax, transaction_date)
        VALUES (:id, :category, :name, :quantity, :amount_excl_tax, :amount_inc_tax, :transaction_date)
        """
        with self.conn:
            try:
                self.cursor.execute("SELECT id FROM transactions")
                existing_ids = set(row[0] for row in self.cursor.fetchall())  # Extraire tous les IDs existants
                filtered_metadata = [item for item in list_dict if item['id'] not in existing_ids]
                for i in range(0, len(filtered_metadata), batch_size):
                    batch_dict = filtered_metadata[i:i+batch_size]  # Extraire un batch
                    # Insertion en lot (batch insertions)
                    for dict in batch_dict:
                        if 'transaction_date' not in dict or dict.get('transaction_date') is None:
                            raise ValueError("TES OU la con")
                    self.cursor.executemany(sql_query, batch_dict)
                    self.conn.commit()  # Confirmer la transaction pour chaque batch                
                logging.info("Bulk import completed successfully.")
            except sqlite3.Error as e:
                # Log l'erreur en cas d'échec
                logging.error(f"An error occurred during bulk import: {e}")
                self.conn.rollback()  # Annule les changements si erreur
                raise  
            # finally:
            #     # Fermer la connexion après import
            #     self.close_connection()
            #     logging.info("Database connection closed.")    

    def count_transactions_by_date(self, transaction_date):
        """
        Counts the number of rows with a specific transaction_date.
        
        :param transaction_date: The date to search for in the format 'YYYY-MM-DD'.
        :return: The count of matching rows.
        """
        query = "SELECT COUNT(*) FROM transactions WHERE transaction_date = ?"
        
        try:
            with self.conn:
                
                self.cursor.execute(query, (transaction_date,))
                count = self.cursor.fetchone()[0]
                logging.info(f"Count of transactions on {transaction_date}: {count}")
                return count
        except sqlite3.Error as e:
            logging.error(f"Error executing query: {e}")
            return None
    
    def sum_total_transaction(self):
        """
        Returns the sum of the values amount_inc_tax column.
        
        :return: The sum of the values in the column, or None if an error occurs.
        """
        query = f"SELECT SUM(amount_inc_tax) FROM transactions"
        
        try:
            with self.conn:
                self.cursor.execute(query)
                total_sum = self.cursor.fetchone()[0]
                logging.info(f"Sum of amount_inc_tax: {total_sum}")
                return total_sum
        except sqlite3.Error as e:
            logging.error(f"Error executing query: {e}")
            return None
    
    def get_balance_by_date_sql(self, product_name="Amazon Echo Dot"):
        """
        Calculates the balance (SELL - BUY) by date for a specific product using SQL query.
        
        :param product_name: The name of the product to filter on, default is "Amazon Echo Dot".
        :return: A DataFrame with the balance (SELL - BUY) by date.
        """
        query = """
        SELECT transaction_date,
               SUM(CASE 
                       WHEN category = 'SELL' THEN quantity
                       WHEN category = 'BUY' THEN -quantity
                       ELSE 0
                   END) AS balance
        FROM transactions
        WHERE name = ?
        GROUP BY transaction_date
        ORDER BY transaction_date;
        """
        
        try:
            with self.conn:
                # Execute the SQL query and load the result into a pandas DataFrame
                balance_by_date = pd.read_sql_query(query, self.conn, params=(product_name,))
                logging.info(f"Balance by date calculated for {product_name} using SQL.")
                return balance_by_date
        except sqlite3.Error as e:
            logging.error(f"Error executing query: {e}")
            return None
    
    def get_cumulated_balance_by_date(self, product_name="Amazon Echo Dot"):
        """
        Calculates the cumulated balance (SELL - BUY) by date for a specific product.
        
        :param product_name: The name of the product to filter on, default is "Amazon Echo Dot".
        :return: A DataFrame with the cumulated balance by date.
        """
        query = """
        SELECT transaction_date,
               SUM(CASE 
                       WHEN category = 'SELL' THEN quantity
                       WHEN category = 'BUY' THEN -quantity
                       ELSE 0
                   END) AS balance
        FROM transactions
        WHERE name = ?
        GROUP BY transaction_date
        ORDER BY transaction_date;
        """
        
        try:
            with self.conn:
                # Execute the SQL query and load the result into a pandas DataFrame
                balance_by_date = pd.read_sql_query(query, self.conn, params=(product_name,))
                
                if balance_by_date.empty:
                    logging.info(f"No data found for product: {product_name}")
                    return None

                # Calculate the cumulated balance using pandas cumsum
                balance_by_date['cumulated_balance'] = balance_by_date['balance'].cumsum()

                logging.info(f"Cumulated balance by date calculated for {product_name}.")
                return balance_by_date
        except sqlite3.Error as e:
            logging.error(f"Error executing query: {e}")
            return None