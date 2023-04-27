import sqlite3
import pandas as pd

class DatabaseManager:

    def __init__(self):
        self.connection = sqlite3.connect('data.db')

    # Creates the SQLite database table named 'master'
    def create_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS `master` (
                `id` INT AUTO_INCREMENT,
                `date` DATE(20),
                `ticker` VARCHAR(16),
                `type` VARCHAR(10),
                `value` FLOAT(20),
                PRIMARY KEY (`id`)
            );
        """)
        self.connection.commit()

    # Deletes the SQLite database table named 'master'
    def drop_tables(self):
        cursor = self.connection.cursor()
        cursor.execute("""DROP TABLE IF EXISTS `master`;""")
        self.connection.commit()

    # Returns a list of all tickers stored in the database
    def get_active_tickers(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT DISTINCT ticker FROM master;""")
        return cursor.fetchall()

    # Inserts pandas DataFrame into the SQLite database.db file
    def insert_data(self, df):
        cursor = self.connection.cursor()
        cursor.executemany("""
            INSERT INTO `master` (date, ticker, type, value) VALUES (?, ?, ?, ?);
        """, df.values.tolist())
        self.connection.commit()

    # Returns the entire database as a pandas DataFrame
    def get_database(self, df):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT * FROM master""")
        rows = cursor.fetchall()
        # Without calling the reset_index method column 'index' is entirely populated by 'None'
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description]).reset_index()
        return df
    
    # Closes the SQL cursor object
    def close(self):
        self.connection.close()
    