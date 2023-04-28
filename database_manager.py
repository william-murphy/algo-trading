import sqlite3
import pandas as pd
import mplfinance as mpf


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
    def get_database(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT date, ticker, type, value FROM master""")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        return df

    # Plot candlestick charts of individual tickers in database
    def plot_candlestick(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT date, ticker, type, value FROM master""")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        df['date'] = pd.to_datetime(df['date'])
        df = df.reset_index()
        df = df.set_index('date')
        df = pd.pivot_table(df, values='value', index=['date', 'ticker'], columns='type')
        print(df)

        subplots = {}

        for ticker in df.index.levels[1]:
            df_ticker = df.loc[df.index.get_level_values(1) == ticker]
            df_ticker = df_ticker.reset_index(level='ticker')
            mpf.plot(df_ticker, type='candle', title=ticker, mav=(50, 200), volume=True, warn_too_much_data=999999999)

        return 0
    
    # Plot line graphs of returns by ticker
    def plot_returns(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT date, ticker, type, value FROM master""")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        df['date'] = pd.to_datetime(df['date'])
        df = df.reset_index()
        df = df.set_index('date')
        df = pd.pivot_table(df, values='value', index=['date', 'ticker'], columns='type')
        print(df)

        subplots = {}

        for ticker in df.index.levels[1]:
            df_ticker = df.loc[df.index.get_level_values(1) == ticker]
            df_ticker = df_ticker.reset_index(level='ticker')
            mpf.plot(df_ticker, type='line', title=ticker, warn_too_much_data=999999999)

        return 0
    
    # Review key statistics of data by ticker
    def get_statistics(self):
        cursor = self.connection.cursor()
        cursor.execute("""SELECT date, ticker, type, value FROM master""")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        df['date'] = pd.to_datetime(df['date'])
        df = df.reset_index()
        df = df.set_index('date')
        df = pd.pivot_table(df, values='value', index=['date', 'ticker'], columns='type')
        df = df.groupby('ticker').describe()

        return print(df)

    # Closes the SQL cursor object
    def close(self):
        self.connection.close()
