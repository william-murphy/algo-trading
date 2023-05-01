import sqlite3
import pandas as pd
import mplfinance as mpf


class DatabaseManager:

    def __init__(self):
        # Opens an SQLite database connection on disk with database named "database"
        self.connection = sqlite3.connect("database.db")

    def create_tables(self):
        # Creates the SQLite table named "master"
        cursor = self.connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS master (
                `id` INT AUTO_INCREMENT,
                `date` DATE(20),
                `ticker` VARCHAR(16),
                `type` VARCHAR(10),
                `value` FLOAT(20),
                PRIMARY KEY (`id`)
            );
        """)
        self.connection.commit()  # Commit changes to SQLite datbase
        cursor.close()  # Close SQL cursor object

    def drop_tables(self):
        # Deletes the SQLite database table named "master" if one exists
        cursor = self.connection.cursor()  # Open SQL cursor object
        cursor.execute("""DROP TABLE IF EXISTS master;""")
        self.connection.commit()  # Commit changes to SQLite datbase
        cursor.close()  # Close SQL cursor object

        return print("Table 'master' dropped successfully.")

    def get_active_tickers(self):
        # Returns all unique tickers stored in the SQLite master table
        cursor = self.connection.cursor()  # Open SQL cursor object
        cursor.execute("""SELECT DISTINCT ticker FROM master;""")  # Select unique tickers from master table as tuples
        active_tickers = [ticker[0] for ticker in cursor.fetchall()]  # Extract first element of each tuple
        cursor.close()  # Close SQL cursor object

        return active_tickers

    def insert_data(self, df):
        # Inserts pandas DataFrame into the SQLite database.db file
        cursor = self.connection.cursor()
        cursor.executemany("""
            INSERT INTO master (date, ticker, type, value) VALUES (?, ?, ?, ?);
        """, df.values.tolist())
        self.connection.commit()

        cursor.close()  # Close SQL cursor object

        return 0

    def get_data(self, tickers=None):
        # Returns data (of specefied tickers) frrom the SQLite database as a pandas DataFrame
        # "tickers" parameter  defaults to "active_tickers"
        if tickers is None:
            tickers = self.get_active_tickers()

        df = pd.DataFrame()  # Initialize an empty dataframe
        cursor = self.connection.cursor()  # Create SQL cursor object

        for ticker in tickers:
            query = f"""SELECT * FROM master WHERE Ticker = '{ticker}'"""  # Query specefied tickers
            data = pd.read_sql_query(query, self.connection)
            # Append the data from this query to the initalized dataframe
            df = pd.concat([data, df])

        cursor.close()  # Close SQL cursor object
        print(f"Data for tickers {tickers}")  # Print output response

        return df
        
    def plot_candlestick(self):
        # Plot candlestick charts of individual tickers in database
        cursor = self.connection.cursor()
        cursor.execute("""SELECT date, ticker, type, value FROM master""")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        df["date"] = pd.to_datetime(df["date"])
        df = df.reset_index()
        df = df.set_index("date")
        df = pd.pivot_table(df, values="value", index=["date", "ticker"], columns="type")
        print(df)

        subplots = {}

        for ticker in df.index.levels[1]:
            df_ticker = df.loc[df.index.get_level_values(1) == ticker]
            df_ticker = df_ticker.reset_index(level="ticker")
            mpf.plot(df_ticker, type="candle", title=ticker, mav=(50, 200), volume=True, warn_too_much_data=999999999)

        cursor.close()  # Close SQL cursor object

        return 0
    
    def plot_returns(self):
        # Plot line graphs of returns by ticker
        cursor = self.connection.cursor()
        cursor.execute("""SELECT date, ticker, type, value FROM master""")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        df["date"] = pd.to_datetime(df["date"])
        df = df.reset_index()
        df = df.set_index("date")
        df = pd.pivot_table(df, values="value", index=["date", "ticker"], columns="type")
        print(df)

        subplots = {}

        for ticker in df.index.levels[1]:
            df_ticker = df.loc[df.index.get_level_values(1) == ticker]
            df_ticker = df_ticker.reset_index(level="ticker")
            mpf.plot(df_ticker, type="line", title=ticker, warn_too_much_data=999999999)

        cursor.close()  # Close SQL cursor object

        return 0
    
    def get_statistics(self):
        # Review key statistics of data by ticker
        cursor = self.connection.cursor()
        cursor.execute("""SELECT date, ticker, type, value FROM master""")
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[col[0] for col in cursor.description])
        df["date"] = pd.to_datetime(df["date"])
        df = df.reset_index()
        df = df.set_index("date")
        df = pd.pivot_table(df, values="value", index=["date", "ticker"], columns="type")
        df = df.groupby("ticker").describe()

        cursor.close()  # Close SQL cursor object

        return df
