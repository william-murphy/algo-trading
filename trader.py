from database_manager import DatabaseManager

class Trader:

    def __init__(self):
        pass

    def start(self):
        # this will be the "jumping off" point for the program, currently just calls the temp database manager test code
        self.test_database_manager()

    def test_database_manager(self):
        # temporary driver code for showcasing database manager
        import yfinance as yf
        import pandas as pd

        tickers = ["JPM", "MANU", "CLH"]
        df = pd.concat([yf.download(ticker, start='2020-08-10', interval='1d').assign(Ticker=ticker).reset_index() for ticker in tickers]).melt(id_vars=['Date', 'Ticker'], var_name='Type', value_name='Value')
        df["Date"] = df["Date"].apply(lambda x : x.date())

        db = DatabaseManager()
        db.create_tables()
        db.insert_data(df)
        print(db.get_active_tickers())
        db.close()


# old trader code
# class Trader():

#     def __init__(self):
#         pass

#     def connect(self):
#         try:
#             cnx = mysql.connector.connect(
#                 host=SQL_HOST, user=SQL_USERNAME, passwd=SQL_PASSWORD)
#         except mysql.connector.Error as err:
#             if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#                 print('Incorrect user name & password combination.')
#             elif err.errno == errorcode.ER_BAD_DB_ERROR:
#                 print('Invalid Database.')
#             else:
#                 print(err)
#         else:
#             cnx.close()

#     # Create get_data function to return DataFrames of error-free data for analysis
#     def get_data(self, tickers, start_date, end_date, interval):

#         # get_data returns long and 2 verisons of wide data as DataFrames
#         # get_data plots histograms and line graphs of data and removes erroneous values
#         #
#         # tickers (str): desired Yahoo Finance symbol(s) for which to fetch data
#         # start_date (str): start date of query in YYYY-MM-DD format
#         # end_date (str): end date of query in YYYY-MM-DD format, defaults to today
#         # interval (str): intervals of data, options include '1d', '1wk', or '1mo'

#         # end_date defaults to today
#         if end_date is None:
#             end_date = dt.date.today().strftime('%Y-%m-%d')

#         # Get Yahoo Finance API data
#         df = pd.concat([yf.download(ticker, start=start_date, group_by='Ticker', interval='1d').assign(Ticker=ticker) for ticker in tickers], ignore_index=False)

#         # Add 'Date' index as column and reset index of dataframe
#         df = df.reset_index()

#         # Create long version of dataframe
#         df_long = df.melt(id_vars=['Date', 'Ticker'], var_name='OHLCV', value_name='Value')

#         # Create multi-index for df_long such that each value has a unique date, ticker, & OHLCV combination
#         df_long.set_index(['Date', 'Ticker', 'OHLCV'], inplace=True)

#         # Create a wide dataframe version with tickers as columns
#         df_wide_ticker = df_long.unstack('Ticker')['Value']
#         df_wide_ticker.columns.name = 'Ticker'
#         df_wide_ticker = df_wide_ticker.reset_index().rename_axis(None, axis=1)

#         # Create a wide dataframe version with OHLCV types as columns
#         df_wide_ohlcv = df_long.unstack('OHLCV')['Value']
#         df_wide_ohlcv.columns.name = None
#         df_wide_ohlcv = df_wide_ohlcv.reset_index()

#         # Calculate the original Adj Close for each instrument
#         orig_adj_close = df_wide_ohlcv.groupby('Ticker')['Adj Close'].first()

#         # Create a new column 'Return' and set its value for each row
#         df_wide_ohlcv['Return'] = df_wide_ohlcv.apply(lambda row: row['Adj Close'] / orig_adj_close[row['Ticker']], axis=1)

#         # Create tickers histograms
#         df_wide_ticker.hist()

#         # Display tickers histograms
#         plt.show()

#         # Create OHLCV histograms
#         df_wide_ohlcv.hist()

#         # Display OHLCV histograms
#         plt.show()

#         # Pivot the data
#         df_pivot = df_wide_ohlcv.pivot(index='Date', columns='Ticker', values='Return')

#         # Plot each ticker's return over time
#         for col in df_pivot.columns:
#             plt.plot(df_pivot.index, df_pivot[col], label=col)

#         # Set the x-axis label and title
#         plt.xlabel('Date')
#         plt.title('Returns by Ticker')

#         # Add a legend
#         plt.legend()

#         # Show the plot
#         plt.show()

#         return df_long, df_wide_ticker, df_wide_ohlcv

#     def trader(self):
#         self.connect()
#         # Test get_data
#         df_long, df_wide_ticker, df_wide_ohlcv = self.get_data(['JPM', 'BAC', 'C', 'WFC', 'GS', 'MS'], '2009-03-09', None, '1d')

#         # Print get_data dataframes
#         print(df_long, df_wide_ticker, df_wide_ohlcv)

#         # Test get_data again
#         eg_long, eg_wide_ticker, eg_wide_ohlcv = self.get_data(['GC=F', 'SI=F'], '2011-06-15', None, '1mo')

#         # Print get_data dataframes again
#         print(eg_long, eg_wide_ticker, eg_wide_ohlcv)

#         return 0
