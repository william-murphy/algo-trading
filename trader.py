# Import package dependencies
import yfinance as yf
import pandas as pd
import mysql.connector

# Import credentials.py file with sensitive info
from credentials import *

# Connect to local SQL database
from mysql.connector import errorcode

try:
    cnx = mysql.connector.connect(host=SQL_HOST, user=SQL_USERNAME, passwd=SQL_PASSWORD)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Incorrect user name & password combination.')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print('Invalid Database.')
    else:
        print(err)
else:
  cnx.close()

# Define time frame to query historical data
start_date = '2009-03-09'

# Create list of ticker symbols
tickers = ['JPM', 'BAC', 'C', 'WFC', 'GS', 'MS']

# Get Yahoo Finance API data
df = pd.concat([yf.download(ticker, start=start_date, group_by="Ticker", interval='1d').assign(Ticker=ticker) for ticker in tickers], ignore_index=False)

# Add 'Date' index as column and reset index of dataframe
df = df.reset_index()

# Create long version of dataframe
df_long = df.melt(id_vars=['Date', 'Ticker'], var_name='OHLCV', value_name='Value')

# Create multi-index for df_long such that each value has a unique date, ticker, & OHLCV combination
df_long.set_index(['Date', 'Ticker', 'OHLCV'], inplace=True)

# Create a wide dataframe version with tickers as columns
df_wide_ticker = df_long.unstack('Ticker')['Value']
df_wide_ticker.columns.name = 'Ticker'
df_wide_ticker = df_wide_ticker.reset_index().rename_axis(None, axis=1)

# Create a wide dataframe version with OHLCV types as columns
df_wide_ohlcv = df_long.unstack('OHLCV')['Value']
df_wide_ohlcv.columns.name = None
df_wide_ohlcv = df_wide_ohlcv.reset_index()

# Print dataframes
print(df_long, df_wide_ticker, df_wide_ohlcv)