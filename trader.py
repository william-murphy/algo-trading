# Import package dependencies
import yfinance as yf
import datetime as dt
import numpy as np
import pandas as pd
import matplotlib as plt
import import_ipynb
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

# Get data
df = pd.concat([yf.download(ticker, start=start_date, group_by="Ticker", interval='1d').assign(ticker=ticker) for ticker in tickers], ignore_index=False)

# Print data
print(df)