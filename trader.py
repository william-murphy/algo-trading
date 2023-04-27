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

        tickers = ["JPM", "MANU", "CLH", "NEE"]
        df = pd.concat([yf.download(ticker, start='2020-08-10', interval='1d').assign(Ticker=ticker).reset_index() for ticker in tickers]).melt(id_vars=['Date', 'Ticker'], var_name='Type', value_name='Value')
        df["Date"] = df["Date"].apply(lambda x : x.date())

        # Test stuff
        db = DatabaseManager()
        db.create_tables()
        db.insert_data(df)
        print(db.get_active_tickers())
        print(db.get_database())
        db.plot_candlestick()
        db.get_statistics()
        db.plot_returns()

        # Close SQL cursor object
        db.close()
