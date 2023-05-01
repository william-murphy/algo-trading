from database_manager import DatabaseManager
from strategy_manager import StrategyManager

class Trader:

    def __init__(self):
        pass

    def start(self):
        # This will be the "jumping off" point for the program, currently just calls the temp database manager test code
        self.test_database_manager()

    def test_database_manager(self):
        # Temporary driver code for showcasing database manager
        import yfinance as yf
        import pandas as pd

        tickers = ["JPM", "BAC", "C", "WFC", "GS", "MS"]  # JPMorgan, Bank of America, Citigroup, Wells Fargo, Goldman Sachs, & Morgan Stanley
        df = pd.concat([yf.download(ticker, start="2020-08-10", interval="1d").assign(Ticker=ticker).reset_index() for ticker in tickers]).melt(id_vars=["Date", "Ticker"], var_name="Type", value_name="Value")
        df["Date"] = df["Date"].apply(lambda x : x.date())

        # Test stuff
        db = DatabaseManager()

        db.drop_tables()
        db.create_tables()
        db.insert_data(df)

        print(db.get_active_tickers())
        print(db.get_data())
        print(db.get_data(tickers=["WFC", "MS", "GS"]))

        # db.plot_candlestick()
        # print(db.get_statistics())
        # db.plot_returns()

        # Close SQL connection
        db.connection.close()
