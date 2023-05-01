from database_manager import DatabaseManager
import pandas as pd

class StrategyManager:

    def __init__(self, name, tickers, start, end, logic):
        self.name = name  # Name of strategy
        self.tickers = tickers  # Tickers on which strategy will be run
        self.start = start  # Start date of strategy
        self.end = end  # End date of strategy
        self.logic = logic  # Trading rules of strategy/Signal generation logic

    # Returns the strategy dataframe, with selected tickers, timeframe, added data, & trade signals
    def create_strategy(self):
        strat_df = pd.DataFrame()
        return (strat_df)
