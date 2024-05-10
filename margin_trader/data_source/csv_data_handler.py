import os
import pandas as pd

from margin_trader.data_source.data_handler import BacktestDataHandler
from margin_trader.event import MarketEvent

class HistoricCSVDataHandler(BacktestDataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface. 
    """

    def __init__(self, events, csv_dir, symbol_list, add_label=None):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.

        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.csv_dir = csv_dir
        super().__init__(events=events, symbol_list=symbol_list, add_label=add_label)

    def _load_data(self, symbol):
        """
        Load a CSV file for a symbol into a pandas DataFrame with proper indexing.
        """
        filepath = os.path.join(self.csv_dir, f"{symbol}.csv")
        df = pd.read_csv(
            filepath,
            index_col=0,
            parse_dates=True
        ).sort_index()
        return df