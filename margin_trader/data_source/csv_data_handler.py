import os
import pandas as pd

from margin_trader.data_source.data_handler import DataHandler
from margin_trader.event import MarketEvent

class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface. 
    """

    def __init__(self, events, csv_dir, symbol_list):
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
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.start_date = None
        self.current_datetime = None
        self.continue_backtest = True
        self.comb_index = None

        self._prepare_data()

    def _prepare_data(self):
        """Prepare dataset for backtest"""
        self._load_csv_files()
        if self.comb_index is not None:
            self.start_date = self.comb_index[0]
            for symbol in self.symbol_list:
                self.latest_symbol_data[symbol] = []
                self.symbol_data[symbol] = self.symbol_data[symbol].reindex(
                    index=self.comb_index, method='pad'
                )
                self.symbol_data[symbol] = self.symbol_data[symbol].iterrows()

    def _load_csv_file(self, symbol):
        """
        Load a CSV file for a symbol into a pandas DataFrame with proper indexing.
        """
        columns = ["open", "high", "low", "close", "volume"]
        filepath = os.path.join(self.csv_dir, f"{symbol}.csv")
        df = pd.read_csv(
            filepath,
            index_col=0,
            parse_dates=True
        ).sort_index()
        df.columns = df.columns.str.lower()
        df.index.name = "datetime"
        try:
            df.columns = columns
        except ValueError:
            print("The number of columns in the datesetdoes\
                   not match the expected number of columns")
        return df
    
    def _load_csv_files(self):
        for symbol in self.symbol_list:
            self.symbol_data[symbol] = self._load_csv_file(symbol)

             # Combine the index to pad forward values
            if self.comb_index is None:
                self.comb_index = self.symbol_data[symbol].index
            else:
               self.comb_index.union(self.symbol_data[symbol].index)

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed as a tuple of 
        (sybmbol, datetime, open, low, high, close, volume).
        """
        bar = next(self.symbol_data[symbol])
        return (
                bar[0],
                bar[1].open,
                bar[1].high,
                bar[1].low,
                bar[1].close,
                bar[1].volume
        )
            
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]
        
    def get_latest_close_price(self, symbol):
        latest_bar = self.get_latest_bars(symbol)
        return latest_bar[0][4]
        
    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = self._get_new_bar(s)
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.current_datetime = bar[0]
        self.events.put(MarketEvent())