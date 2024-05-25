from abc import ABC, abstractmethod
from datetime import datetime
from margin_trader.event import MarketEvent


class DataHandler(ABC):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OLHCVI) for each symbol requested. 

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")
    

class BacktestDataHandler(DataHandler):
    """
    Handle data for backtesting by loading the data into pandas DataFrame.

    Parameters
    ----------
    symbols : list
        A list of symbols to backtest.
    start_date : str or datetime
        The start date of the backtest.
    end_date : str or datetime
        The end date of the backtest.
    add_label : list, optional
        Additional columns to include in the data.

    Attributes
    ----------
    symbol_list : list
        A list of symbols to backtest.
    symbol_data : dict
        A dictionary to store the data for each symbol.
    latest_symbol_data : dict
        A dictionary to store the latest data for each symbol.
    start_date : str or datetime
        The start date of the backtest.
    end_date : str or datetime
        The end date of the backtest.
    current_datetime : str or datetime.
        The current datetime in the backtest.
    continue_backtest : bool
        A flag to indicate whether to continue the backtest.
    comb_index : pandas.Index or None
        The combined index of all symbols' data.
    events : Queue
        The event queue for the backtest.
    """

    def __init__(self, symbols, start_date=None, end_date=None, add_label=None):
        self.symbols = symbols
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.start_date = start_date
        self.end_date = end_date
        self.current_datetime = start_date
        self.continue_backtest = True
        self.comb_index = None
        self.__ohlc = ["open", "high", "low", "close"]
        self.__extra_label = add_label

        self._prepare_data()

    def _add_event_queue(self, event_queue):
        self.events = event_queue

    def _prepare_data(self):
        """Prepare dataset for backtest"""
        self._load_symbols()
        if self.comb_index is not None:
            for symbol in self.symbols:
                self.latest_symbol_data[symbol] = []
                self.symbol_data[symbol] = self.symbol_data[symbol].reindex(
                    index=self.comb_index, method='pad'
                )
                self.symbol_data[symbol] = (
                    self.symbol_data[symbol].itertuples(name=symbol)
                )

    def _load_symbols(self):
        if self.__extra_label:
            labels = [label.lower() for label in self.__extra_label]
            cols = self.__ohlc + labels
        else:
            cols = self.__ohlc
        for symbol in self.symbols:
            df = self._load_data(symbol, self.start_date, self.end_date)
            df.columns = df.columns.str.lower()
            try:
                df = df[cols]
                self.symbol_data[symbol] = df
            except KeyError:
                print(
                    f"""The columns in {symbol} data does
                    not match the expected columns. Expected {cols},
                    but got {df.columns.to_list()}.
                    """
                )
                continue

             # Combine the index to pad forward values
            if self.comb_index is None:
                self.comb_index = self.symbol_data[symbol].index
            else:
               self.comb_index.union(self.symbol_data[symbol].index)

    def _load_data(self, symbol: str, start: str|datetime, end: str|datetime):
        """
        Load a data for a symbol into a pandas DataFrame with proper indexing.
        """
        raise NotImplementedError("Should implement loading symbol data from source")
    
    def _get_new_bar(self, symbol: str):
        """
        Return the latest bar from the data feed as a named tuple.

        Parameters
        ----------
        symbol
            The symbol to get the latest bar for.

        Returns
        -------
        namedtuple
            The latest bar for the symbol.
        """
        return next(self.symbol_data[symbol])
            
    def get_latest_bars(self, symbol: str, N: int = 1):
        """
        Return the last N bars from the latest_symbol list, or N-k if less available.

        Parameters
        ----------
        symbol
            The symbol to get the latest bars for.
        N
            The number of bars to return, by default 1.

        Returns
        -------
        list
            The last N bars for the symbol.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
        else:
            return bars_list[-N:]

    def get_latest_price(self, symbol: str, price: str = "close"):
        """
        Return the latest price for a symbol.

        Parameters
        ----------
        symbol
            The symbol to get the latest price for.
        price
            The price type to return ("open", "high", "low", "close"), 
            by default "close".

        Returns
        -------
        float
            The latest price for the symbol.
        """
        latest_bar = self.get_latest_bars(symbol)
        if price == "open":
            return latest_bar[0].open
        elif price == "high":
            return latest_bar[0].high
        elif price == "low":
            return latest_bar[0].low
        return latest_bar[0].close
        
    def update_bars(self):
        """
        Push the latest bar to the latest_symbol_data structure for all symbols 
        successfully loaded in the symbol data.
        """

        if self.symbol_data:
            for s in self.symbol_data:
                try:
                    bar = self._get_new_bar(s)
                except StopIteration:
                    self.continue_backtest = False
                else:
                    if bar is not None:
                        self.latest_symbol_data[s].append(bar)
                        self.current_datetime = bar.Index
            self.events.put(MarketEvent())
        else:
            print("The data history has no symbols.")