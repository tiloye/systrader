from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING

from margin_trader.event import MARKETEVENT

if TYPE_CHECKING:
    from margin_trader.event import EventManager


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

    def __init__(self):
        self.symbol_data = {}
        self.latest_symbol_data = {}

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
            print("That symbol is not available in the historical dataset.")
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

    @abstractmethod
    def get_new_bar(self, symbol: str):
        """
        Return the latest bar from a data feed as a named tuple.

        Parameters
        ----------
        symbol
            The symbol to get the latest bar for.

        Returns
        -------
        namedtuple
            The latest bar for the symbol.
        """
        raise NotImplementedError("Should implement get_new_bar()")

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
    symbols : str or list
        A list of symbols to backtest. If backtesting is only for one symbol,
        symbols can be a string ("AAPL") or list (["AAPL"]).
    start_date : str or datetime
        The start date of the backtest.
    end_date : str or datetime
        The end date of the backtest.
    use_cols : list[str], optional
        Additional columns to include in the data.

    Attributes
    ----------
    symbols : list
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
    events : Queue
        The event queue for the backtest.
    """

    def __init__(
        self,
        symbols: str | list[str],
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        use_cols: list[str] | None = None,
    ):
        super().__init__()
        self.symbols = symbols if isinstance(symbols, list) else [symbols]
        self.start_date = start_date
        self.end_date = end_date
        self.continue_backtest = True
        self._ohlc = ["open", "high", "low", "close"]
        self.use_cols = use_cols

        self._prepare_data()

    def add_event_manager(self, event_manager: EventManager):
        self.event_manager = event_manager

    def _prepare_data(self):
        """Prepare dataset for backtest"""
        self._load_symbols()
        for symbol in self.symbols:
            self.latest_symbol_data[symbol] = []
            self.symbol_data[symbol] = self.symbol_data[symbol].itertuples(name=symbol)

    def _load_symbols(self):
        if self.use_cols:
            labels = [label.lower() for label in self.use_cols]
            cols = self._ohlc + labels
        else:
            cols = self._ohlc
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

    def _load_data(
        self, symbol: str, start: str | datetime | None, end: str | datetime | None
    ):
        """
        Load a data for a symbol into a pandas DataFrame with proper indexing.
        """
        raise NotImplementedError("Should implement loading symbol data from source")

    def get_new_bar(self, symbol: str):
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

    def update_bars(self):
        """
        Push the latest bar to the latest_symbol_data structure for all symbols
        successfully loaded in the symbol data.
        """

        if self.symbol_data:
            for s in self.symbol_data:
                try:
                    bar = self.get_new_bar(s)
                except StopIteration:
                    self.continue_backtest = False
                else:
                    if bar is not None:
                        self.latest_symbol_data[s].append(bar)
                        self.current_datetime = bar.Index
            if self.continue_backtest:
                self.event_manager.notify(MARKETEVENT)
        else:
            print("The data history has no symbols.")
