from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING

from systrader.datahandler.utils import transform_data
from systrader.event import MARKETEVENT

if TYPE_CHECKING:
    from pandas import DataFrame

    from systrader.event import EventManager


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
        self.event_manager = None

    def add_event_manager(self, event_manager: EventManager):
        self.event_manager = event_manager

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

    def request_bars(self, N: int):
        """
        Request N latest bars from the data source.

        Parameters
        ----------
        N
            The number of latest bars to request.
        """
        raise NotImplementedError("Should implement request_bars()")

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
    symbols
        A list of symbols to backtest. If backtesting is only for one symbol,
        symbols can be a string ("AAPL") or list (["AAPL"]).
    start_date
        The start date of the backtest.
    end_date
        The end date of the backtest.
    add_fields
        Additional fields or columns to add to the OHLCV data.

    Attributes
    ----------
    symbols
        A list of symbols to backtest.
    symbol_data
        A dictionary to store the data for each symbol.
    latest_symbol_data
        A dictionary to store the latest data for each symbol.
    start_date
        The start date of the backtest.
    end_date
        The end date of the backtest.
    timestamp
        The timestamp of the latest bar in the backtest.
    event_manager
        The event manager that notify other objects about events.
    continue_backtest
        A flag to indicate whether to continue the backtest.
    """

    def __init__(
        self,
        symbols: str | list[str],
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        add_fields: list[str] | None = None,
    ):
        super().__init__()
        self.symbols = symbols if isinstance(symbols, list) else [symbols]
        self.start_date = start_date
        self.end_date = end_date
        self.continue_backtest = True
        self.__additional_fields = add_fields

        self._prepare_data()

    def _prepare_data(self):
        """Prepare dataset for backtest"""
        dfs = self._load_data(self.symbols, self.start_date, self.end_date)
        for symbol, df in zip(self.symbols, dfs):
            self.latest_symbol_data[symbol] = []
            df = transform_data(df, self.__additional_fields)
            self.symbol_data[symbol] = df.itertuples(index=False, name=symbol)

    def _load_data(
        self,
        symbols: list[str],
        start: str | datetime | None,
        end: str | datetime | None,
    ) -> list[DataFrame]:
        """
        Load data for one or more symbols into pandas DataFrames with proper indexing.
        """
        raise NotImplementedError("Should implement loading symbol data from source")

    def request_bars(self, N: int):
        """
        Request N latest bars from the backetest data source.

        Parameters
        ----------
        N
            The number of latest bars to request.
        """
        for i in range(N):
            self.update_bars()

    @property
    def timestamp(self):
        """Returns the timestamp of the the latest bars."""

        return self.get_latest_bars(self.symbols[0])[0].timestamp

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

            if self.continue_backtest and self.event_manager:
                self.event_manager.notify(MARKETEVENT)
        else:
            print("The data history has no symbols.")


class LiveDataHandler(DataHandler):
    """
    Handles data for live trading

    Parameters
    ----------
    symbols: str or list[str]
        Symbol(s) to request data for.
    time_frame: TimeFrame
        Timeframe of the bars needed for live trading.
    minimum_bars: int
        Minimum number of bars to preload for each symbol.
    api_key: "str", optional
        API key for the data source
    secret_key: str, optional
        API secret key for the data source
    """

    def __init__(
        self,
        symbols: str | list[str],
        timeframe: str,
    ):
        super().__init__()
        self.symbols = symbols if isinstance(symbols, list) else [symbols]
        self.time_frame = timeframe

    def request_bars(self, N: int):
        """Request N latest bars from the data source."""

        dfs = self._load_data(self.symbols, N)
        for symbol, df in zip(self.symbols, dfs):
            self.latest_symbol_data[symbol] = []
            df = transform_data(df)
            self.latest_symbol_data[symbol] = list(
                df.itertuples(index=False, name=symbol)
            )

    def _load_data(self, symbols: str | list[str], N: int) -> list[DataFrame]:
        """
        Load data for one or more symbols into pandas DataFrames with proper indexing.
        """
        raise NotImplementedError("Should implement loading symbol data from source")
