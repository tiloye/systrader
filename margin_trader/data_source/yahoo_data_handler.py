from datetime import datetime

import yfinance as yf
import yfinance.shared as shared

from margin_trader.data_source.data_handler import BacktestDataHandler


class YahooDataHandler(BacktestDataHandler):
    """
    Load requested symbols from Yahoo Finance and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.

    Parameters
    ----------
    symbols : list
        A list of symbol strings.
    start_date : str or datetime
        The start date of the backtest.
    end_date : str or datetime
        The end date of the backtest.
    add_label : list, optional
        Additional labels to include in the data.

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
    comb_index : pandas.Index or None
        The combined index of all symbols' data.
    events : Queue
        The event queue for the backtest.
    """

    def __init__(
        self,
        symbols: list[str],
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        use_cols: list[str] | None = None,
    ):
        super().__init__(
            symbols=symbols, start_date=start_date, end_date=end_date, use_cols=use_cols
        )

    def _load_symbols(self) -> None:
        self._download_data(self.symbols, self.start_date, self.end_date)
        if self.use_cols:
            labels = [label.lower() for label in self.use_cols]
            cols = self._ohlc + labels
        else:
            cols = self._ohlc
        for symbol in self.symbol_data:
            self.symbol_data[symbol] = self.symbol_data[symbol][cols]

    def _download_data(
        self,
        symbols: str | list[str],
        start: str | datetime | None,
        end: str | datetime | None,
    ):
        if len(symbols) > 1:
            df = yf.download(symbols, start=start, end=end, group_by="ticker")
            if shared._ERRORS:
                symbols_with_error = list(shared._ERRORS.keys())
                if set(symbols_with_error).issubset(symbols):
                    print(f"Could not download data for {symbols_with_error}")
                    df = df.drop(columns=symbols_with_error, axis=1, level=0)
                    if df.empty:  # No symbol was downloaded
                        raise ValueError(shared._ERRORS)
            self.symbol_data = {
                symbol: self.__adjust(df[symbol]) for symbol in df.columns.levels[0]
            }
        else:
            df = yf.download(symbols, start=start, end=end, group_by="ticker")
            if shared._ERRORS:
                symbols_with_error = list(shared._ERRORS.keys())
                raise ValueError(shared._ERRORS)
            self.symbol_data = {symbols[0]: self.__adjust(df)}

        pass

    def __adjust(self, data):
        data = data.copy()
        data.columns = data.columns.str.lower()
        adj_close = data["adj close"]
        close = data["close"]
        adj_factor = adj_close / close
        data.iloc[:, :-2] = data.iloc[:, :-2].mul(adj_factor, axis=0)
        data["volume"] = data["volume"].div(adj_factor)
        return data
