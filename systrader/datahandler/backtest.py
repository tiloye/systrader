import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import yfinance as yf
import yfinance.shared as shared

from systrader.datahandler.datahandler import BacktestDataHandler


class HistoricCSVDataHandler(BacktestDataHandler):
    """
    Read CSV files for each requested symbol from disk and load them into a dataframe.

    The class provides an interface to obtain the "latest" bar in a manner identical
    to a live trading interface. It will be assumed that all files are of the form
    'symbol.csv', where symbol is a string in the list.

    Parameters
    ----------
    csv_dir
        Absolute directory path to the CSV files.
    symbols
        A list of symbol strings.
    start_date
        The start date of the backtest.
    end_date
        The end date of the backtest.
    add_fields : list[str], optional
        Additional fields or columns to add to the OHLCV data.

    Attributes
    ----------
    csv_dir
        Absolute directory path to the CSV files.
    
    See docstring of BacktestDataHandler for information about other attributes.
    """

    def __init__(
        self,
        csv_dir: str | Path,
        symbols: list[str],
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        add_fields: list[str] | None = None,
    ):
        self.csv_dir = csv_dir
        super().__init__(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            add_fields=add_fields,
        )

    def _load_data(
        self,
        symbols: list[str],
        start: str | datetime | None,
        end: str | datetime | None,
    ) -> pd.DataFrame:
        """
        Load a CSV file for a symbol into a pandas DataFrame with proper indexing.

        Parameters
        ----------
        symbol
            The symbol to load the data for.

        Returns
        -------
        pandas.DataFrame
            The loaded data for the symbol.
        """
        filepaths = [os.path.join(self.csv_dir, f"{symbol}.csv") for symbol in symbols]
        dfs = [
            pd.read_csv(filepath, index_col=0, parse_dates=True)
            .sort_index()
            .loc[start:end]
            for filepath in filepaths
        ]
        return dfs


class PandasDataHandler(BacktestDataHandler):
    """
    Read CSV files for each requested symbol from disk and load them into a dataframe.

    The class provides an interface to obtain the "latest" bar in a manner identical
    to a live trading interface. It will be assumed that all files are of the form
    'symbol.csv', where symbol is a string in the list.

    Parameters
    ----------
    symbol_dfs
        Dictionary containing symbols (key) and historical data (values).
    start_date
        The start date of the backtest.
    end_date
        The end date of the backtest.
    add_fields
        Additional fields or columns to add to the OHLCV data.

    Attributes
    ----------
    See docstring of BacktestDataHandler.
    """

    def __init__(
        self,
        symbol_dfs: dict[str, pd.DataFrame],
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        add_fields: list[str] | None = None,
    ):
        assert isinstance(
            symbol_dfs, dict
        ), "symbol_dfs must be a dictionary must be a dictionary of dataframe(s)"
        self.symbol_dfs = symbol_dfs
        self.symbols = list(symbol_dfs.keys())
        super().__init__(
            symbols=self.symbols,
            start_date=start_date,
            end_date=end_date,
            add_fields=add_fields,
        )

    def _load_data(
        self,
        symbols: list[str],
        start: str | datetime | None,
        end: str | datetime | None,
    ) -> pd.DataFrame:
        """
        Get the pandas dataframe for a given symbol.

        Parameters
        ----------
        symbol
            The symbol to load the data for.
        start
            The start date for the backtest.
        end
            The end date for the backtest.

        Returns
        -------
        pandas.DataFrame
            The loaded data for the symbol.
        """
        return list(self.symbol_dfs.values())


class YahooDataHandler(BacktestDataHandler):
    """
    Load requested symbols from Yahoo Finance and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.

    Parameters
    ----------
    symbols : list
        A list of symbol strings.
    start_date
        The start date of the backtest.
    end_date
        The end date of the backtest.

    Attributes
    ----------
    See docstring of BacktestDataHandler.
    """

    def __init__(
        self,
        symbols: list[str],
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
    ):
        super().__init__(symbols=symbols, start_date=start_date, end_date=end_date)

    def _load_data(self, symbols, start_date, end_date) -> list[pd.DataFrame]:
        return self._download_data(self.symbols, self.start_date, self.end_date)

    def _download_data(
        self,
        symbols: str | list[str],
        start: str | datetime | None,
        end: str | datetime | None,
    ) -> list[pd.DataFrame] :
        if len(symbols) > 1:
            df = yf.download(symbols, start=start, end=end, group_by="ticker")
            if shared._ERRORS:
                symbols_with_error = list(shared._ERRORS.keys())
                if set(symbols_with_error).issubset(symbols):
                    print(f"Could not download data for {symbols_with_error}")
                    df = df.drop(columns=symbols_with_error, axis=1, level=0)
                    if df.empty:  # No symbol was downloaded
                        raise ValueError(shared._ERRORS)
            return [self._adjust(df[symbol]) for symbol in df.columns.levels[0]]
        else:
            df = yf.download(symbols, start=start, end=end, group_by="ticker")
            if shared._ERRORS:
                raise ValueError(shared._ERRORS)
            return [self._adjust(df)]

    def _adjust(self, data):
        """Adjust ohlcv with the yfinance adjustment factor (adj_close/close)."""
        data = data.copy()
        data.columns = data.columns.str.lower()
        adj_close = data["adj close"]
        close = data["close"]
        adj_factor = adj_close / close
        data.iloc[:, :-2] = data.iloc[:, :-2].mul(adj_factor, axis=0)
        data["volume"] = data["volume"].div(adj_factor)
        return data
