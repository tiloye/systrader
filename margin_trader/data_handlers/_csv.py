import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from margin_trader.data_handlers.data_handler import BacktestDataHandler


class HistoricCSVDataHandler(BacktestDataHandler):
    """
    Read CSV files for each requested symbol from disk and load them into a dataframe.

    The class provides an interface to obtain the "latest" bar in a manner identical
    to a live trading interface. It will be assumed that all files are of the form
    'symbol.csv', where symbol is a string in the list.

    Parameters
    ----------
    csv_dir : str
        Absolute directory path to the CSV files.
    symbols : list
        A list of symbol strings.
    start_date : str or datetime
        The start date of the backtest.
    end_date : str or datetime
        The end date of the backtest.
    use_cols : list, optional
        Additional labels to include in the data.

    Attributes
    ----------
    csv_dir : str
        Absolute directory path to the CSV files.
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
        csv_dir: str | Path,
        symbols: list[str],
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        use_cols: list[str] | None = None,
    ):
        self.csv_dir = csv_dir
        super().__init__(
            symbols=symbols, start_date=start_date, end_date=end_date, use_cols=use_cols
        )

    def _load_data(
        self, symbol: str, start: str | datetime | None, end: str | datetime | None
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
        filepath = os.path.join(self.csv_dir, f"{symbol}.csv")
        df = pd.read_csv(filepath, index_col=0, parse_dates=True).sort_index()
        if start:
            df = df.loc[start:]
        if end:
            df = df.loc[:end]
        return df
