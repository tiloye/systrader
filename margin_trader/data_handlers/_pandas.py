from datetime import datetime

import pandas as pd

from margin_trader.data_handlers.data_handler import BacktestDataHandler


class PandasDataHandler(BacktestDataHandler):
    """
    Read CSV files for each requested symbol from disk and load them into a dataframe.

    The class provides an interface to obtain the "latest" bar in a manner identical
    to a live trading interface. It will be assumed that all files are of the form
    'symbol.csv', where symbol is a string in the list.

    Parameters
    ----------
    symbol_dfs : dict[str, DataFrame]
        Dictionary containing symbols (key) and historical data (values).
    start_date : str or datetime
        The start date of the backtest.
    end_date : str or datetime
        The end date of the backtest.
    use_cols : list, optional
        Additional labels to include in the data.

    Attributes
    ----------
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
        symbol_dfs: dict[str, pd.DataFrame],
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
        use_cols: list[str] | None = None,
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
            use_cols=use_cols,
        )

    def _load_data(
        self, symbol: str, start: str | datetime | None, end: str | datetime | None
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
        df = self.symbol_dfs[symbol]
        if start:
            df = df.loc[start:]
        if end:
            df = df.loc[:end]
        return df
