import yfinance as yf
from datetime import datetime
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
            start_date: str|datetime = None,
            end_date: str|datetime = None,
            add_label: list[str]|None = None
            ):
        super().__init__(symbols=symbols, start_date=start_date,
                         end_date=end_date, add_label=add_label)

    def _load_data(self, symbol: str, start: str|datetime, end: str|datetime):
        """
        Load symbol data.

        Parameters
        ----------
        symbol
            The symbol to load the data for.

        Returns
        -------
        pandas.DataFrame
            The loaded data for the symbol.
        """
        pass

    def _load_symbols(self):
        self._download_data(self.symbols, self.start_date, self.end_date)
        pass
    
    def _download_data(self, symbols: str|list[str], start: str|datetime,
                       end: str|datetime):
        if isinstance(symbols, list) and len(symbols) > 1:
            df = yf.download(symbols, start=start, end=end, group_by="ticker")
            self.symbol_data = {symbol: df[symbol] for symbol in df.columns.levels[0]}
        else:
            df = yf.download(symbols, start=start, end=end, group_by="ticker")
        pass

    def __clean(data):
        data = data.copy()
        data.columns = data.columns.str.lower()
        adj_close = data["adj close"]
        close = data["close"]
        adj_factor = adj_close/close
        data.iloc[:, :-2] = data.iloc[:, :-2].mul(adj_factor, axis=0)
        data["volume"] = data["volume"].div(adj_factor)
        return data