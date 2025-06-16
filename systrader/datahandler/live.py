from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING

from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from pandas import concat

from systrader.datahandler import LiveDataHandler
from systrader.datahandler.utils import (
    convert_bar_df_to_tuple,
    get_n_of_weekends_b2in,
    transform_data,
)
from systrader.event import MARKETEVENT

if TYPE_CHECKING:
    from pandas import DataFrame

time_frame = {
    "1M": TimeFrame.Minute,
    "1H": TimeFrame.Hour,
    "1D": TimeFrame.Day,
}


class AlpacaDataHandler(LiveDataHandler):
    """
    Handles retrieval of live data from Alpaca
    """

    def __init__(
        self,
        symbols: str | list[str],
        time_frame: str,
        feed: str = "iex",
        api_key: str | None = None,
        secret_key: str | None = None,
    ):
        super().__init__(symbols, time_frame)
        self.feed = feed
        self._asset_class = {
            "EQ": [
                self.__get_symbols_str(symbol, "EQ")
                for symbol in self.symbols
                if "EQ" in symbol
            ],
            "CC": [
                self.__get_symbols_str(symbol, "CC")
                for symbol in self.symbols
                if "CC" in symbol
            ],
        }
        self.symbols = [self.__get_symbols_str(symbol) for symbol in self.symbols]

        if self._asset_class["EQ"]:
            self._stock_client = StockHistoricalDataClient(api_key, secret_key)

        if self._asset_class["CC"]:
            self._crypto_client = CryptoHistoricalDataClient(api_key, secret_key)

    def __get_symbols_str(self, symbol: str, asset_class: str | None = None) -> str:
        if asset_class:
            return symbol.split(asset_class + ".")[-1]
        return symbol[3:]

    def _load_data(self, symbols: str | list[str], n_of_bars: int) -> list[DataFrame]:
        start, end = self._get_start_end_timestamp(n_of_bars)

        bars = self._get_historical_bars(symbols, start, end)
        dfs = [bars.loc[symbol] for symbol in symbols]

        return dfs

    def update_bars(self):
        """
        Push the latest bar to the latest_symbol_data structure for all symbols
        successfully loaded in the symbol data.
        """
        start, end = self._get_start_end_timestamp(1)
        bars = self._get_historical_bars(self.symbols, start, end)

        for s in self.symbols:
            bar = transform_data(bars.loc[s])
            bar = convert_bar_df_to_tuple(s, bar)
            self.latest_symbol_data[s].append(bar)

        if self.event_manager:
            self.event_manager.notify(MARKETEVENT)

    def _get_start_end_timestamp(
        self, n_bars: int
    ) -> tuple[dt.datetime, dt.datetime | None]:
        curr_time = dt.datetime.now().utcnow()
        if n_bars == 1:
            end = None
            if self.time_frame == "1M":
                start = dt.datetime(
                    curr_time.year,
                    curr_time.month,
                    curr_time.day,
                    curr_time.hour,
                    curr_time.minute,
                )
            elif self.time_frame == "1H":
                start = dt.datetime(
                    curr_time.year, curr_time.month, curr_time.day, curr_time.hour
                )
            else:
                start = dt.datetime(curr_time.year, curr_time.month, curr_time.day)
        else:
            if self.time_frame == "1M":
                end = dt.datetime.utcnow() - dt.timedelta(minutes=1)
                start = end - dt.timedelta(minutes=n_bars)
            elif self.time_frame == "1H":
                end = dt.datetime(
                    curr_time.year, curr_time.month, curr_time.day, curr_time.hour
                )
                start = end - dt.timedelta(hours=n_bars)
            else:
                end = dt.datetime(curr_time.year, curr_time.month, curr_time.day)
                start = end - dt.timedelta(days=n_bars)
                start = start - dt.timedelta(days=get_n_of_weekends_b2in(start, end))

        return start, end

    def _get_historical_bars(
        self, symbols: str | list[str], start: dt.datetime, end: dt.datetime | None
    ) -> DataFrame:
        """
        Get the latest bar for a symbol
        """
        stock_symbols = [s for s in symbols if s in self._asset_class["EQ"]]
        crypto_symbols = [s for s in symbols if s in self._asset_class["CC"]]

        bar_dfs = []

        timeframe = time_frame[self.time_frame]
        if stock_symbols:
            request = StockBarsRequest(
                symbol_or_symbols=symbols,
                start=start,
                end=end,
                timeframe=timeframe,
            )
            stock_bars = self._stock_client.get_stock_bars(request).df
            bar_dfs.append(stock_bars)

        if crypto_symbols:
            request = CryptoBarsRequest(
                symbol_or_symbols=symbols,
                start=start,
                end=end,
                timeframe=timeframe,
            )
            crypto_bars = self._crypto_client.get_crypto_bars(request).df
            bar_dfs.append(crypto_bars)

        bars = concat(bar_dfs)
        return bars
