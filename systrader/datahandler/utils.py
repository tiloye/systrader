from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from pandas import DataFrame


def transform_data(df: DataFrame, additional_fields: list[str] | None = None):
    ohlc = ["open", "high", "low", "close", "volume"]

    df.columns = df.columns.str.lower()

    if additional_fields:
        additional_fields = [field.lower() for field in additional_fields]
        cols = ohlc + additional_fields
    else:
        cols = ohlc

    df = df[cols]
    df.columns = df.columns.str.lower()
    df.index.name = "timestamp"

    return df.reset_index()


def convert_bar_df_to_tuple(symbol: str, df: DataFrame) -> tuple | list[tuple]:
    if len(df) > 1:
        return list(df.itertuples(index=False, name=symbol))
    return next(df.itertuples(index=False, name=symbol))


def get_n_of_weekends_b2in(start: str | datetime, end: str | datetime) -> int:
    from pandas import date_range

    dates = date_range(start, end)
    return int((dates.weekday >= 5).sum())
