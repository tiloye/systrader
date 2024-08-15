import pandas as pd


def get_pyfolio_roundtrips(account_history: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Convert position/trade history to pyfolio roundtrip trade history.

    Returns a dataframe with the following columns:
    pnl: profit or loss from the trade
    open_dt: time the position was opened
    close_dt: time the position was closed
    long: side of the position (buy or sell)
    symbol: symbol of the asset traded
    duration: duration of the trade
    returns: return on the account balance
    """

    balance_history = account_history["balance_equity"]["balance"]
    positions = account_history["positions"].copy()
    pyfolio_rts = positions[["pnl", "open_time", "close_time", "side", "symbol"]].copy()
    pyfolio_rts.rename(
        columns={"open_time": "open_dt", "close_time": "close_dt", "side": "long"},
        inplace=True,
    )
    pyfolio_rts["long"] = pyfolio_rts["long"].eq("buy")
    pyfolio_rts["duration"] = pyfolio_rts["close_dt"].sub(pyfolio_rts["open_dt"])
    pyfolio_rts["returns"] = get_trade_roi(positions, balance_history)
    return pyfolio_rts


def get_trade_roi(positions: pd.DataFrame, balance_history: pd.Series) -> pd.Series:
    """Get the return on account balance at the time the position was closed."""
    pnl = positions["pnl"].copy()
    curr_balance = balance_history.loc[positions["close_time"]].reset_index(drop=True)
    prev_balance = curr_balance - pnl
    roi = (curr_balance / prev_balance) - 1
    return roi
