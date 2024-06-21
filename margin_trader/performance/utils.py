import pandas as pd


def get_pyfolio_roundtrips(account_history: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Convert position/trade history to pyfolio roundtrip trade history."""

    balance_history = account_history["balance_equity"]["balance"]
    positions = account_history["positions"].copy()
    pyfolio_rts = positions[["pnl", "open_time", "close_time", "side", "symbol"]].copy()
    pyfolio_rts.rename(
        columns={"open_time": "open_dt", "close_time": "close_dt", "side": "long"},
        inplace=True,
    )
    pyfolio_rts["long"] = pyfolio_rts["long"].eq("BUY")
    pyfolio_rts["duration"] = pyfolio_rts["close_dt"].sub(pyfolio_rts["open_dt"])
    pyfolio_rts["returns"] = get_trade_roi(positions, balance_history)
    return pyfolio_rts


def get_trade_roi(trade_history: pd.DataFrame, balance_history: pd.Series) -> pd.Series:
    """Get the return series needed for roundtrip trade analysis."""
    pnl = trade_history["pnl"].copy()
    curr_balance = balance_history.loc[trade_history["close_time"]].reset_index(
        drop=True
    )
    prev_balance = curr_balance - pnl
    roi = (curr_balance / prev_balance) - 1
    return roi
