import pandas as pd

def get_pyfolio_roundtrips(history: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Convert position/trade history to pyfolio roundtrip trade history."""

    balance_history = history["balance_equity"].set_index("timeindex").balance
    positions = history["positions"].copy()
    pyfolio_rts = positions[["pnl", "open_time", "close_time", "side", "symbol"]].copy()
    pyfolio_rts.rename(
        columns={"open_time": "open_dt", "close_time": "close_dt", "side": "long"},
        inplace=True,
    )
    pyfolio_rts.loc[:, "long"] = pyfolio_rts.long.apply(
        lambda x: True if x == "BUY" else False
    )
    pyfolio_rts["duration"] = pyfolio_rts.close_dt.sub(pyfolio_rts.open_dt)
    pyfolio_rts["returns"] = pyfolio_rts.pnl.div(
        balance_history.loc[pyfolio_rts.close_dt].sub(pyfolio_rts.pnl.values).values
    )
    return pyfolio_rts
