import empyrical as emp
import numpy as np
import pandas as pd
import pyfolio as pf


def total_return(returns: pd.Series | np.ndarray) -> float:
    """Calculate the total return on investment (ROI) from a series of returns.

    Parameters
    ----------
    returns
        A pandas Series or NumPy ndarray containing the investment's historical
        returns. Each element represents the return for a specific period
        (e.g., daily, monthly).

    Returns
    -------
    float
        The percentage return on investment (ROI).
    """
    roi = emp.cum_returns_final(returns)
    return roi


def annual_return(returns: pd.Series | np.ndarray, periods: int = 252) -> float:
    """Calculate the annualized return of a series of returns.

    This function computes the compounded annual growth rate (CAGR) of the
    investment based on a provided series of returns.

    Parameters
    ----------
    returns
        A pandas Series or NumPy ndarray containing the investment's historical
        returns. Each element represents the return for a specific period
        (e.g., daily, monthly).

    periods
        The number of periods per year used for annualization. This adjustment
        factor is needed to convert the return to an annual basis. If the data
        is daily, set `periods` to 252. For other frequencies, adjust accordingly
        (e.g., hourly: 252 * 6.5, minutely: 252 * 6.5 * 60). Defaults to 252
        (assuming daily data).

    Returns
    -------
    float
    The annualized return of the investment as a single value.
    """
    return emp.annual_return(returns, annualization=periods)


def annual_volatility(returns: pd.Series | np.ndarray, periods: int = 252) -> float:
    """Calculate the annualized volatility of a series of returns.

    Parameters
    ----------
    returns
        A pandas Series or NumPy ndarray containing the investment's historical
        returns. Each element represents the return for a specific period
        (e.g., daily, monthly).

    periods
        The number of periods per year used for annualization. This adjustment
        factor is needed to convert the volatility to an annual basis. If the data
        is daily, set `periods` to 252. For other frequencies, adjust accordingly
        (e.g., hourly: 252 * 6.5, minutely: 252 * 6.5 * 60). Defaults to 252
        (assuming daily data).

    Returns
    -------
    float
        The annualized volatility of the investment.
    """
    return emp.annual_volatility(returns, annualization=periods)


def sharpe_ratio(
    returns: pd.Series | np.ndarray, rf: float = 0.0, periods: int = 252
) -> float:
    """Calculate the Sharpe ratio of a strategy or investment returns.

    Parameters
    ----------
    returns
        A pandas Series containing the returns of the strategy for a specific
        period. Each value in the Series should represent the percentage return
        for that period.

    rf
        The risk-free rate used for the calculation. Defaults to 0.0.

    periods
        The number of periods per year used for annualization. This is
        necessary to convert the Sharpe ratio to an annualized value.
        Common choices include:
            * Daily: 252
            * Hourly: 252 * 6.5
            * Minutely: 252 * 6.5 * 60
        Defaults to 252 (assuming daily data).

    Returns
    -------
    float
        The Sharpe ratio of the strategy.
    """
    return emp.sharpe_ratio(returns, risk_free=rf, annualization=periods)


def max_drawdown(returns: pd.Series | np.ndarray) -> float:
    """Calculate the maximum drawdown experienced by a series of returns.

    Parameters
    ----------
    returns
        A pandas Series or NumPy ndarray containing the investment's historical
        returns. Each element represents the return for a specific period
        (e.g., daily, monthly).

    Returns
    -------
    float
        The maximum drawdown experienced by the investment.
    """
    return emp.max_drawdown(returns)


def var(returns: pd.Series | np.ndarray) -> float:
    """Calculate the Value at Risk (VaR) for a series of returns.

    Parameters
    ----------
    returns
        A pandas Series or NumPy ndarray containing the investment's historical
        returns. Each element represents the return for a specific period
        (e.g., daily, monthly).

    Returns
    -------
    float
        The VaR of the investment at the 95% confidence level.
    """
    return emp.value_at_risk(returns)


def longest_dd_period(returns: pd.Series | np.ndarray) -> int:
    """Find the duration of the longest drawdown period in a return series.

    Parameters
    ----------
    returns
        A pandas Series or NumPy ndarray containing the investment's historical
        returns. Each element represents the return for a specific period
        (e.g., daily, monthly).

    Returns
    -------
    int
        The duration (number of periods) of the longest drawdown period.
    """
    dd_series = emp.stats.drawdown_series(returns)
    duration = [0] * len(dd_series)
    for i in range(1, len(dd_series)):
        curr_dd = dd_series.iloc[i]
        duration[i] = 0 if curr_dd == 0 else duration[i - 1] + 1
    return max(duration)


def win_rate(position_outcome: pd.Series) -> float:
    """Calcuate the percentage difference between
    number of losing trades and winnig trades

    Parameters
    ----------
    position_outcome
        A pandas Series containing the outcome of each trade, where positive
        values represent wins and negative values represent losses.

    Returns
    -------
    float
        The win rate as a percentage between 0 and 1.
    """
    result = position_outcome.gt(0).astype(int)
    win_rate = result.sum() / len(result)
    return win_rate


def expectancy(position_outcome: pd.Series) -> float:
    """Calculate the expectancy (average profit per trade)
    based on win rate, average win size, and average loss size.

    Parameters
    ----------
    position_outcome
        A pandas Series containing the outcome of each trade, where positive
        values represent wins and negative values represent losses.

    Returns
    -------
    float
        The expectancy, representing the average profit per trade.
    """
    avg_win = position_outcome.loc[position_outcome > 0].mean()
    avg_win = avg_win if not pd.isna(avg_win) else 0.0
    avg_loss = -1 * position_outcome.loc[position_outcome < 0].mean()
    avg_loss = avg_loss if not pd.isna(avg_loss) else 0.0
    win_perc = win_rate(position_outcome)
    loss_perc = 1 - win_perc
    return win_perc * avg_win - loss_perc * avg_loss


def profit_factor(position_outcome: pd.Series) -> float:
    """Calculates the profit factor(ratio of average win size to average loss size).

    Parameters
    ----------
    position_outcome
        A pandas Series containing the outcome of each trade, where positive
        values represent wins and negative values represent losses.

    Returns
    -------
    float
        The profit factor, representing the ratio of average win to average loss.
    """
    gross_win = position_outcome.loc[position_outcome > 0].sum()
    gross_loss = -1 * position_outcome.loc[position_outcome < 0].sum()
    if gross_loss > 0:
        return gross_win / gross_loss
    return gross_win


def returns_stats(returns: pd.Series) -> pd.DataFrame:
    """
    Get return statistics with pyfolio.

    Parameters
    ----------
    returns
        Daily equity/strategy returns.

    Returns
    -------
    pd.DataFrame
        A single column dataframe containing various metrics
        indexed the corresponding names.

    """

    assert isinstance(
        returns.index, pd.DatetimeIndex
    ), "Series must have pandas datetime index"

    stats = pf.show_perf_stats(returns, return_df=True)
    stats = (
        stats.Backtest.apply(
            lambda x: str(round(float(x.split("%")[0]), 2)) + "%"
            if isinstance(x, str)
            else x
        )
        .apply(lambda x: round(x, 2) if isinstance(x, float) else x)
        .to_frame()
    )
    return stats
