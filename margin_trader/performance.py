import numpy as np
import pandas as pd
import empyrical as emp

def calculate_total_return(returns: pd.Series|np.ndarray):
    """Calculate return on investment from returns history"""
    roi = emp.cum_returns_final(returns)
    return roi
    

def calculate_sharpe_ratio(
        returns: pd.Series|np.ndarray,
        rf: float=0.0,
        periods: int=252
    ) -> float:
    """
    Calculates the Sharpe ratio of a strategy.

    The Sharpe ratio is a measure of risk-adjusted return. It is defined as the 
    average return of an investment minus the risk-free rate, divided by the 
    standard deviation of the investment's returns.

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

