import numpy as np
import pandas as pd
import empyrical as emp

def calculate_total_return(returns: pd.Series|np.ndarray) -> float:
    """Calculates the total return on investment (ROI) from a series of returns.

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

def calculate_annual_return(returns: pd.Series|np.ndarray, periods: int=252) -> float:
    """Calculates the annualized return of a series of returns.

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

def calculate_annual_volatility(returns: pd.Series|np.ndarray,
                                periods: int=252) -> float:
    """Calculates the annualized volatility of a series of returns.

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

def calculate_sharpe_ratio(
        returns: pd.Series|np.ndarray,
        rf: float=0.0,
        periods: int=252
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

def calculate_max_drawdown(returns: pd.Series|np.ndarray) -> float:
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

def calculate_var(returns: pd.Series|np.ndarray) -> float:
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

def calculate_longest_dd_period(returns: pd.Series|np.ndarray) -> int:
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
    curr_dd = dd_series.iloc[0]
    duration = [0] * len(dd_series)
    for i in range(1, len(dd_series)):
        curr_dd = dd_series.iloc[i]
        duration[i] = 0 if curr_dd == 0 else duration[i-1] + 1
    return max(duration)

