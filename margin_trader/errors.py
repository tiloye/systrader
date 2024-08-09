class LimitOrderError(ValueError):
    """Error related to limit orders."""

    pass


class MarketOrderError(ValueError):
    """Error related to market orders."""

    pass


class StopOrderError(ValueError):
    """Error related to stop orders."""

    pass


class StopLossPriceError(ValueError):
    """Error related to stop loss price."""

    pass


class TakeProfitPriceError(ValueError):
    """Error related to take profit price."""

    pass
