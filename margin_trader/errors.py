class OrderError(Exception):
    """Error related to orders."""

    pass


class LimitOrderError(OrderError):
    """Error related to limit orders."""

    pass


class MarketOrderError(OrderError):
    """Error related to market orders."""

    pass


class StopOrderError(OrderError):
    """Error related to stop orders."""

    pass


class StopLossPriceError(OrderError):
    """Error related to stop loss price."""

    pass


class TakeProfitPriceError(OrderError):
    """Error related to take profit price."""

    pass
