class OrderError(Exception):
    pass


class LimitOrderError(OrderError):
    pass


class MarketOrderError(OrderError):
    pass


class StopOrderError(OrderError):
    pass


class StopLossPriceError(OrderError):
    pass


class TakeProfitPriceError(OrderError):
    pass
