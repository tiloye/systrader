from datetime import datetime


class Event:
    """
    Event is base class providing an interface for all subsequent
    (inherited) events, that will trigger further events in the
    trading infrastructure.
    """

    pass


class MarketEvent(Event):
    """
    Handles the event of receiving a new market update with
    corresponding bars.

    Attributes
    ----------
    type : str
        The type of the event, in this case 'MARKET'.
    """

    def __init__(self) -> None:
        """
        Initialises the MarketEvent.
        """
        self.type = "MARKET"


class OrderEvent(Event):
    """
    Handles the event of sending an Order to an execution system.
    The order contains a symbol (e.g. GOOG), a type (market or limit),
    units and a direction.

    Parameters
    ----------
    symbol : str
        The symbol to trade.
    order_type : str
        'MKT' or 'LMT' for Market or Limit orders.
    units : int
        Non-negative integer for order quantity.
    side : str
        'BUY' or 'SELL' for long or short.

    Attributes
    ----------
    type : str
        The type of the event, in this case 'ORDER'.
    symbol : str
        The symbol to trade.
    order_type : str
        'MKT' or 'LMT' for Market or Limit orders.
    units : int
        Non-negative integer for order quantity.
    side : str
        'BUY' or 'SELL' for long or short.
    status : str
        The status of the order.
    id : int
        The ID of the order.
    """

    def __init__(self, symbol: str, order_type: str, units: int, side: str) -> None:
        self.type = "ORDER"
        self.symbol = symbol
        self.order_type = order_type
        self.units = units
        self.side = side
        self.status = ""
        self.id = 0

    def print_order(self) -> None:
        """
        Outputs the values within the Order.
        """
        print(
            "Order: Symbol=%s, Type=%s, units=%s, Direction=%s"
            % (self.symbol, self.order_type, self.units, self.side)
        )


class FillEvent(Event):
    """
    Encapsulates the notion of a Filled Order, as returned
    from a brokerage.

    Stores the units of an instrument actually filled and at what price.
    In addition, stores the commission of the trade from the brokerage.

    Parameters
    ----------
    timeindex : datetime
        The bar-resolution when the order was filled.
    symbol : str
        The symbol which was filled.
    units : int
        The number of units filled.
    side : str
        The direction of fill ('BUY' or 'SELL')
    fill_price : float
        The price the order was filled.
    commission : float, optional
        An optional commission sent from IB.
    result : str, optional
        The position outcome of an executed order ("open" or "close").
    id : int, optional
        The order ID for tracking the position.

    Attributes
    ----------
    type : str
        The type of the event, in this case 'FILL'.
    timeindex : datetime
        The bar-resolution when the order was filled.
    symbol : str
        The symbol which was filled.
    units : int
        The number of units filled.
    side : str
        The direction of fill ('BUY' or 'SELL').
    fill_price : float
        The price the order was filled.
    commission : float
        The commission of the trade from the brokerage.
    result : str
        The position outcome of an executed order ("open" or "close").
    id : int
        The order ID for tracking the position.
    """

    def __init__(
        self,
        timeindex: datetime,
        symbol: str,
        units: int,
        side: str,
        fill_price: float,
        commission: float = 0.0,
        result: str = "open",
        id: int = 0,
    ) -> None:
        self.type = "FILL"
        self.timeindex = timeindex
        self.symbol = symbol
        self.units = units
        self.side = side
        self.fill_price = fill_price
        self.commission = commission
        self.result = result
        self.id = id

    @property
    def is_close(self) -> bool:
        if self.result == "close":
            return True
        return False
