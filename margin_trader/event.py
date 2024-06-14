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
    """

    def __init__(self) -> None:
        """
        Initialises the MarketEvent.
        """
        self.type = "MARKET"


class SignalEvent(Event):
    """
    Handles the event of sending a Signal from a Strategy object.
    This is received by a Portfolio object and acted upon.
    """

    def __init__(self, symbol: str, timeindex: datetime, signal_type: str):
        """
        Initialises the SignalEvent.

        Parameters:
        symbol - The ticker symbol, e.g. 'GOOG'.
        datetime - The timestamp at which the signal was generated.
        signal_type - 'LONG' or 'SHORT'.
        """

        self.type = "SIGNAL"
        self.symbol = symbol
        self.timeindex = timeindex
        self.signal_type = signal_type


class OrderEvent(Event):
    """
    Handles the event of sending an Order to an execution system.
    The order contains a symbol (e.g. GOOG), a type (market or limit),
    units and a direction.
    """

    def __init__(self, symbol: str, order_type: str, units: int, side: str) -> None:
        """
        Initialises the order type, setting whether it is
        a Market order ('MKT') or Limit order ('LMT'), has
        a units (integral) and its direction ('BUY' or
        'SELL').

        Parameters:
        symbol - The instrument to trade.
        order_type - 'MKT' or 'LMT' for Market or Limit.
        units - Non-negative integer for units.
        side - 'BUY' or 'SELL' for long or short.
        """

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
    from a brokerage. Stores the units of an instrument
    actually filled and at what price. In addition, stores
    the commission of the trade from the brokerage.
    """

    def __init__(
        self,
        timeindex: datetime,
        symbol: str,
        units: int,
        side: str,
        fill_price: float,
        commission: float | None = None,
        result: str = "open",
        id: int = 0,
    ) -> None:
        """
        Initialises the FillEvent object. Sets the symbol, exchange,
        units, direction, cost of fill and an optional
        commission.

        If commission is not provided, the Fill object will
        calculate it based on the trade size and Interactive
        Brokers fees.

        Parameters:
        timeindex - The bar-resolution when the order was filled.
        symbol - The instrument which was filled.
        units - The number of units filled.
        side - The direction of fill ('BUY' or 'SELL')
        fill_price - The price the order was filled.
        commission - An optional commission sent from IB.
        result - The position outcome of an executed order ("open" or "close").
        id - The order id for tracking the position
        """

        self.type = "FILL"
        self.timeindex = timeindex
        self.symbol = symbol
        self.units = units
        self.side = side
        self.fill_price = fill_price
        self.result = result
        self.id = id

        # Calculate commission
        if commission is None:
            self.commission = self.calculate_ib_commission()
        else:
            self.commission = commission

    @property
    def is_close(self) -> bool:
        if self.result == "close":
            return True
        return False

    def calculate_ib_commission(self) -> float:
        """
        Calculates the fees of trading based on an Interactive
        Brokers fee structure for API, in USD.

        This does not include exchange or ECN fees.

        Based on "US API Directed Orders":
        https://www.interactivebrokers.com/en/index.php?f=commission&p=stocks2
        """
        full_cost = 1.3
        if self.units <= 500:
            full_cost = max(1.3, 0.013 * self.units)
        else:  # Greater than 500
            full_cost = max(1.3, 0.008 * self.units)
        full_cost = min(full_cost, 0.5 / 100.0 * self.units * self.fill_price)
        return full_cost
