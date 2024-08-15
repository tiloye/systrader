from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime

    from margin_trader.broker.order import OrderSide


class Fill:
    """
    Encapsulates the notion of a Filled Order, as returned
    from a brokerage.

    Stores the units of an instrument actually filled and at what price.
    In addition, stores the commission of the trade from the brokerage.

    Parameters
    ----------
    timestamp : datetime
        The time when the order was filled.
    symbol : str
        The symbol which was filled.
    units : int
        The number of units filled.
    side : OrderSide
        The direction of fill ('BUY' or 'SELL')
    fill_price : float
        The price the order was filled.
    commission : float
        An optional commission sent from IB.
    result : str
        The position outcome of an executed order ("open" or "close").
    order_id : int
        The ID of the order that triggered the position.
    position_id: int
        The ID needed for closing an open position.

    Attributes
    ----------
    type : str
        The type of the event, in this case 'FILL'.
    timestamp : datetime
        The bar-resolution when the order was filled.
    symbol : str
        The symbol which was filled.
    units : int
        The number of units filled.
    side : OrderSide
        The direction of fill ('BUY' or 'SELL').
    fill_price : float
        The price the order was filled.
    commission : float
        The commission of the trade from the brokerage.
    result : str
        The position outcome of an executed order ("open" or "close").
    order_id : int
        The order ID for tracking the position.
    position_id: int
        The ID needed for closing an open position.
    """

    def __init__(
        self,
        timestamp: datetime,
        symbol: str,
        units: int,
        side: OrderSide,
        fill_price: float,
        commission: float = 0.0,
        result: str = "open",
        order_id: int = 0,
        position_id: int = 0,
    ) -> None:
        self.type = "FILL"
        self.timestamp = timestamp
        self.symbol = symbol
        self.units = units
        self.side = side
        self.fill_price = fill_price
        self.commission = commission
        self.result = result
        self.order_id = order_id
        self.position_id = position_id

    @property
    def is_close(self) -> bool:
        if self.result == "close":
            return True
        return False
