from __future__ import annotations

from collections import namedtuple
from copy import deepcopy
from typing import TYPE_CHECKING

from margin_trader.constants import OrderSide, OrderStatus, OrderType
from margin_trader.errors import (
    LimitOrderError,
    MarketOrderError,
    OrderError,
    StopLossPriceError,
    StopOrderError,
    TakeProfitPriceError,
)

if TYPE_CHECKING:
    from datetime import datetime

    from margin_trader.broker.position import Position
    from margin_trader.broker.sim_broker import SimBroker


class Order:
    """
    Represents an order event sent to an execution system.
    The order contains a symbol (e.g. GOOG), a type (market, limit, or stop),
    units and a direction.

    Parameters
    ----------
    timestamp:
        The time when the Order was created.
    symbol
        The symbol to trade.
    order_type : OrderType
        Type of order (MARKET, LIMIT, STOP).
    units
        Non-negative integer for order quantity.
    side
        'BUY' or 'SELL' for long or short.
    price
       Execution price of LIMIT or STOP orders.
    sl
        Stop loss price for closing the position.
    tp
        Take profit price for closing the position.
    order_id
        The ID of the order.
    position_id
        The ID of the position an order should operate on. Used to identify orders that
        closed or modify a position.

    Attributes
    ----------
    type : str
        The type of the event, in this case 'ORDER'.
    symbol : str
        The symbol to trade.
    order_type : OrderType
        Type of order (MARKET, LIMIT, STOP).
    units : int
        Non-negative integer for order quantity.
    side : OrderSide
        BUY or SELL for long or short.
    price : float | None
        Execution price of LIMIT or STOP orders.
    sl : float | None
        Stop loss price for closing the position.
    tp : float | None
        Take profit price for closing the position.
    status : str
        The status of the order.
    request : str
        The type of request the order fulfilled. Can be "open" (opened a position) or
        "close" (closed a position).
    order_id : int
        The ID of the order (assigned by broker).
    position_id: int
        The ID of the position an order should operate on. Used to identify orders that
        closed or modify a position.
    """

    def __init__(
        self,
        *,
        timestamp: datetime,
        symbol: str,
        order_type: OrderType,
        units: int,
        side: OrderSide,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        order_id: int = 0,
        position_id: int = 0,
    ) -> None:
        self.timestamp = timestamp
        self.symbol = symbol
        self.order_type = order_type
        self.units = units
        self.side = side
        self.price = price
        self.sl = sl
        self.tp = tp
        self.status = OrderStatus.PENDING
        self.order_id = order_id
        self.position_id = position_id
        self.request = ""

    def execute(self) -> None:
        self.status = OrderStatus.EXECUTED

    def reject(self) -> None:
        self.status = OrderStatus.REJECTED

    def is_bracket_order(self):
        if isinstance(self.sl, float) and isinstance(self.tp, float):
            return True
        return False

    def is_cover_order(self):
        if self.sl is not None and self.tp is None:
            return True
        elif self.sl is None and self.tp is not None:
            return True
        else:
            return False

    def print_order(self) -> None:
        """
        Outputs the values within the Order.
        """
        print(
            "Order: Symbol=%s, Type=%s, units=%s, Direction=%s"
            % (self.symbol, self.order_type, self.units, self.side)
        )


CoverOrder = namedtuple("CoverOrder", ["primary_order", "cover_order", "id"])
BracketOrder = namedtuple(
    "BracketOrder", ["primary_order", "stop_order", "limit_order", "id"]
)
ReverseOrder = namedtuple("ReverseOrder", ["close_order", "open_order", "id"])


class OrderManager:
    """Creates and tracks orders to be submitted to the backtest broker."""

    def __init__(self, broker: SimBroker):
        self.broker = broker
        self.pending_orders = {}
        self.history = []
        self.__order_id = 1

    def create_order(
        self,
        *,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        units: int = 100,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        position_id: int = 0,
    ) -> Order | ReverseOrder | int:
        """
        Create a market, limit, or stop order.

        Parameters
        ----------
        symbol
            The symbol for the order.
        order_type
            The type of the order.
        side
            The side of the order, either "BUY" or "SELL".
        units
            The number of units, default is 100.
        price
            Execution price of LMT or STP orders.
        sl
            Stop loss price for closing the position.
        tp
            Take profit price for closing the position.
        position_id
            The position ID of an existing position. If the value is less than the order
            ID, then the order will close or reverse an existing position.

        Returns
        -------
        Order
            The market order to be executed by the broker
        ReverseOrder
            The reverse order to be executed
        int
            The order id of the pending order that was created

        Raises
        ------
        OrderError
            If order_type is not in OrderType
        MarketOrderError
            If order_type is OrderType.MARKET and price is None
        LimitOrderError
            If order_type is OrderType.LIMIT and price is None
            If order_type is OrderType.LIMIT and price is invalid
        StopOrderError
            If order_type is OrderType.STOP and price is None
            If order_type is OrderType.STOP and price is invalid
        """

        self.__verify_order(symbol, order_type, side, price, sl, tp)

        if self.broker.acct_mode == "netting":
            order = self.__create_net_order(
                symbol, order_type, side, units, price, sl, tp, position_id
            )
            self.__order_id += 1
            return order
        elif self.broker.acct_mode == "hedging":
            order = self.__create_hedge_order(
                symbol, order_type, side, units, price, sl, tp, position_id
            )
            self.__order_id += 1
            return order
        else:
            raise ValueError(f"Unknown broker account mode, {self.broker.acct_mode}.")

    def __verify_order(self, symbol, order_type, side, price, sl, tp) -> None:
        if order_type not in [OrderType.MARKET, OrderType.LIMIT, OrderType.STOP]:
            raise OrderError("Invalid order type")

        if order_type == OrderType.MARKET and price is not None:
            raise MarketOrderError("Market order price should be 'None'.")
        elif order_type == OrderType.LIMIT:
            self.__verify_lmt_order(symbol, side, price)
        elif order_type == OrderType.STOP:
            self.__verify_stp_order(symbol, side, price)

        if sl is not None or tp is not None:
            self.__verify_sl_tp_price(symbol, side, price, sl, tp)

    def __verify_lmt_order(self, symbol: str, side: str, price: float) -> None:
        if price is None:
            raise LimitOrderError("Limit order requires price.")

        curr_price = self.broker.data_handler.get_latest_price(symbol)
        if side == OrderSide.BUY:
            if price >= curr_price:
                raise LimitOrderError(
                    "Buy limit price must be less than current market price."
                )
        else:
            if price <= curr_price:
                raise LimitOrderError(
                    "Sell limit price must be greater than current market price."
                )

    def __verify_stp_order(self, symbol, side: str, price: float) -> None:
        if price is None:
            raise StopOrderError("Stop order requires price.")

        curr_price = self.broker.data_handler.get_latest_price(symbol)
        if side == OrderSide.BUY:
            if price <= curr_price:
                raise StopOrderError(
                    "Buy stop price must be greater than current market price."
                )
        else:
            if price >= curr_price:
                raise StopOrderError(
                    "Sell stop price must be less than current market price."
                )

    def __verify_sl_tp_price(
        self,
        symbol: str,
        side: str,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ) -> None:
        if price is None:
            price = self.broker.data_handler.get_latest_price(symbol)

        if sl:
            if side == OrderSide.BUY:
                if sl >= price:
                    raise StopLossPriceError(
                        "Stop loss price must be less than buy price."
                    )
            else:
                if sl <= price:
                    raise StopLossPriceError(
                        "Stop loss price must be greater than sell price."
                    )

        if tp:
            if side == OrderSide.BUY:
                if tp <= price:
                    raise TakeProfitPriceError(
                        "Take profit price must be greater than buy price."
                    )
            else:
                if tp >= price:
                    raise TakeProfitPriceError(
                        "Take profit price must be less than sell price."
                    )

    def __create_net_order(
        self,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        units: int = 100,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        position_id: int = 0,
    ) -> Order | ReverseOrder | int:
        order = Order(
            timestamp=self.broker.data_handler.current_datetime,
            symbol=symbol,
            order_type=order_type,
            units=units,
            side=side,
            price=price,
            sl=sl,
            tp=tp,
            order_id=self.__order_id,
            position_id=self.__order_id if position_id == 0 else position_id,
        )

        pos = self.broker.get_position(order.symbol)
        if pos:  # There is an open position
            if (
                order.order_id == order.position_id
            ):  # Call from broker's buy or sell method
                if order.units > pos.units:  # type: ignore
                    return self.__create_reverse_order(order, pos)  # type: ignore
                elif order.units <= pos.units:  # type: ignore
                    order.position_id = pos.id  # type: ignore
                    order.request = "close"
                    return self.__create_regular_order(order)
                else:
                    raise NotImplementedError("Unknown order request")
            else:  # Call from call from broker close method
                order.request = "close"
                return self.__create_regular_order(order)
        else:  # No open position
            order.request = "open"
            if order.is_bracket_order():
                return self.__create_bracket_order(order)
            elif order.is_cover_order():
                return self.__create_cover_order(order)
            else:
                return self.__create_regular_order(order)

    def __create_reverse_order(
        self, order: Order, position: Position
    ) -> ReverseOrder | int:
        order1 = deepcopy(order)
        order1.units = position.units
        order1.position_id = position.id
        order1.request = "close"

        order2 = order
        order2.units = order.units - position.units
        order2.request = "open"

        self.history.append(order1)
        self.history.append(order2)

        rorder = ReverseOrder(order1, order2, order.order_id)
        if order.order_type == OrderType.MARKET and self.broker._exec_price == "next":
            self.pending_orders[rorder.id] = rorder
            return rorder.id
        else:
            return rorder

    def __create_hedge_order(
        self,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        units: int = 100,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        position_id: int = 0,
    ) -> Order | int:
        order = Order(
            timestamp=self.broker.data_handler.current_datetime,
            symbol=symbol,
            order_type=order_type,
            units=units,
            price=price,
            side=side,
            sl=sl,
            tp=tp,
            order_id=self.__order_id,
            position_id=self.__order_id if position_id == 0 else position_id,
        )

        if order.order_id == order.position_id:  # Call from broker's buy/sell method
            order.request = "open"
            if order.is_bracket_order():
                return self.__create_bracket_order(order)
            elif order.is_cover_order():
                return self.__create_cover_order(order)
            else:
                return self.__create_regular_order(order)
        else:  # Call from broker's close
            order.request = "close"
            return self.__create_regular_order(order)

    def __create_bracket_order(self, order: Order) -> Order | int:
        border = self.__get_bracket_orders(order)
        self.pending_orders[border.id] = border
        self.history.append(border.primary_order)
        self.history.append(border.stop_order)
        self.history.append(border.limit_order)
        return border.id

    def __create_cover_order(self, order: Order) -> Order | int:
        corder = self.__get_cover_order(order, sl=True if order.sl else False)
        self.pending_orders[corder.id] = corder
        self.history.append(corder.primary_order)
        self.history.append(corder.cover_order)
        return corder.id

    def __create_regular_order(self, order: Order) -> Order | int:
        # Create orders without bracket or cover orders
        if order.order_type == OrderType.MARKET:
            if self.broker._exec_price == "next":
                self.pending_orders[order.order_id] = order
                self.history.append(order)
                return order.order_id
            else:
                self.history.append(order)
                return order
        else:
            self.pending_orders[order.order_id] = order
            self.history.append(order)
            return order.order_id

    def __get_bracket_orders(self, order: Order) -> BracketOrder:
        if order.side == OrderSide.BUY:
            sl_order = Order(
                timestamp=order.timestamp,
                symbol=order.symbol,
                order_type=OrderType.STOP,
                units=order.units,
                side=OrderSide.SELL,
                price=order.sl,
                order_id=order.order_id,
                position_id=order.position_id,
            )
            sl_order.request = "close"

            tp_order = Order(
                timestamp=order.timestamp,
                symbol=order.symbol,
                order_type=OrderType.LIMIT,
                units=order.units,
                side=OrderSide.SELL,
                price=order.tp,
                order_id=order.order_id,
                position_id=order.position_id,
            )
            tp_order.request = "close"
            return BracketOrder(order, sl_order, tp_order, order.order_id)
        else:
            sl_order = Order(
                timestamp=order.timestamp,
                symbol=order.symbol,
                order_type=OrderType.STOP,
                units=order.units,
                side=OrderSide.BUY,
                price=order.sl,
                order_id=order.order_id,
                position_id=order.position_id,
            )
            sl_order.request = "close"

            tp_order = Order(
                timestamp=order.timestamp,
                symbol=order.symbol,
                order_type=OrderType.LIMIT,
                units=order.units,
                side=OrderSide.BUY,
                price=order.tp,
                order_id=order.order_id,
                position_id=order.position_id,
            )
            tp_order.request = "close"
            return BracketOrder(order, sl_order, tp_order, order.order_id)

    def __get_cover_order(self, order: Order, sl: bool) -> CoverOrder:
        if order.side == OrderSide.BUY:
            if sl:
                stp_order = Order(
                    timestamp=order.timestamp,
                    symbol=order.symbol,
                    order_type=OrderType.STOP,
                    units=order.units,
                    side=OrderSide.SELL,
                    price=order.sl,
                    order_id=order.order_id,
                    position_id=order.position_id,
                )
                stp_order.request = "close"
                return CoverOrder(order, stp_order, order.order_id)
            else:
                lmt_order = Order(
                    timestamp=order.timestamp,
                    symbol=order.symbol,
                    order_type=OrderType.LIMIT,
                    units=order.units,
                    side=OrderSide.SELL,
                    price=order.tp,
                    order_id=order.order_id,
                    position_id=order.position_id,
                )
                lmt_order.request = "close"
                return CoverOrder(order, lmt_order, order.order_id)
        else:
            if sl:
                stp_order = Order(
                    timestamp=order.timestamp,
                    symbol=order.symbol,
                    order_type=OrderType.STOP,
                    units=order.units,
                    side=OrderSide.BUY,
                    price=order.sl,
                    order_id=order.order_id,
                    position_id=order.position_id,
                )
                stp_order.request = "close"
                return CoverOrder(order, stp_order, order.order_id)
            else:
                lmt_order = Order(
                    timestamp=order.timestamp,
                    symbol=order.symbol,
                    order_type=OrderType.LIMIT,
                    units=order.units,
                    side=OrderSide.BUY,
                    price=order.tp,
                    order_id=order.order_id,
                    position_id=order.position_id,
                )
                lmt_order.request = "close"
                return CoverOrder(order, lmt_order, order.order_id)

    def reset(self):
        self.pending_orders = {}
        self.history = []
        self.__order_id = 1
