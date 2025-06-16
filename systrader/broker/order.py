from __future__ import annotations

from collections import namedtuple
from copy import deepcopy
from typing import TYPE_CHECKING

from systrader.constants import OrderSide, OrderStatus, OrderType
from systrader.errors import (
    LimitOrderError,
    MarketOrderError,
    OrderError,
    StopLossPriceError,
    StopOrderError,
    TakeProfitPriceError,
)

if TYPE_CHECKING:
    from datetime import datetime

    from systrader.broker.position import Position
    from systrader.broker.sim_broker import SimBroker


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
    type
        The type of the event, in this case 'ORDER'.
    symbol
        The symbol to trade.
    order_type : OrderType
        Type of order (MARKET, LIMIT, STOP).
    units
        Non-negative integer for order quantity.
    side
        BUY or SELL for long or short.
    price
        Execution price of LIMIT or STOP orders.
    sl
        Stop loss price for closing the position.
    tp
        Take profit price for closing the position.
    status
        The status of the order.
    request
        The type of request the order fulfilled. Can be "open" (opened a position) or
        "close" (closed a position).
    order_id
        The ID of the order (assigned by broker).
    position_id
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


ReverseOrder = namedtuple("ReverseOrder", ["close_order", "open_order", "order_id"])


def create_sl_tp_order_from_order(
    order: Order, order_side: OrderSide, order_type: OrderType, price: float
):
    sl_tp_order = Order(
        timestamp=order.timestamp,
        symbol=order.symbol,
        order_type=order_type,
        units=order.units,
        side=order_side,
        price=price,
        order_id=order.order_id,
        position_id=order.position_id,
    )
    sl_tp_order.request = "close"
    return sl_tp_order


class CoverOrder:
    def __init__(self, order: Order):
        self.primary_order = order
        self.order_id = order.order_id
        self.sl = order.sl
        self.tp = order.tp
        self.cover_order = self._get_cover_order()

    def _get_cover_order(self):
        if self.primary_order.side == OrderSide.BUY:
            if self.primary_order.sl:
                return create_sl_tp_order_from_order(
                    self.primary_order,
                    OrderSide.SELL,
                    OrderType.STOP,
                    self.primary_order.sl,
                )
            else:
                return create_sl_tp_order_from_order(
                    self.primary_order,
                    OrderSide.SELL,
                    OrderType.LIMIT,
                    self.primary_order.tp,
                )
        else:
            if self.primary_order.sl:
                return create_sl_tp_order_from_order(
                    self.primary_order,
                    OrderSide.BUY,
                    OrderType.STOP,
                    self.primary_order.sl,
                )
            else:
                return create_sl_tp_order_from_order(
                    self.primary_order,
                    OrderSide.BUY,
                    OrderType.LIMIT,
                    self.primary_order.tp,
                )


class BracketOrder:
    def __init__(self, order: Order):
        self.primary_order = order
        self.order_id = order.order_id
        self.sl = order.sl
        self.tp = order.tp
        self.sl_order, self.tp_order = self._get_bracket_order()

    def _get_bracket_order(self):
        if self.primary_order.side == OrderSide.BUY:
            sl_order = create_sl_tp_order_from_order(
                self.primary_order,
                OrderSide.SELL,
                OrderType.STOP,
                self.primary_order.sl,
            )
            tp_order = create_sl_tp_order_from_order(
                self.primary_order,
                OrderSide.SELL,
                OrderType.LIMIT,
                self.primary_order.tp,
            )
            return sl_order, tp_order
        else:
            sl_order = create_sl_tp_order_from_order(
                self.primary_order, OrderSide.BUY, OrderType.STOP, self.primary_order.sl
            )
            tp_order = create_sl_tp_order_from_order(
                self.primary_order,
                OrderSide.BUY,
                OrderType.LIMIT,
                self.primary_order.tp,
            )
            return sl_order, tp_order


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

    def __verify_order(
        self,
        symbol: str,
        order_type: OrderType,
        side: OrderSide,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ) -> None:
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

    def __verify_lmt_order(self, symbol: str, side: OrderSide, price: float | None) -> None:
        if price is None:
            raise LimitOrderError("Limit order requires price.")

        curr_price = self.broker.data_handler.get_latest_price(
            symbol, self.broker._trading_price
        )
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

    def __verify_stp_order(self, symbol, side: OrderSide, price: float | None) -> None:
        if price is None:
            raise StopOrderError("Stop order requires price.")

        curr_price = self.broker.data_handler.get_latest_price(
            symbol, self.broker._trading_price
        )
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
        side: OrderSide,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ) -> None:
        if price is None:  # Market order
            price = self.broker.data_handler.get_latest_price(
                symbol, self.broker._trading_price
            )

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
            timestamp=self.broker.data_handler.timestamp,
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
        if order.order_type in (OrderType.LIMIT, OrderType.STOP):
            self.pending_orders[rorder.order_id] = rorder
            return rorder.order_id
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
            timestamp=self.broker.data_handler.timestamp,
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
        border = BracketOrder(order)
        self.pending_orders[border.order_id] = border
        self.history.append(border.primary_order)
        self.history.append(border.sl_order)
        self.history.append(border.tp_order)
        return border.order_id

    def __create_cover_order(self, order: Order) -> Order | int:
        corder = CoverOrder(order)
        self.pending_orders[corder.order_id] = corder
        self.history.append(corder.primary_order)
        self.history.append(corder.cover_order)
        return corder.order_id

    def __create_regular_order(self, order: Order) -> Order | int:
        # Create orders without bracket or cover orders
        if order.order_type == OrderType.MARKET:
            self.history.append(order)
            return order
        else:
            self.pending_orders[order.order_id] = order
            self.history.append(order)
            return order.order_id

    def cancel_order(self, order_id: int) -> Order:
        order = self.pending_orders[order_id]

        if isinstance(order, (CoverOrder, BracketOrder)):
            if order.primary_order.status == OrderStatus.PENDING:
                order.primary_order.status = OrderStatus.CANCELED

            if isinstance(order, CoverOrder):
                order.cover_order.status = OrderStatus.CANCELED
            else:
                order.sl_order.status = OrderStatus.CANCELED
                order.tp_order.status = OrderStatus.CANCELED
        else:
            order.status = OrderStatus.CANCELED

        del self.pending_orders[order_id]
        return order

    def modify_order(
        self,
        order_id: int,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ):
        order = self.pending_orders[order_id]

        if isinstance(order, Order):
            return self._modify_order(order, price, sl, tp)
        elif isinstance(order, CoverOrder):
            return self._modify_cover_order(order, price, sl, tp)
        else:
            return self._modify_bracket_order(order, price, sl, tp)

    def _modify_order(
        self,
        order: Order,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ):
        if price and order.status == OrderStatus.EXECUTED:
            raise ValueError("Cannot modify price of executed orders")

        price = order.price if price is None else price
        self.__verify_order(order.symbol, order.order_type, order.side, price)
        order.price = price

        if sl and tp:
            order.sl = sl
            order.tp = tp
            border = BracketOrder(order)
            self.pending_orders[border.primary_order.order_id] = border

            self.history.append(border.sl_order)
            self.history.append(border.tp_order)
        elif sl or tp:
            if sl:
                order.sl = sl
            else:
                order.tp = tp
            corder = CoverOrder(order)
            self.pending_orders[corder.primary_order.order_id] = corder
            self.history.append(corder.cover_order)

        return order.order_id

    def _modify_cover_order(
        self,
        order: CoverOrder,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ):
        if price and order.primary_order.status == OrderStatus.EXECUTED:
            raise ValueError("Cannot modify price of executed orders")

        porder = order.primary_order
        corder = order.cover_order
        price = porder.price if price is None else price
        self.__verify_order(
            porder.symbol, porder.order_type, porder.side, price, sl, tp
        )
        porder.price = price

        if sl and tp:
            porder.sl = sl
            porder.tp = tp
            border = BracketOrder(porder)
            self.pending_orders[porder.order_id] = border

            # Remove previous cover order from history and insert the new orders
            self.history.remove(corder)
            porder_idx = self.history.index(porder)
            self.history.insert(porder_idx + 1, border.sl_order)
            self.history.insert(porder_idx + 2, border.tp_order)
        elif sl or tp:
            if corder.order_type == OrderType.STOP:
                if sl:
                    corder.price = sl
                else:
                    corder.price = tp
                    corder.order_type = OrderType.LIMIT
            else:
                if tp:
                    corder.price = tp
                else:
                    corder.price = sl
                    corder.order_type = OrderType.STOP
        else:
            porder.sl = sl
            porder.tp = tp
            self.pending_orders[porder.order_id] = porder
            self.history.remove(corder)

        return porder.order_id

    def _modify_bracket_order(
        self,
        order: BracketOrder,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ):
        if price and order.primary_order.status == OrderStatus.EXECUTED:
            raise ValueError("Cannot modify price of executed orders")

        porder = order.primary_order
        sl_order = order.sl_order
        tp_order = order.tp_order
        price = porder.price if price is None else price
        self.__verify_order(
            porder.symbol, porder.order_type, porder.side, price, sl, tp
        )
        porder.price = price

        if sl and tp:
            sl_order.price = sl
            tp_order.price = tp
        elif sl or tp:
            porder.sl = sl
            porder.tp = tp
            corder = CoverOrder(porder)
            self.pending_orders[porder.order_id] = corder

            # Remove bracket order from history and insert new order
            self.history.remove(order.tp_order)
            self.history[self.history.index(order.sl_order)] = corder.cover_order
        else:
            porder.sl = sl
            porder.tp = tp
            self.pending_orders[porder.order_id] = porder

            self.history.remove(sl_order)
            self.history.remove(tp_order)

        return porder.order_id

    def modify_position(
        self, position_id: int, sl: float | None = None, tp: float | None = None
    ):
        order = list(
            filter(lambda order: order.position_id == position_id, self.history)
        )[0]
        return self._modify_order(order, sl=sl, tp=tp)

    def reset(self):
        self.pending_orders = {}
        self.history = []
        self.__order_id = 1
