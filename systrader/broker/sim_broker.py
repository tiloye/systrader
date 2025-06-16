from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from systrader.broker.broker import Broker
from systrader.broker.fill import Fill
from systrader.broker.order import (
    BracketOrder,
    CoverOrder,
    Order,
    OrderManager,
    ReverseOrder,
)
from systrader.broker.position import (
    HedgePositionManager,
    NetPositionManager,
    Position,
)
from systrader.constants import OrderSide, OrderStatus, OrderType
from systrader.datahandler import BacktestDataHandler
from systrader.event import FILLEVENT, ORDEREVENT, EventListener

if TYPE_CHECKING:
    from systrader.event import EventManager


class SimBroker(Broker, EventListener):
    """Simulate live trading on a broker account.

    This class simplifies trading by automatically converting order objects to fill
    objects without latency, slippage, or fill ratio issues.

    Parameters
    ----------
    balance
        The initial balance of the account. Default is 100_000.
    acct_mode
        The account mode (netting or hedging) managing positions. Default is netting.
    leverage
        The leverage for trading. Default is 1.
    commission
        The commission for each trade. Default is None.
    stop_out_level
        The level for closing all positions when their isn't enough maintenance margin.
        Default is 0.2 (20%)
    trading_price
        The price at which market orders are executed. Can be one of "open" or "close".
        Default is "close". This price is also used to update the value of open positions.

    Attributes
    ----------
    balance
        The current account balance.
    equity
        The total current account value (balance + unrealized profit/loss).
    free_margin
        The available margin for new positions.
    leverage
        The leverage ratio used for margin calculations.
    commission, optional
        The commission fee charged per trade (if applicable).
    account_history
        A list of dictionaries storing the historical balance and equity data.
        The dictionary has the following keys: timestamp, balance, and equity.
    _trading_price
        The price at which market orders are executed

    Notes
    -----
    * Orders are executed without any slippage assumptions.
    * Orders are rejected only if there isn't enough margin to execute the order.
    """

    def __init__(
        self,
        balance: int | float = 100_000,
        acct_mode: str = "netting",
        leverage: int = 1,
        commission: float = 0.0,
        stop_out_level: float = 0.2,
        trading_price: str = "close",
    ):
        self.balance = balance
        self.equity = balance
        self.free_margin = balance
        self.acct_mode = acct_mode
        self.leverage = leverage
        self.commission = commission
        self.account_history = []
        self._trading_price = trading_price
        self._stop_out_level = stop_out_level
        self._order_manager = OrderManager(self)
        if self.acct_mode == "netting":
            self._p_manager = NetPositionManager(self)
        else:
            self._p_manager = HedgePositionManager(self)  # type: ignore[assignment]
        self.__pos_hist_total = len(self._p_manager.history)  # For balance updates

    def add_event_manager(self, event_manager: EventManager) -> None:
        self.event_manager = event_manager

    def add_data_handler(self, data_handler: BacktestDataHandler) -> None:
        self.data_handler = data_handler

    def buy(
        self,
        *,
        symbol: str,
        order_type: OrderType = OrderType.MARKET,
        units: int = 100,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ) -> None:
        """
        Buy x units of symbol.

        Parameters
        ----------
        symbol
            The symbol to buy.
        order_type
            The type of order. Can be one of MKT (market), LMT (limit),
            or STP (stop) order. Default is "MKT".
        units
            The number of units to buy, default is 100.
        price
            Execution price of LMT or STP orders. Should be "None" for MKT orders.
        sl
            Stop loss price for closing the position.
        tp
            Take profit price for closing the position.
        """
        order = self._order_manager.create_order(
            symbol=symbol,
            order_type=order_type,
            side=OrderSide.BUY,
            units=units,
            price=price,
            sl=sl,
            tp=tp,
        )
        self.__submit(order)

    def sell(
        self,
        symbol: str,
        order_type: OrderType = OrderType.MARKET,
        units: int = 100,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ) -> None:
        """
        Sell x units of symbol.

        Parameters
        ----------
        symbol
            The symbol to sell.
        order_type
            The type of order. Can be one of MKT (market), LMT (limit),
            or STP (stop) order. Default is "MKT".
        units
            The number of units to sell, default is 100.
        price
            Execution price of LMT or STP orders. Should be "None" for MKT orders.
        sl
            Stop loss price for closing the position.
        tp
            Take profit price for closing the position.
        """
        order = self._order_manager.create_order(
            symbol=symbol,
            order_type=order_type,
            side=OrderSide.SELL,
            units=units,
            price=price,
            sl=sl,
            tp=tp,
        )
        self.__submit(order)

    def close(self, position: Position, units: int | None = None) -> None:
        """
        Close an existing position with an opposing order.

        Parameters
        ----------
        position
            The position to close.
        units
            The number of units to close. If no units is given,
            the full position will be closed
        """
        if not isinstance(position, Position):
            raise TypeError(
                "Only 'Position' objects can be closed."
                + f" Got '{type(position).__name__}' object instead."
            )

        side = position.side
        units = units if units is not None else position.units
        if side == OrderSide.BUY:
            order = self._order_manager.create_order(
                symbol=position.symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.SELL,
                units=units,
                position_id=position.id,
            )
            self.__submit(order)
        else:
            order = self._order_manager.create_order(
                symbol=position.symbol,
                order_type=OrderType.MARKET,
                side=OrderSide.BUY,
                units=units,
                position_id=position.id,
            )
            self.__submit(order)

        if position.id in self._order_manager.pending_orders:  # Has pending order
            self.cancel_order(position.id)

    def close_all_positions(self) -> None:
        """Close all open positions."""

        def close_all(positions):
            for p in positions:
                self.close(p)

        positions = list(self.get_positions().values())
        if positions:
            if self.data_handler.continue_backtest:
                close_all(positions)
            else:
                self._trading_price = (
                    "close" if self._trading_price == "open" else self._trading_price
                )
                close_all(positions)

    def cancel_order(self, order_id: int) -> None:
        """Cancel a pending order and notify broker listeners of changes to the order."""
        order = self._order_manager.cancel_order(order_id)
        self.event_manager.notify(ORDEREVENT, order)

    def __submit(self, order: Order | ReverseOrder | int) -> None:
        if isinstance(order, Order):  # Market order
            self.execute_order(order)
        elif isinstance(order, ReverseOrder):  # Reverses net position
            self.execute_order(order.close_order)
            self.execute_order(order.open_order)
        else:  # Pending order
            order_ = self._order_manager.pending_orders[order]
            if isinstance(order_, (CoverOrder, BracketOrder)):
                if order_.primary_order.order_type == OrderType.MARKET:
                    self.execute_order(order_.primary_order)
                    if (
                        order_.primary_order.status == OrderStatus.EXECUTED
                        and self._trading_price == "open"
                    ):
                        self.__submit_cover_bracket_order(order_)
                else:
                    if self._trading_price == "open":
                        self.execute_lmt_stp_order(order_.primary_order)
                        if order_.primary_order.status == OrderStatus.EXECUTED:
                            self.__submit_cover_bracket_order(order_)
            else:
                if self._trading_price == "open":
                    self.execute_lmt_stp_order(order_)

            self.event_manager.notify(ORDEREVENT, order_)

    def __submit_cover_bracket_order(self, order: CoverOrder | BracketOrder):
        if isinstance(order, CoverOrder):
            self.execute_lmt_stp_order(order.cover_order)
            if order.cover_order.status == OrderStatus.EXECUTED:
                del self._order_manager.pending_orders[order.order_id]
        else:
            self.execute_lmt_stp_order(order.sl_order)
            if order.sl_order.status == OrderStatus.PENDING:
                self.execute_lmt_stp_order(order.tp_order)
            if (
                order.sl_order.status == OrderStatus.EXECUTED
                or order.tp_order.status == OrderStatus.EXECUTED
            ):
                # If one of the bracket orders is executed, cancel the other one
                if order.sl_order.status == OrderStatus.EXECUTED:
                    order.tp_order.status = OrderStatus.CANCELED
                else:
                    order.sl_order.status = OrderStatus.CANCELED

                del self._order_manager.pending_orders[order.order_id]

    def __submit_pending_orders(self) -> None:
        """Executes pending orders. Called when new market data is received."""
        executed_orders = []
        order_ids = list(self._order_manager.pending_orders.keys())

        for order_id in order_ids:
            order = self._order_manager.pending_orders[order_id]

            if isinstance(order, ReverseOrder):
                self.__submit(order)
                executed_orders.append(order_id)
            elif isinstance(order, (CoverOrder, BracketOrder)):
                porder = order.primary_order
                if (
                    porder.order_type == OrderType.LIMIT
                    or porder.order_type == OrderType.STOP
                ) and porder.status == OrderStatus.PENDING:
                    self.execute_lmt_stp_order(porder)

                if porder.status == OrderStatus.EXECUTED:
                    self.__submit_cover_bracket_order(order)
            else:
                self.execute_lmt_stp_order(order)
                executed_orders.append(order_id)

        for order_id in executed_orders:
            del self._order_manager.pending_orders[order_id]

    def execute_lmt_stp_order(self, order: Order) -> None:
        if order.order_type == OrderType.LIMIT:
            bar = self.data_handler.get_latest_bars(order.symbol)[-1]
            if order.side == OrderSide.BUY:
                if bar.low <= order.price:
                    self.execute_order(order)
            else:
                if bar.high >= order.price:
                    self.execute_order(order)
        else:
            bar = self.data_handler.get_latest_bars(order.symbol)[-1]
            if order.side == OrderSide.BUY:
                if bar.high >= order.price:
                    self.execute_order(order)
            else:
                if bar.low <= order.price:
                    self.execute_order(order)

    def execute_order(self, order: Order) -> None:
        """
        Convert Order objects into Fill objects naively,
        i.e., without any latency, slippage, or fill ratio problems.

        Parameters
        ----------
        order
            Contains an object with order information.
        """
        if order.order_type == OrderType.MARKET:
            price = self.data_handler.get_latest_price(
                order.symbol, self._trading_price
            )
        elif order.order_type == OrderType.LIMIT or order.order_type == OrderType.STOP:
            price = order.price

        if order.request == "open":  # Order request type
            cost = self.__get_cost(order, price)
            if cost < self.free_margin:
                fill_event = Fill(
                    self.data_handler.timestamp,
                    order.symbol,
                    order.units,
                    order.side,
                    price,
                    self.commission,
                    order_id=order.order_id,
                    position_id=order.position_id,
                )
                order.execute()
                self.update_account(fill_event)
                self.event_manager.notify(FILLEVENT, fill_event)
            else:
                order.reject()
                self.event_manager.notify(ORDEREVENT, order)

        else:  # Close an existing position
            fill_event = Fill(
                self.data_handler.timestamp,
                order.symbol,
                order.units,
                order.side,
                price,
                self.commission,
                "close",
                order.order_id,
                order.position_id,
            )
            order.execute()
            self.update_account(fill_event)
            self.event_manager.notify(FILLEVENT, fill_event)

    def __get_cost(self, event: Order, price) -> float:
        if self.acct_mode == "netting":
            pos = self.get_position(event.symbol)
            if pos and event.units > pos.units:  # type: ignore[union-attr]
                net_units = event.units - pos.units  # type: ignore[union-attr]
                return (net_units * price) / self.leverage
            return (event.units * price) / self.leverage
        else:
            return (event.units * price) / self.leverage

    def update(self, event: None = None) -> None:
        """Updates account values when a market event occurs."""
        self.__submit_pending_orders()
        self.update_account(event=event)

    def update_account(self, event: None | Fill) -> None:
        """Update the account info based on market or fill events."""
        self.__update_positions(event)
        self.__update_fund_values(event)
        self.__update_account_history(event)
        self.__pos_hist_total = len(self.get_positions_history())
        if self.__margin_call():
            self._stop_simulation()

    def __update_positions(self, event: None | Fill) -> None:
        """Update positions based on market or fill events."""
        if event is None:  # Market event occurred
            self.__update_positions_on_market()
        elif isinstance(event, Fill):
            self.__update_positions_on_fill(event)

    def __update_positions_on_fill(self, event: Fill) -> None:
        """Add new positions to the porfolio"""
        self._p_manager.update_position_on_fill(event)

    def __update_positions_on_market(self) -> None:
        """Update portfolio holdings with the latest market price"""
        self._p_manager.update_position_on_market()

    def __update_fund_values(self, event: None | Fill) -> None:
        if event is None:  # Market event occurred
            self.__update_equity()
            self.__update_free_margin()
        elif isinstance(event, Fill):
            self.__update_balance()
            self.__update_equity()
            self.__update_free_margin()

    def __update_balance(self) -> None:
        """Update the account balance based on closed position from a fill event."""
        if len(self.get_positions_history()) > self.__pos_hist_total:
            self.balance += self._p_manager.history[-1].pnl

    def __update_equity(self) -> None:
        """Update the account equity based on market or fill events."""
        self.equity = self.balance + self._p_manager.get_total_pnl()

    def __update_free_margin(self) -> None:
        """Update the free margin available for opening positions."""
        self.free_margin = self.equity - self.get_used_margin()

    def __update_account_history(self, event: None | Fill) -> None:
        """Update the account history based on market or fill events."""
        timestamp = self.data_handler.timestamp
        if event is None:
            self.account_history.append(
                {"timestamp": timestamp, "balance": self.balance, "equity": self.equity}
            )
        elif isinstance(event, Fill):
            # If open positions change due to multiple operation on the same bar
            if len(self.get_positions_history()) > self.__pos_hist_total:
                recent_history = self.account_history[-1]
                if timestamp == recent_history["timestamp"]:
                    recent_history["balance"] = self.balance
                    recent_history["equity"] = self.equity

    def __margin_call(self) -> bool:
        try:
            margin_level = self.equity / self.get_used_margin()
        except ZeroDivisionError:
            return False
        else:
            if margin_level <= self._stop_out_level:
                return True
            return False

    def _stop_simulation(self) -> None:
        self.data_handler.continue_backtest = False
        self.close_all_positions()

    def get_used_margin(self) -> float:
        """
        Get the current used margin.

        Returns
        -------
        float
            The current used margin.
        """
        positions = self.get_positions().values()
        margin = sum(position.get_cost() for position in positions)
        margin = margin / self.leverage
        return margin

    def modify_order(
        self,
        order_id: int,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
    ) -> None:
        """Modify a pending order's price, sl, and/or tp."""

        order_id_ = self._order_manager.modify_order(order_id, price, sl, tp)
        order = self.get_pending_order(order_id_)
        self.event_manager.notify(ORDEREVENT, order)

    def get_pending_order(
        self, order_id: int
    ) -> Order | CoverOrder | BracketOrder | ReverseOrder:
        """Get orders pending execution."""

        return self._order_manager.pending_orders.get(order_id)

    def get_pending_orders(
        self,
    ) -> dict[int, Order | CoverOrder | BracketOrder | ReverseOrder]:
        return self._order_manager.pending_orders

    def modify_position(
        self, position_id: int, sl: float | None = None, tp: float | None = None
    ):
        """Add stop loss and/or take profit to a position."""
        order_id = self._order_manager.modify_position(position_id, sl, tp)
        order = self.get_pending_order(order_id)
        self.event_manager.notify(ORDEREVENT, order)

    def get_position(self, identifier: str | int) -> Position | list[Position] | None:
        """
        Get the current position(s) for a given symbol.

        Parameters
        ----------
        identifier
            The identifier for the position. For netting account, the identifier
            must be a str (symbol name) while for hedging account the identifier
            can be a str (symbol name) or int (position id).

        Returns
        -------
        Position
            The position associated with the identifier (symbol name or postion id).
        list[Position]
            List of positions with in an instrument when account mode is hedging.
        None
            No open position.


        Raises
        ------
        ValueError
            If account mode is netting and identifier is type int.
        """
        if self.acct_mode == "netting" and isinstance(identifier, int):
            raise ValueError(
                "Net account positions can only be accessed by symbol name"
            )
        return self._p_manager.get_position(identifier)  # type: ignore[arg-type]

    def get_positions(self) -> dict[str | int, Position]:
        """
        Get all open positions.

        Returns
        -------
        dict
            The positions dictionary. The keys are the symbol names if account
            mode is netting or position ID if account mode is hedging.
        """
        return self._p_manager.positions

    def get_positions_history(self) -> list[Position]:
        """
        Get the history of all positions.

        Returns
        -------
        list
            The history of positions.
        """
        return self._p_manager.history

    def get_order_history(self, N: None | int = 1) -> list:
        """
        Get the history of submitted orders.

        Parameters
        ----------
        N
            The number of historical orders to return. If None, then the full order
            history is returned

        Returns
        -------
        list
            Order history
        """
        return self._order_manager.history[-N:] if N else self._order_manager.history

    def get_account_history(self) -> dict[str, pd.DataFrame]:
        """
        Get the account balance and equity history.

        Returns
        -------
        dict
            A dictionary containing the balance and equity history
            and the positions history.
        """
        balance_equity = pd.DataFrame.from_records(self.account_history).set_index(
            "timestamp"
        )

        position_history = [vars(position) for position in self.get_positions_history()]
        position_history = pd.DataFrame.from_records(position_history)
        position_history.rename(
            columns={"fill_price": "open_price", "last_price": "close_price"},
            inplace=True,
        )
        position_history["side"] = position_history["side"].astype(str)
        position_history = position_history.reindex(
            columns=[
                "symbol",
                "side",
                "units",
                "open_price",
                "close_price",
                "commission",
                "pnl",
                "open_time",
                "close_time",
                "id",
            ]
        )

        order_history = [vars(order) for order in self.get_order_history(None)]
        order_history = pd.DataFrame.from_records(order_history)
        order_history["side"] = order_history["side"].astype(str)
        order_history["order_type"] = order_history["order_type"].astype(str)
        order_history["status"] = order_history["status"].astype(str)

        return {
            "balance_equity": balance_equity,
            "positions": position_history,
            "orders": order_history,
        }

    def reset(self, balance: float = 100_000) -> None:
        """Replace instance varibles to with their default values."""

        self.balance = balance
        self.equity = balance
        self.free_margin = balance
        self.account_history = []
        self._p_manager.reset()
        self._order_manager.reset()
