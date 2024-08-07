from copy import deepcopy
from queue import Queue

import pandas as pd

from margin_trader.broker.broker import Broker
from margin_trader.broker.position import (
    HedgePositionManager,
    NetPositionManager,
    Position,
)
from margin_trader.data_handlers import BacktestDataHandler
from margin_trader.event import Event, FillEvent, MarketEvent, OrderEvent


class SimBroker(Broker):
    """Simulate live trading on a broker account.

    This class simplifies trading by automatically converting order objects to fill
    objects without latency, slippage, or fill ratio issues. It can only handle market
    orders.

    Parameters
    ----------
    data_handler : DataHandler
        The data handler providing market data.
    balance : int or float, optional
        The initial balance of the account. Default is 100_000.
    acct_mode: str
        The account mode (netting or hedging) managing positions. Default is netting.
    leverage : int, optional
        The leverage for trading. Default is 1.
    commission : int or float, optional
        The commission for each trade. Default is None.
    stop_out_level : float
        The level for closing all positions when their isn't enough maintenance margin.
        Default is 0.2 (20%)
    exec_price : str, optional
        The execution price for orders. Can be "current" (current close price)
        or "next" (next open price). Default is "current".

    Attributes
    ----------
    balance : float
        The current account balance.
    equity : float
        The total current account value (balance + unrealized profit/loss).
    free_margin : float
        The available margin for new positions.
    data_handler : DataHandler
        A reference to the data handler object.
    leverage : int
        The leverage ratio used for margin calculations.
    commission : float, optional
        The commission fee charged per trade (if applicable).
    p_manager : PositionManager
        A reference to the position manager object.
    _exec_price : str
        The price at which orders are executed ("current" or "next").
    pending_orders : Queue
        A queue to store pending orders waiting for execution.
    account_history : list[dict]
        A list of dictionaries, with keys  storing historical account data.
        The dictionary has the following keys:
        * timestamp
        * balance
        * equity
    """

    def __init__(
        self,
        data_handler: BacktestDataHandler,
        balance: int | float = 100_000,
        acct_mode: str = "netting",
        leverage: int = 1,
        commission: float = 0.0,
        stop_out_level: float = 0.2,
        exec_price: str = "current",
    ):
        self.balance = balance
        self.equity = balance
        self.free_margin = balance
        self.data_handler = data_handler
        self.acct_mode = acct_mode
        self.leverage = leverage
        self.commission = commission
        self.pending_orders = Queue()
        self.account_history = []
        self.order_history = []
        self.p_manager: NetPositionManager | HedgePositionManager = (
            NetPositionManager(self.data_handler)
            if acct_mode == "netting"
            else HedgePositionManager(self.data_handler)
        )
        self._exec_price = exec_price
        self.__order_id = 1
        self.__stop_out_level = stop_out_level
        self.__pos_hist_total = len(self.p_manager.history)  # For balance updates

    def add_event_queue(self, event_queue: Queue[Event]) -> None:
        self.events = event_queue

    def buy(
        self,
        symbol: str,
        order_type: str = "MKT",
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
            Execution price of LMT or STP orders.
        sl
            Stop loss price for closing the position.
        tp
            Take profit price for closing the position.
        """
        if order_type == "MKT":
            if price is not None:
                raise ValueError("Do not specify price for Market order.")
            self.__verify_sl_tp_price(symbol, "BUY", price, sl, tp)
            self.__create_order(symbol, order_type, "BUY", units, price, sl, tp)
        elif order_type == "LMT":
            assert price is not None, "Must provide price for Limit Order."
            self.__verify_lmt_order(symbol, "BUY", price)
            self.__verify_sl_tp_price(symbol, "BUY", price, sl, tp)
            self.__create_order(symbol, order_type, "BUY", units, price, sl, tp)
        elif order_type == "STP":
            assert price is not None, "Must provide price for Stop Order."
            self.__verify_stp_order(symbol, "BUY", price)
            self.__verify_sl_tp_price(symbol, "BUY", price, sl, tp)
            self.__create_order(symbol, order_type, "BUY", units, price, sl, tp)

    def sell(
        self,
        symbol: str,
        order_type: str = "MKT",
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
            Execution price of LMT or STP orders.
        sl
            Stop loss price for closing the position.
        tp
            Take profit price for closing the position.
        """
        if order_type == "MKT":
            if price is not None:
                raise ValueError("Do not specify price for Market order.")
            self.__create_order(symbol, order_type, "SELL", units, price, sl, tp)
        elif order_type == "LMT":
            assert price is not None, "Must provide price for Limit order."
            self.__verify_lmt_order(symbol, "SELL", price)
            self.__create_order(symbol, order_type, "SELL", units, price, sl, tp)
        elif order_type == "STP":
            assert price is not None, "Must provide price for Stop Order."
            self.__verify_stp_order(symbol, "SELL", price)
            self.__create_order(symbol, order_type, "SELL", units, price, sl, tp)

    def __verify_lmt_order(self, symbol: str, side: str, price: float) -> None:
        curr_price = self.data_handler.get_latest_price(symbol)
        if side == "BUY":
            if price == curr_price:
                raise ValueError(
                    "Limit order price must not be equal to current price."
                )
            elif price > curr_price:
                raise ValueError(
                    "Limit order price must not be greater than current price."
                )
        else:
            if price == curr_price:
                raise ValueError(
                    "Limit order price must not be equal to current price."
                )
            elif price < curr_price:
                raise ValueError(
                    "Limit order price must not be less than current price."
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
            price = self.data_handler.get_latest_price(symbol)

        if sl:
            if side == "BUY":
                if sl >= price:
                    raise ValueError("Stop loss price must be less than buy price")
            else:
                if sl <= price:
                    raise ValueError("Stop loss price must be greater than sell price")

        if tp:
            if side == "BUY":
                if tp <= price:
                    raise ValueError("Take profit price must be greater than buy price")
            else:
                if tp >= price:
                    raise ValueError("Take profit price must be less than sell price")

    def __verify_stp_order(self, symbol, side: str, price: float) -> None:
        curr_price = self.data_handler.get_latest_price(symbol)
        if side == "BUY":
            if price == curr_price:
                raise ValueError("Stop order price must not be equal to current price.")
            elif price < curr_price:
                raise ValueError(
                    "Stop order price must not be less than current price."
                )
        else:
            if price == curr_price:
                raise ValueError("Stop order price must not be equal to current price.")
            elif price > curr_price:
                raise ValueError(
                    "Stop order price must not be greater than current price."
                )

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
        if side == "BUY":
            self.__create_order(
                position.symbol,
                "MKT",
                "SELL",
                units,
                price=None,
                position_id=position.id,
            )
        else:
            self.__create_order(
                position.symbol,
                "MKT",
                "BUY",
                units,
                price=None,
                position_id=position.id,
            )

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
                self._exec_price = (
                    "current" if self._exec_price == "next" else self._exec_price
                )
                close_all(positions)

    def __create_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        units: int = 100,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        position_id: int = 0,
    ) -> None:
        """
        Create an order event.

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
        """
        if self.acct_mode == "netting":
            self.__create_net_order(
                symbol, order_type, side, units, price, sl, tp, position_id
            )
        else:
            self.__create_hedge_order(
                symbol, order_type, side, units, price, sl, tp, position_id
            )
        self.__order_id += 1

    def __create_net_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        units: int = 100,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        position_id: int = 0,
    ) -> None:
        def split_order(
            order: OrderEvent, position: Position
        ) -> tuple[OrderEvent, OrderEvent]:
            order1 = deepcopy(order)
            order1.units = position.units
            order1.position_id = position.id
            order1.request = "close"

            order2 = order
            order2.units = order.units - position.units
            order2.request = "open"
            return order1, order2

        order = OrderEvent(
            self.data_handler.current_datetime,
            symbol,
            order_type=order_type,
            units=units,
            side=side,
            price=price,
            sl=sl,
            tp=tp,
            order_id=self.__order_id,
            position_id=position_id if position_id != 0 else self.__order_id,
        )

        pos = self.get_position(order.symbol)
        if pos:
            if order.order_id == order.position_id:  # Call from self.buy/self.sell
                if order.units > pos.units:  # type: ignore
                    order1, order2 = split_order(order, pos)  # type: ignore
                    self.__submit(order1)
                    self.__submit(order2)
                elif order.units <= pos.units:
                    order.position_id = pos.id
                    order.request = "close"
                    self.__submit(order)
            else:  # Call from self.close
                order.request = "close"
                self.__submit(order)
        else:
            order.request = "open"
            if order.is_bracket_order():
                sl_order, tp_order = self.__get_sl_tp_orders(order)
                self.__submit(order)
                self.__submit(sl_order)
                self.__submit(tp_order)
            else:
                self.__submit(order)

    def __create_hedge_order(
        self,
        symbol: str,
        order_type: str,
        side: str,
        units: int = 100,
        price: float | None = None,
        sl: float | None = None,
        tp: float | None = None,
        position_id: int = 0,
    ) -> None:
        order = OrderEvent(
            self.data_handler.current_datetime,
            symbol,
            order_type=order_type,
            units=units,
            price=price,
            side=side,
            order_id=self.__order_id,
            position_id=position_id if position_id != 0 else self.__order_id,
        )

        if order.order_id == position_id:  # Call from self.buy/self.sell
            order.request = "open"
            if order.is_bracket_order():
                sl_order, tp_order = self.__get_sl_tp_orders(order)
                self.__submit(order)
                self.__submit(sl_order)
                self.__submit(tp_order)
            else:
                self.__submit(order)
        else:  # Call from self.close
            order.request = "close"
        self.__submit(order)

    def __get_sl_tp_orders(self, order: OrderEvent):
        if order.side == "BUY":
            sl_order = OrderEvent(
                timestamp=order.timestamp,
                symbol=order.symbol,
                order_type="STP",
                units=order.units,
                side="SELL",
                price=order.sl,
                order_id=order.order_id,
                position_id=order.position_id,
            )
            sl_order.request = "close"

            tp_order = OrderEvent(
                timestamp=order.timestamp,
                symbol=order.symbol,
                order_type="LMT",
                units=order.units,
                side="SELL",
                price=order.tp,
                order_id=order.order_id,
                position_id=order.position_id,
            )
            tp_order.request = "close"
            return sl_order, tp_order
        else:
            sl_order = OrderEvent(
                timestamp=order.timestamp,
                symbol=order.symbol,
                order_type="STP",
                units=order.units,
                side="BUY",
                price=order.sl,
                order_id=order.order_id,
                position_id=order.position_id,
            )
            sl_order.request = "close"

            tp_order = OrderEvent(
                timestamp=order.timestamp,
                symbol=order.symbol,
                order_type="LMT",
                units=order.units,
                side="BUY",
                price=order.tp,
                order_id=order.order_id,
                position_id=order.position_id,
            )
            tp_order.request = "close"
            return sl_order, tp_order

    def __submit(self, order: OrderEvent):
        if order.order_type == "MKT":
            if self._exec_price == "current":
                self.execute_order(order)
            elif self._exec_price == "next":
                order.status = "PENDING"
                self.pending_orders.put(order)
        elif order.order_type == "LMT" or order.order_type == "STP":
            self.pending_orders.put(order)

    def execute_order(self, event: OrderEvent) -> None:
        """
        Convert Order objects into Fill objects naively,
        i.e., without any latency, slippage, or fill ratio problems.

        Parameters
        ----------
        event : OrderEvent
            Contains an Event object with order information.

        Raises
        ------
        TypeError
            If the provided event is not an OrderEvent.
        """
        if not isinstance(event, OrderEvent):
            raise TypeError(
                f"Expected an OrderEvent object. Got {type(event).__name__}"
            )

        if event.order_type == "MKT":
            if self._exec_price == "next":
                price = self.data_handler.get_latest_price(event.symbol, "open")
            else:
                price = self.data_handler.get_latest_price(event.symbol)
        elif event.order_type == "LMT" or event.order_type == "STP":
            price = event.price

        if event.request == "open":  # Order request type
            cost = self.__get_cost(event, price)
            if cost < self.free_margin:
                fill_event = FillEvent(
                    self.data_handler.current_datetime,
                    event.symbol,
                    event.units,
                    event.side,
                    price,
                    self.commission,
                    order_id=event.order_id,
                    position_id=event.position_id,
                )
                event.execute()
                self.order_history.append(event)
                self.update_account(fill_event)

                self.events.put(event)
                self.events.put(fill_event)
            else:
                event.reject()
                self.order_history.append(event)

        else:  # Close an existing position
            fill_event = FillEvent(
                self.data_handler.current_datetime,
                event.symbol,
                event.units,
                event.side,
                price,
                self.commission,
                "close",
                event.order_id,
                event.position_id,
            )
            event.execute()
            self.order_history.append(event)
            self.update_account(fill_event)

            self.events.put(event)
            self.events.put(fill_event)

    def execute_pending_orders(self) -> None:
        if not self.pending_orders.empty():
            n_pending = len(self.pending_orders.queue)
            for _ in range(n_pending):
                order = self.pending_orders.get(False)
                if order.order_type == "MKT":
                    self.execute_order(order)
                else:
                    bar = self.data_handler.get_latest_bars(order.symbol)[0]
                    if order.order_type == "LMT":
                        if order.side == "BUY":
                            if bar.low <= order.price:
                                self.execute_order(order)
                            else:  # Add order back to pending order queue
                                self.events.put(order)
                        else:
                            if bar.high >= order.price:
                                self.execute_order(order)
                            else:
                                self.events.put(order)
                    elif order.order_type == "STP":
                        if order.side == "BUY":
                            if bar.high >= order.price:
                                self.execute_order(order)
                            else:
                                self.events.put(order)
                        else:
                            if bar.low <= order.price:
                                self.execute_order(order)
                            else:
                                self.events.put(order)

    def __get_cost(self, event: OrderEvent, price) -> float:
        if self.acct_mode == "netting":
            pos = self.get_position(event.symbol)
            if pos and event.units > pos.units:  # type: ignore[union-attr]
                net_units = event.units - pos.units  # type: ignore[union-attr]
                return (net_units * price) / self.leverage
            return (event.units * price) / self.leverage
        else:
            return (event.units * price) / self.leverage

    def update_account(self, event: MarketEvent | FillEvent):
        """
        Update the account details based on market or fill events.

        Parameters
        ----------
        event
            The event to update the account from.
        """
        self.__update_positions(event)
        self.__update_fund_values(event)
        self.__update_account_history(event)
        self.__pos_hist_total = len(self.get_positions_history())
        if self.__margin_call():
            self._stop_simulation()

    def __update_positions(self, event: MarketEvent | FillEvent) -> None:
        """Update positions based on market or fill events."""
        if isinstance(event, MarketEvent):
            self.__update_positions_on_market()
        elif isinstance(event, FillEvent):
            self.__update_positions_on_fill(event)
            order = self.order_history[-1]
            if event.result == "open" and order.timestamp < event.timestamp:
                # Update PnL if a filled position was executed from pending order
                position = self.get_position(event.symbol)
                if isinstance(position, list):  # Hedging account
                    position[-1].update(
                        self.data_handler.get_latest_price(event.symbol)
                    )
                elif isinstance(position, Position):  # Netting account
                    position.update(self.data_handler.get_latest_price(event.symbol))

    def __update_positions_on_fill(self, event: FillEvent) -> None:
        """Add new positions to the porfolio"""
        self.p_manager.update_position_on_fill(event)

    def __update_positions_on_market(self) -> None:
        """Update portfolio holdings with the latest market price"""
        self.p_manager.update_position_on_market()

    def __update_fund_values(self, event: MarketEvent | FillEvent) -> None:
        if isinstance(event, MarketEvent):
            self.__update_equity()
            self.__update_free_margin()
        elif isinstance(event, FillEvent):
            self.__update_balance()
            self.__update_equity()
            self.__update_free_margin()

    def __update_balance(self) -> None:
        """Update the account balance based on closed position from a fill event."""
        if len(self.get_positions_history()) > self.__pos_hist_total:
            self.balance += self.p_manager.history[-1].pnl

    def __update_equity(self) -> None:
        """Update the account equity based on market or fill events."""
        self.equity = self.balance + self.p_manager.get_total_pnl()

    def __update_free_margin(self) -> None:
        """Update the free margin available for opening positions."""
        self.free_margin = self.equity - self.get_used_margin()

    def __update_account_history(self, event: MarketEvent | FillEvent) -> None:
        """Update the account history based on market or fill events."""
        timestamp = self.data_handler.current_datetime
        if isinstance(event, MarketEvent):
            self.account_history.append(
                {"timestamp": timestamp, "balance": self.balance, "equity": self.equity}
            )
        elif isinstance(event, FillEvent):
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
            if margin_level <= self.__stop_out_level:
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

    def get_position(self, symbol: str) -> Position | list[Position] | None:
        """
        Get the current position for a given symbol.

        Parameters
        ----------
        symbol
            The symbol to get the position for.

        Returns
        -------
        Position
            The current position for the symbol if account mode is Netting or list of
            open position for the symbol if account mode is Hedging.
        """
        return self.p_manager.get_position(symbol)

    def get_positions(self) -> dict[str | int, Position]:
        """
        Get all open positions.

        Returns
        -------
        dict
            The positions dictionary. The keys are the symbol names if account
            mode is netting or position ID if account mode is hedging.
        """
        return self.p_manager.positions

    def get_positions_history(self) -> list[Position]:
        """
        Get the history of all positions.

        Returns
        -------
        list
            The history of positions.
        """
        return self.p_manager.history

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

        order_history = [vars(order) for order in self.order_history]
        order_history = pd.DataFrame.from_records(order_history)
        return {
            "balance_equity": balance_equity,
            "positions": position_history,
            "orders": order_history,
        }
