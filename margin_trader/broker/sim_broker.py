import pandas as pd
from datetime import datetime
from queue import Queue
from margin_trader.broker import Broker
from margin_trader.data_source import DataHandler
from margin_trader.event import FillEvent, OrderEvent, MarketEvent

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
    leverage : int, optional
        The leverage for trading. Default is 1.
    commission : int or float, optional
        The commission for each trade. Default is None.
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
        A list of dictionaries storing historical account data
        (timeindex, balance, equity).
    """
    
    def __init__(
            self,
            data_handler: DataHandler,
            balance: int|float = 100_000,
            leverage: int = 1,
            commission: None|int|float = None,
            exec_price = "current"
        ):
        self.balance = balance
        self.equity = balance
        self.free_margin = balance
        self.data_handler = data_handler
        self.leverage = leverage
        self.commission = commission
        self.p_manager = PositionManager()
        self._exec_price = exec_price
        self.pending_orders = Queue()
        self.account_history = [
            {
                "timeindex": self.data_handler.start_date,
                "balance": self.balance,
                "equity": self.equity
            }
        ]

    def _add_event_queue(self, event_queue):
        self.events = event_queue

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
            raise TypeError("Expected an order event object")
        
        order = self.__check_order(event)
        if event.status == "PENDING":
            if event.order_type == "MKT":
                price = self.data_handler.get_latest_price(event.symbol, "open")
        else:
            price = self.data_handler.get_latest_price(event.symbol)
        
        if order == "OPEN":
            cost = (event.units * price) / self.leverage
            if cost < self.free_margin:
                fill_event = FillEvent(
                    self.data_handler.current_datetime,
                    event.symbol,
                    event.units,
                    event.side,
                    price,
                    self.commission
                )
                self.events.put(fill_event)
            else:
                print("Order rejected; not enough margin")
        else:
            fill_event = FillEvent(
                    self.data_handler.current_datetime,
                    event.symbol,
                    event.units,
                    event.side,
                    price,
                    self.commission,
                    "close"
            )
            self.events.put(fill_event)

    def __check_order(self, event: OrderEvent):
        """
        Check if an order event is to open or close a position.

        Parameters
        ----------
        event
            The order event to check.

        Returns
        -------
        str
            "OPEN" if opening a position, "CLOSE" if closing a position.
        """

        symbol = event.symbol
        side = event.side
        position = self.p_manager.positions.get(symbol, False)
        if position:
            if position.side != side:
                return "CLOSE"
        return "OPEN"
            
    
    def buy(self, symbol: str,
            order_type: str = "MKT", units: int|float = 100) -> None:
        """
        Buy x units of symbol.

        Parameters
        ----------
        symbol
            The symbol to buy.
        order_type
            The type of order, default is "MKT".
        units
            The number of units to buy, default is 100.
        """
        self.__create_order(symbol, order_type, "BUY", units)

    def sell(self, symbol: str,
             order_type: str = "MKT", units: int|float = 100) -> None:
        """
        Sell x units of symbol.

        Parameters
        ----------
        symbol
            The symbol to sell.
        order_type
            The type of order, default is "MKT".
        units
            The number of units to sell, default is 100.
        """
        self.__create_order(symbol, order_type, "SELL", units)

    def close(self, symbol: str, units: int|float = 100) -> None:
        """
        Close an existing position with an opposing order.

        Parameters
        ----------
        symbol
            The symbol of the position to close.
        units
            The number of units to close, default is 100.
        """
        position = self.p_manager.positions.get(symbol, False)
        if position:
            side = position.side
            if side == "BUY":
                self.sell(symbol, units=units)
            else:
                self.buy(symbol, units=units)
        else:
            print(f"There is no open position for {symbol}")

    def __create_order(self, symbol: str, order_type: str,
                    side: str, units: int|float = 100) -> None:
        """
        Create an order event.

        Parameters
        ----------
        symbol : str
            The symbol for the order.
        order_type : str
            The type of the order.
        side : str
            The side of the order, either "BUY" or "SELL".
        units : int or float, optional
            The number of units, default is 100.
        """
        order = OrderEvent(symbol, order_type=order_type,
                           units=units, side=side)
        if order.order_type == "MKT":
            if self._exec_price == "current":
                self.events.put(order)
            elif self._exec_price == "next":
                order.status = "PENDING"
                self.pending_orders.put(order)
    
    def check_pending_orders(self):
        """Check if there are pending orders and them to the event queue."""
        if not self.pending_orders.empty():
            n_pending = len(self.pending_orders.queue)
            for i in range(n_pending):
                order = self.pending_orders.get(False)
                if order.order_type == "MKT":
                    self.events.put(order)
                elif order.order_type == "LMT":
                    # TODO: Add limit orders to event queue if price has been tagged
                    pass

    def update_account(self, event: MarketEvent|FillEvent):
        """
        Update the account details based on market or fill events.

        Parameters
        ----------
        event
            The event to update the account from.
        """
        self.__update_positions(event)
        self.__update_balance(event)
        self.__update_equity(event)
        self.__update_free_margin()
        self.__update_account_history(event)

    def __update_positions(self, event: MarketEvent|FillEvent):
        """Update positions based on market or fill events."""
        if event.type == "MARKET":
            self.__update_positions_from_price()
        elif event.type == "FILL":
            self.__update_positions_from_fill(event)

    def __update_positions_from_fill(self, event: FillEvent) -> None:
        """Add new positions to the porfolio"""
        self.p_manager.update_position_from_fill(event)

    def __update_positions_from_price(self) -> None:
        """Update portfolio holdings with the latest market price"""
        for symbol in self.get_positions():
            self.p_manager.update_pnl(
                symbol,
                self.data_handler.get_latest_price(symbol)
            )
            
    def __update_balance(self, event: FillEvent) -> None:
        """Update the account balance based on closed position."""
        # Check if a position has been closed
        if event.type == "FILL" and event.result == "close":
            self.balance += self.p_manager.history[-1].pnl

    def __update_equity(self, event: MarketEvent|FillEvent) -> None:
        """Update the account equity based on market or fill events."""
        if event.type == "MARKET":
            self.equity += self.p_manager.get_total_pnl()
        elif event.type == "FILL":
            if event.result == "close":
                self.equity = self.balance + self.p_manager.get_total_pnl()

    def __update_free_margin(self) -> None:
        """Update the free margin available for trading."""
        self.free_margin = self.equity - self.get_used_margin()

    def __update_account_history(self, event):
        """Update the account history based on market or fill events."""
        timeindex = self.data_handler.current_datetime
        if event.type == "MARKET":
            self.account_history.append(
                {"timeindex": timeindex, "balance": self.balance, "equity": self.equity}
            )
        elif event.type == "FILL" and event.result == "close":
            recent_history = self.account_history[-1]
            if timeindex == recent_history["timeindex"]:
                recent_history["balance"] = self.balance
                recent_history["equity"] = self.equity

    def get_used_margin(self) -> float:
        """
        Get the current used margin.

        Returns
        -------
        float
            The current used margin.
        """
        symbols = self.p_manager.positions.keys()
        margin = sum(self.p_manager.positions[symbol].get_cost() for symbol in symbols)
        margin = margin/self.leverage
        return margin
    
    def get_position(self, symbol: str):
        """
        Get the current position for a given symbol.

        Parameters
        ----------
        symbol
            The symbol to get the position for.

        Returns
        -------
        Position or bool
            The current position for the symbol or False if no position exists.
        """
        return self.p_manager.positions.get(symbol, False)

    def get_positions(self):
        """
        Get all current positions.

        Returns
        -------
        dict
            The current positions.
        """
        return self.p_manager.positions
    
    def get_positions_history(self):
        """
        Get the history of all positions.

        Returns
        -------
        list
            The history of positions.
        """
        return self.p_manager.history
    
    def get_account_history(self):
        """
        Get the account balance and equity history.

        Returns
        -------
        dict
            A dictionary containing the balance and equity history
            and the positions history.
        """
        balance_equity = pd.DataFrame.from_records(self.account_history)
        position_history = [vars(position) for position in self.get_positions_history()]
        position_history = pd.DataFrame.from_records(position_history)
        position_history.rename(
            columns={"fill_price": "open_price", "last_price":"close_price"},
            inplace=True
        )
        return {"balance_equity": balance_equity, "positions": position_history}


class PositionManager:
    """
    Keep track of open and closed positions based on filled orders.

    Attributes
    ----------
    positions : dict
        A dictionary of current open positions.
    history : list
        A list of closed positions.
    """
    
    def __init__(self):
        self.positions = {}
        self.history = []

    def update_pnl(self, symbol: str, price: float) -> None:
        """
        Update position PnL from market event.

        Parameters
        ----------
        symbol
            The symbol of the position to update.
        price
            The latest market price.
        """
        self.positions[symbol].update(price)

    def update_position_from_fill(self, event: FillEvent) -> None:
        """
        Add/remove a position based on recently filled order.

        Parameters
        ----------
        event
            The fill event to update positions from.
        """
        if event.result == "open":
            self.__open_position(event)
        else:
            self.__close_position(event)
            
    def __open_position(self, event: FillEvent) -> None:
        """
        Open a new position from a fill event.

        Parameters
        ----------
        event
            The fill event to open a position from.
        """
        self.positions[event.symbol] = Position(
                timeindex=event.timeindex,
                symbol=event.symbol,
                units=event.units,
                fill_price=event.fill_price,
                commission=event.commission,
                side=event.side
        )
        
    def __close_position(self, event: FillEvent) -> None:
        """
        Close an existing position from a fill event.

        Parameters
        ----------
        event
            The fill event to close a position from.
        """
        self.positions[event.symbol].commission += event.commission # openNclose fee
        self.positions[event.symbol].update(event.fill_price)
        self.positions[event.symbol].update_close_time(event.timeindex)
        self.history.append(self.positions[event.symbol])
        del self.positions[event.symbol]

    def get_total_pnl(self) -> int:
        """
        Get the total PnL of all open positions.

        Returns
        -------
        int
            The total PnL of all open positions.
        """
        total_pnl = sum(self.positions[symbol].pnl for symbol in self.positions)
        return total_pnl
    

class Position:
    """
    Represent a trading position.

    Parameters
    ----------
    timeindex : str or datetime
        The time when the position was filled.
    symbol : str
        The symbol of the traded asset.
    units : int or float
        The number of units in the position.
    fill_price : float
        The price at which the position was filled.
    commission : float or None
        The commission for the trade.
    side : str
        The side of the position, either "BUY" or "SELL".

    Attributes
    ----------
    symbol : str
        The symbol of the traded asset.
    units : int or float
        The number of units in the position.
    fill_price : float
        The price at which the position was filled.
    last_price : float
        The last market price of the symbol.
    commission : float
        The commission for the trade.
    side : str
        The side of the position, either "BUY" or "SELL".
    open_time : str or datetime
        The time when the position was opened.
    pnl : float
        The profit and loss of the position.
    close_time : str or datetime
        The time when the position was closed.
    """

    def __init__(
            self,
            timeindex: str|datetime,
            symbol: str,
            units: int|float,
            fill_price: float,
            commission: float|None,
            side: str
        ):
        self.symbol = symbol
        self.units = units
        self.fill_price = fill_price
        self.last_price = fill_price
        self.commission = commission
        self.side = side
        self.open_time = timeindex
        self.pnl = 0

    def update_pnl(self) -> None:
        """Update the PnL of the position."""
        pnl = (self.last_price - self.fill_price) * self.units
        if self.side == "BUY":
            self.pnl = pnl - self.commission
        else:
            self.pnl = -1 * pnl - self.commission

    def update_last_price(self, price: float) -> None:
        """
        Update the last market price.

        Parameters
        ----------
        price
            The latest market price.
        """
        self.last_price = price

    def update(self, price: float) -> None:
        """
        Update the position last price and PnL.

        Parameters
        ----------
        price : float
            The latest market price.
        """
        self.update_last_price(price)
        self.update_pnl()
    
    def update_close_time(self, timeindex: str|datetime) -> None:
        """
        Update the close time of the position.

        Parameters
        ----------
        timeindex
            The time when the position was closed.
        """
        self.close_time = timeindex

    def get_cost(self) -> float:
        """
        Get the cost of the position.

        Returns
        -------
        float
            The cost of the position.
        """
        return self.fill_price * self.units

    def __repr__(self) -> str:
        """
        Return a string representation of the position.

        Returns
        -------
        str
            A string representation of the position.
        """
        position = f"{self.symbol}|{self.side}|{self.units}|{self.pnl}"
        return position
    