from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from typing import TYPE_CHECKING

from margin_trader.broker.order import OrderSide

if TYPE_CHECKING:
    from margin_trader.broker.fill import Fill
    from margin_trader.broker.sim_broker import SimBroker


class Position:
    """
    Represent a trading position.

    Parameters
    ----------
    timestamp : str or datetime
        The time when the position was filled.
    symbol : str
        The symbol of the traded asset.
    units : int or float
        The number of units in the position.
    fill_price : float
        The price at which the position was filled.
    commission : float or None
        The commission for the trade.
    side : OrderSide
        The side of the position, either "BUY" or "SELL".
    id_ : int
        The ID of the position. Equal to the ID of the order that initiated
        the position.

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
        timestamp: str | datetime,
        symbol: str,
        units: int,
        fill_price: float,
        commission: float,
        side: OrderSide,
        id_: int,
    ):
        self.symbol = symbol
        self.units = units
        self.fill_price = fill_price
        self.last_price = fill_price
        self.commission = commission
        self.side = side
        self.open_time = timestamp
        self.id = id_
        self.pnl = 0.0

    def update_pnl(self) -> None:
        """Update the PnL of the position."""
        pnl = (self.last_price - self.fill_price) * self.units
        if self.side == OrderSide.BUY:
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

    def update_close_time(self, timestamp: str | datetime) -> None:
        """
        Update the close time of the position.

        Parameters
        ----------
        timestamp
            The time when the position was closed.
        """
        self.close_time = timestamp

    def get_cost(self) -> float:
        """
        Get the cost of the position.

        Returns
        -------
        float
            The cost of the position.
        """
        return self.fill_price * self.units

    def increase_size(self, price: float, units: int) -> None:
        """Add more units to position."""
        prev_price = self.fill_price
        prev_units = self.units
        avg_price = (prev_units * prev_price + units * price) / (prev_units + units)
        self.fill_price = avg_price
        self.units = prev_units + units
        self.update(price)

    def reduce_size(self, units: int) -> None:
        self.units = self.units - units

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

    def __eq__(self, other) -> bool:
        return self.id == other.id


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

    def __init__(self, broker: SimBroker) -> None:
        self.broker = broker
        self.positions: dict[str | int, Position] = {}
        self.history: list[Position] = []

    def update_position_on_market(self) -> None:
        """
        Update position PnL from market event.

        Parameters
        ----------
        symbol
            The symbol of the position to update.
        price
            The latest market price.
        """
        for position in self.positions.values():
            position.update(self.broker.data_handler.get_latest_price(position.symbol))

    def update_position_on_fill(self, event: Fill) -> None:
        """
        Add/remove a position based on recently filled order.

        Parameters
        ----------
        event
            The fill event to update positions from.
        """
        if event.result == "open":
            self._open_position(event)
        else:
            self._close_position(event)

    def _open_position(self, event: Fill) -> None:
        raise NotImplementedError("Implement position opening logic in a subclass.")

    def _close_position(self, event: Fill) -> None:
        raise NotImplementedError("Implement position closing logic in a subclass.")

    def _close_partial_position(self, position: Position, event: Fill) -> None:
        partial_position = deepcopy(position)
        partial_position.units = event.units
        position.reduce_size(event.units)
        position.update(event.fill_price)
        self._add_to_history(partial_position, event)

    def _add_to_history(self, position: Position, event: Fill) -> None:
        position.commission += event.commission
        position.update(event.fill_price)
        position.update_close_time(event.timestamp)
        self.history.append(position)

    def get_total_pnl(self) -> float:
        """
        Get the total PnL of all open positions.

        Returns
        -------
        int
            The total PnL of all open positions.
        """
        total_pnl = sum(pos.pnl for pos in self.positions.values())
        return total_pnl

    def reset(self):
        self.positions = {}
        self.history = []


class NetPositionManager(PositionManager):
    """
    Represents a net position management system. It stores open positions as key value
    pairs of symbol name and Position object.
    """

    def _open_position(self, event: Fill) -> None:
        position = self.positions.get(event.symbol)
        if position:
            if position.side == event.side:  # New order is in the same direction
                position.increase_size(event.fill_price, event.units)
            else:
                self._close_position(event)
        else:
            self.positions[event.symbol] = Position(
                timestamp=event.timestamp,
                symbol=event.symbol,
                units=event.units,
                fill_price=event.fill_price,
                commission=event.commission,
                side=event.side,
                id_=event.order_id,
            )

    def _close_position(self, event: Fill) -> None:
        position = self.positions[event.symbol]
        if event.units < position.units:
            self._close_partial_position(position, event)
        else:
            self._add_to_history(position, event)
            del self.positions[event.symbol]

    def get_position(self, symbol: str) -> Position | None:
        return self.positions.get(symbol)


class HedgePositionManager(PositionManager):
    """
    Represents a hegde postion managment system. It store open positions as key value
    pairs of position ID and the Position object
    """

    def __init__(self, broker: SimBroker) -> None:
        super().__init__(broker)
        self.position_grp = defaultdict(list)

    def _open_position(self, event: Fill) -> None:
        position = Position(
            timestamp=event.timestamp,
            symbol=event.symbol,
            units=event.units,
            fill_price=event.fill_price,
            commission=event.commission,
            side=event.side,
            id_=event.order_id,
        )
        self.positions[position.id] = position
        self.position_grp[position.symbol].append(position.id)

    def _close_position(self, event: Fill) -> None:
        position = self.positions[event.position_id]
        if event.units < position.units:
            self._close_partial_position(position, event)
        else:
            self._add_to_history(position, event)
            del self.positions[event.position_id]
            self.position_grp[event.symbol].remove(event.position_id)
            if self.position_grp[event.symbol] == []:
                del self.position_grp[event.symbol]

    def get_position(self, identifier: str | int) -> Position | list[Position] | None:
        if isinstance(identifier, int):
            return self.positions.get(identifier)
        elif isinstance(identifier, str):
            pos_ids = self.position_grp.get(identifier)
            if pos_ids:
                return [self.positions[i] for i in pos_ids]
            return None
