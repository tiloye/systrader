import pandas as pd
from datetime import datetime
from queue import Queue
from margin_trader.broker import Broker
from margin_trader.data_source import DataHandler
from event import FillEvent, OrderEvent
from margin_trader.performance import create_sharpe_ratio, create_drawdowns

class SimulatedBroker(Broker):
    """
    Simulate live trading on a broker account.
    
    State:
        - Convert all order objects into their equivalent fill objects automatically
            without latency, slippage or fill-ratio issues.
        - Can only handle market order events
        - Execute all orders at close price
    """
    
    def __init__(
            self,
            balance: int|float,
            data_handler: DataHandler,
            events: Queue,
            leverage: int = 1
        ):
        self.balance = balance
        self.equity = balance
        self.free_margin = balance
        self.data_handler = data_handler
        self.events = events
        self.portfolio = PostitionManager()
        self._total_trades = 0

    def execute_order(self, event: OrderEvent):
        """
        Simply converts Order objects into Fill objects naively,
        i.e. without any latency, slippage or fill ratio problems.

        Parameters:
        event - Contains an Event object with order information.
        """
        if event.type == 'ORDER':
            fill_event = FillEvent(self.bar.current_datetime, event.symbol,
                                   event.quantity, event.direction,
                                   self.bar.get_latest_close_price(event.symbol))
            self.events.put(fill_event)
    
    def buy(self, symbol: str, order_type: str = "MKT", units: int|float = 100):
        """Buy x units of symbol."""
        self.create_order(symbol, order_type, "BUY", units)

    def sell(self, symbol: str, order_type: str = "MKT", units: int|float = 100):
        """Sell x units of symbol."""
        self.create_order(symbol, order_type, "SELL", units)

    def close(self, symbol: str, units: int|float = 100):
        """Close an existion position with an opposing order"""
        position = self.portfolio.positions.get(symbol, False)
        if position:
            side = position.side
            if side == "BUY":
                self.create_order(symbol, "MKT", "SELL", units)
            else:
                self.create_order(symbol, "MKT", "BUY", units)

    def create_order(self, symbol: str, order_type: str,
                    side: str, units: int|float = 100):
        """Create an order event"""
        order = OrderEvent(symbol, order_type=order_type,
                           quantity=units, direction=side)
        self.events.put(order)

    def update_porfolio_from_price(self):
        """Update portfolio holdings with the latest market price"""
        price = self.get_last_prices()
        for symbol in self.portfolio.positions:
            self.portfolio.update_pnl(symbol, price[symbol])

    def update_portfolio_from_fill(self, event: FillEvent):
        """Add new positions to the porfolio"""
        self.portfolio.update_position_from_fill(event)

    def update_account_from_fill(self, event: FillEvent):
        self.update_portfolio_from_fill(event)
        self.update_balance()
            
    def update_balance(self):
        # Check if a position has been closed
        if self._check_new_trade():
            self.balance += self.portfolio.history[-1].pnl
            # Gain or loss of cash implies trade
            self._total_trades = len(self.portfolio.history)

    def update_equity(self):
        total_pnl = self.portfolio.get_totat_pnl()
        self.equity += total_pnl

    def update_margin(self):
        pass
            
    def get_last_price(self):
        price = {
            symbol: self.data_handler.get_latest_close_price(symbol)
            for symbol in self.portfolio.positions
        }
        return price
    
    def _check_new_trade(self):
        """Check if a closed position has been added to the position history."""
        if len(self.portfolio.history) > self._total_trades:
            return True
        return False



class PostitionManager:
    """Open and close positions based on filled orders."""
    
    def __init__(self):
        self.positions = {}
        self.history = []

    def update_pnl(self, symbol: str, price: float):
        """Update position PnL when from market event"""
        self.positions[symbol].update(price)

    def update_position_from_fill(self, event: FillEvent):
        """Add/remove a position for recently filled order."""
        symbol = event.symbol
        if event.symbol not in self.positions:
            self.positions[symbol] = Position(
                datetime=event.timeindex,
                symbol=event.symbol,
                quantity=event.quantity,
                fill_price=event.fill_price,
                commission=event.commission,
                side=event.direction
            )
        else:
            self.positions[symbol].update(event.fill_price)
            self.positions[symbol].update_close_time = event.time_index
            self.history.append(self.positions[symbol])
            del self.positions[event.symbol]

    def get_totat_pnl(self):
        total_pnl = sum(self.postions[symbol].pnl for symbol in self.positions)
        return total_pnl
    
    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0+curve['returns']).cumprod()
        self.equity_curve = curve
    
    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio such
        as Sharpe Ratio and drawdown information.
        """
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)

        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                ("Drawdown Duration", "%d" % dd_duration)]
        return stats


class Position:
    def __init__(
            self,
            timeindex: str|datetime,
            symbol: str,
            units: int|float,
            fill_price: float,
            commission: float|None,
            side
        ):
        self.symbol = symbol
        self.units = units
        self.fill_price = fill_price
        self.last_price = fill_price
        self.commission = commission
        self.side = side
        self.fill_time = timeindex
        self.pnl = 0

    def update_pnl(self):
        self.pnl = (self.last_price - self.fill_price) * self.units
        if self.side == "SELL":
            self.pnl = -1 * self.pnl

    def update_last_price(self, price: float):
        self.last_price = price

    def update(self, price: float):
        self.update_last_price(price)
        self.update_pnl()
    
    def update_close_time(self, timeindex: str|datetime):
        self.close_time = timeindex

    def __repr__(self):
        position = f"{self.side} | {self.quantity} | {self.last_price} | {self.pnl}"
        return position