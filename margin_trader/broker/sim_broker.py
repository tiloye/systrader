import pandas as pd
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
            data_source: DataHandler,
            events: Queue,
            leverage: int = 1
        ):
        self.balance = balance
        self.equity = balance
        self.free_margin = balance
        self.bar = data_source
        self.events = events
        self.portfolio = PostitionManager()

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

    def update_account_from_fill(self, fill):
        """
        Takes a FillEvent object and updates the account balance

        Parameters:
        fill - The FillEvent object to update the holdings with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1

        # TODO: Add account update logic

class PostitionManager:
    """Open and close positions based on filled orders."""
    
    def __init__(self):
        self.positions = {}
        self.postion_history = []

    def update_position(self):
        """Update position last price and PnL when from market event"""
        pass

    def update_position_from_fill(self, event: FillEvent):
        """Add a position for recently filled order."""
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
            self.positions[symbol].close_time = event.time_index
            self.postion_history.append(self.positions[symbol])
            del self.positions[event.symbol]
    
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
    def __init__(self, datetime, symbol, quantity, fill_price, commission, side):
        self.symbol = symbol
        self.quantity = quantity
        self.fill_price = fill_price
        self.last_price = fill_price
        self.commission = commission
        self.side = side
        self.fill_time = datetime
        self.pnl = self.update_pnl()

    def update_pnl(self):
        self.pnl = (self.last_price - self.fill_price) * self.quantity
        if self.side == "SELL":
            self.pnl = -1 * self.pnl

    def update_last_price(self, price: float):
        self.last_price = price

    def update(self, price: float):
        self.update_last_price(price)
        self.update_pnl()

    def __repr__(self):
        position = f"{self.side} | {self.quantity} | {self.last_price} | {self.pnl}"
        return position