import datetime
import pandas as pd
from base import ExecutionHandler
from event import FillEvent, OrderEvent
from margin_trader.performance import create_sharpe_ratio, create_drawdowns

class SimulatedExecutionHandler(ExecutionHandler):
    """
    The simulated execution handler simply converts all order
    objects into their equivalent fill objects automatically
    without latency, slippage or fill-ratio issues.

    This allows a straightforward "first go" test of any strategy,
    before implementation with a more sophisticated execution
    handler.
    """
    
    def __init__(self, events):
        """
        Initialises the handler, setting the event queues
        up internally.

        Parameters:
        events - The Queue of Event objects.
        """
        self.events = events

    def execute_order(self, event):
        """
        Simply converts Order objects into Fill objects naively,
        i.e. without any latency, slippage or fill ratio problems.

        Parameters:
        event - Contains an Event object with order information.
        """
        if event.type == 'ORDER':
            fill_event = FillEvent(datetime.datetime.utcnow(), event.symbol,
                                   'ARCA', event.quantity, event.direction, None)
            self.events.put(fill_event)

class PostitionManager:
    """
    The NaivePortfolio object is designed to send orders to
    a brokerage object with a constant quantity size blindly,
    i.e. without any risk management or position sizing. It is
    used to test simpler strategies such as BuyAndHoldStrategy.
    """
    
    def __init__(self, bars, events, start_date, initial_capital=100000.0):
        """
        Initialises the portfolio with bars and an event queue. 
        Also includes a starting datetime index and initial capital 
        (USD unless otherwise stated).

        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """
        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital
        
        # self.all_positions = self.construct_all_positions()
        # self.current_positions = {symbol: 0 for symbol in sestraderlf.symbol_list}

        # self.all_holdings = self.construct_all_holdings()
        # self.current_holdings = self.construct_current_holdings()

        self.positions = {}
        self.postion_history = []

    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current 
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OLHCVI).

        Makes use of a MarketEvent from the events queue.
        """
        current_datetime = self.bars.current_datetime

        # Update positions
        positions = {symbol: self.current_positions[symbol] for symbol in self.symbol_list}
        positions['datetime'] = current_datetime

        # Append the current positions
        self.all_positions.append(positions)

        # Update holdings
        holdings = {symbol: 0.0 for symbol in self.symbol_list}
        holdings["datetime"] = current_datetime
        holdings["cash"] = self.current_holdings["cash"]
        holdings["commission"] = self.current_holdings["commission"]
        holdings["total"] = self.current_holdings["cash"]

        for symbol in self.symbol_list:
            # Approximation to the real value
            close_price = self.bars.get_latest_close_price(symbol)
            market_value = self.current_positions[symbol] * close_price
            holdings[symbol] = market_value
            holdings['total'] += market_value

        # Append the current holdings
        self.all_holdings.append(holdings)

    def update_positions_from_fill(self, fill):
        """
        Takes a FilltEvent object and updates the position matrix
        to reflect the new position.

        Parameters:
        fill - The FillEvent object to update the positions with.
        """
        if fill.symbol not in self.positions:
            self.positions[fill.symbol] = Position(
                datetime=fill.timeindex,
                symbol=fill.symbol,
                quantity=fill.quantity,
                fill_price=fill.fill_cost,
                commission=fill.commission,
                side=fill.direction
            )

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

    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings 
        from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            # self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal, order_type = "MKT"):
        """
        Simply transacts an OrderEvent object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.

        Parameters:
        signal - The SignalEvent signal information.
        """
        order = None

        symbol = signal.symbol
        direction = signal.signal_type

        mkt_quantity = 100
        cur_quantity = self.current_positions[symbol]

        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY')
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'SELL')   
    
        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL')
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'BUY')
        return order
    
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders 
        based on the portfolio logic.
        """
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)
    
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
    """Manage open and closed positions"""

    def __init__(self, datetime, symbol, quantity, fill_price, commission, side):
        self.symbol = symbol
        self.quantity = quantity
        self.fill_price = fill_price
        self.last_price = fill_price
        self.commission = commission
        self.side = side
        self.fill_time = datetime
        self.pnl = 0

    def update_pnl(self):
        self.pnl = (self.last_price - self.fill_price) * self.quantity

    def __repr__(self):
        position = f"{self.side} | {self.quantity} | {self.last_price} | {self.value}"
        return position