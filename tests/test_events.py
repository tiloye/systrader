import unittest
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO

from margin_trader.constants import OrderSide, OrderStatus, OrderType
from margin_trader.event import Fill, Market, Order


class TestMarketEvent(unittest.TestCase):
    def test_market_event_initialization(self):
        market_event = Market()
        self.assertEqual(market_event.type, "MARKET")


SYMBOL = "GOOG"


class TestOrder(unittest.TestCase):
    def test_execute_buy(self):
        for order_type, price in zip(
            [OrderType.MARKET, OrderType.LIMIT, OrderType.STOP], [None, 100.0, 105.0]
        ):
            with self.subTest(order_type=order_type, price=price):
                order = Order(
                    timestamp=datetime(2023, 1, 1, 12, 0, 0),
                    symbol=SYMBOL,
                    order_type=order_type,
                    units=100,
                    side=OrderSide.BUY,
                    price=price,
                )
                order.execute()
                self.assertEqual(order.status, OrderStatus.EXECUTED)

    def test_execute_sell(self):
        for order_type, price in zip(
            [OrderType.MARKET, OrderType.LIMIT, OrderType.STOP], [None, 105.0, 100.0]
        ):
            with self.subTest(order_type=order_type, price=price):
                order = Order(
                    timestamp=datetime(2023, 1, 1, 12, 0, 0),
                    symbol=SYMBOL,
                    order_type=order_type,
                    units=100,
                    side=OrderSide.SELL,
                    price=price,
                )
                order.execute()
                self.assertEqual(order.status, OrderStatus.EXECUTED)

    def test_reject_buy(self):
        for order_type, price in zip(OrderType, [None, 100.0, 105.0]):
            with self.subTest(order_type=order_type, price=price):
                order = Order(
                    timestamp=datetime(2023, 1, 1, 12, 0, 0),
                    symbol=SYMBOL,
                    order_type=order_type,
                    units=100,
                    side=OrderSide.BUY,
                    price=price,
                )
                order.reject()
                self.assertEqual(order.status, OrderStatus.REJECTED)

    def test_reject_sell(self):
        for order_type, price in zip(OrderType, [None, 105.0, 100.0]):
            with self.subTest(order_type=order_type, price=price):
                order = Order(
                    timestamp=datetime(2023, 1, 1, 12, 0, 0),
                    symbol=SYMBOL,
                    order_type=order_type,
                    units=100,
                    side=OrderSide.SELL,
                    price=price,
                )
                order.reject()
                self.assertEqual(order.status, OrderStatus.REJECTED)

    def test_print_order(self):
        order = Order(
            timestamp=datetime(2023, 1, 1, 12, 0, 0),
            symbol=SYMBOL,
            order_type=OrderType.MARKET,
            units=100,
            side=OrderSide.BUY,
        )
        expected_output = (
            f"Order: Symbol={SYMBOL}, Type=mkt, units=100, Direction=buy\n"
        )
        with StringIO() as out, redirect_stdout(out):
            order.print_order()
            output = out.getvalue()
            self.assertEqual(expected_output, output)


class TestFillEvent(unittest.TestCase):
    def test_fill_event_initialization(self):
        timestamp = datetime(2023, 1, 1, 12, 0, 0)
        symbol = "GOOG"
        units = 100
        side = "BUY"
        fill_price = 1500.0
        commission = 0.0
        fill_event = Fill(timestamp, symbol, units, side, fill_price)
        self.assertEqual(fill_event.type, "FILL")
        self.assertEqual(fill_event.timestamp, timestamp)
        self.assertEqual(fill_event.symbol, symbol)
        self.assertEqual(fill_event.units, units)
        self.assertEqual(fill_event.side, side)
        self.assertEqual(fill_event.fill_price, fill_price)
        self.assertEqual(fill_event.commission, commission)
        self.assertEqual(fill_event.result, "open")
        self.assertEqual(fill_event.order_id, 0)
        self.assertEqual(fill_event.position_id, 0)


if __name__ == "__main__":
    unittest.main()
