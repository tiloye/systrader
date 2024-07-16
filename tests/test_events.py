import unittest
from contextlib import redirect_stdout
from datetime import datetime
from io import StringIO

from margin_trader.event import FillEvent, MarketEvent, OrderEvent


class TestMarketEvent(unittest.TestCase):
    def test_market_event_initialization(self):
        market_event = MarketEvent()
        self.assertEqual(market_event.type, "MARKET")


class TestOrderEvent(unittest.TestCase):
    def setUp(self):
        self.timestamp = datetime(2023, 1, 1, 12, 0, 0)
        self.symbol = "GOOG"
        self.order_type = "MKT"
        self.units = 100
        self.side = "BUY"
        self.order_event = OrderEvent(
            self.timestamp, self.symbol, self.order_type, self.units, self.side
        )

    def test_order_event_initialization(self):
        self.assertEqual(self.order_event.type, "ORDER")
        self.assertEqual(self.order_event.timestamp, self.timestamp)
        self.assertEqual(self.order_event.symbol, self.symbol)
        self.assertEqual(self.order_event.order_type, self.order_type)
        self.assertEqual(self.order_event.units, self.units)
        self.assertEqual(self.order_event.side, self.side)
        self.assertEqual(self.order_event.status, "PENDING")
        self.assertEqual(self.order_event.order_id, 0)
        self.assertEqual(self.order_event.position_id, 0)

    def test_execute(self):
        self.order_event.execute()
        self.assertEqual(self.order_event.status, "EXECUTED")

    def test_reject(self):
        self.order_event.reject()
        self.assertEqual(self.order_event.status, "REJECTED")

    def test_print_order(self):
        timestamp = datetime(2023, 1, 1, 12, 0, 0)
        symbol = "GOOG"
        order_type = "MKT"
        units = 100
        side = "BUY"
        order_event = OrderEvent(timestamp, symbol, order_type, units, side)
        expected_output = "Order: Symbol=GOOG, Type=MKT, units=100, Direction=BUY\n"
        with StringIO() as out, redirect_stdout(out):
            order_event.print_order()
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
        fill_event = FillEvent(timestamp, symbol, units, side, fill_price)
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
