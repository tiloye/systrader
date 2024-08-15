import unittest
from datetime import datetime

from margin_trader.broker.fill import Fill


class TestFill(unittest.TestCase):
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
