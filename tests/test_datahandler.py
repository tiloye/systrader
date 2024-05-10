import unittest
import csv
import os
from pathlib import Path
from queue import Queue
from margin_trader.data_source import HistoricCSVDataHandler

CSV_DIR = Path(__file__).parent


class TestHistoricalCSVDataHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = [
            ["2024-05-03", 100.0, 105.0, 98.0, 102.0, 0],
            ["2024-05-04", 102.0, 108.0, 100.0, 106.0, 0],
            ["2024-05-07", 106.0, 110.0, 104.0, 108.0, 0],
            ["2024-05-08", 108.0, 112.0, 106.0, 110.0, 0],
            ["2024-05-09", 110.0, 115.0, 108.0, 112.0, 0]
        ]

        with open(CSV_DIR/"AAPL.csv", "w") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                ["date", "open", "high", "low", "close", "volume"]
            )
            for day in data:
                writer.writerow(day)

    def setUp(self):
        self.event_queue = Queue()
        self.symbols = ["AAPL"]
        self.bars = HistoricCSVDataHandler(
            events = self.event_queue,
            csv_dir = CSV_DIR,
            symbol_list = self.symbols
        )
    
    def test_initialisation(self):
        self.assertEqual(CSV_DIR, self.bars.csv_dir)
        self.assertEqual(len(self.bars.latest_symbol_data), len(self.bars.symbol_list))
        self.assertEqual(len(self.bars.symbol_list), len(self.bars.symbol_data))
        self.assertEqual(self.bars.current_datetime, None)

    def test_empty_latest_bars(self):
        symbol = self.bars.symbol_list[0]
        no_bar = self.bars.get_latest_bars(symbol)
        self.assertEqual(no_bar, [])

    def test_update_bars(self):
        for i in range(5):
            self.bars.update_bars()
        self.assertEqual(len(self.bars.events.queue), 5)
        self.assertEqual(self.bars.current_datetime.strftime("%Y-%m-%d"), "2024-05-09")

    def test_latest_bars(self):
        for i in range(5):
            self.bars.update_bars()
        symbol = self.bars.symbol_list[0]
        last_5_bars = self.bars.get_latest_bars(symbol=symbol, N=5)
        self.assertEqual(len(last_5_bars), 5)
    
    def test_get_latest_price(self):
        self.bars.update_bars()
        ohlc = ["open", "high", "low", "close"]
        symbol = self.bars.symbol_list[0]
        bar = [
            self.bars.get_latest_price(symbol, price=price) for price in ohlc
        ]
        self.assertListEqual(bar, [100.0, 105.0, 98.0, 102.0])

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(CSV_DIR/"AAPL.csv"):
            os.remove(CSV_DIR/"AAPL.csv")


if __name__ == "__main__":
    unittest.main()