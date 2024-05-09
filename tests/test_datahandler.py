import unittest
import csv
import os
from pathlib import Path
from queue import Queue
from margin_trader.data_source import HistoricCSVDataHandler

event_queue = Queue()
CSV_DIR = Path(__file__).parent
symbols = ["AAPL"]


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

        cls.bars = HistoricCSVDataHandler(
            events = event_queue,
            csv_dir = CSV_DIR,
            symbol_list = symbols
        )
    
    def test_initialisation(self):
        self.assertEqual(CSV_DIR, self.bars.csv_dir)
        self.assertEqual(len(self.bars.latest_symbol_data), len(self.bars.symbol_list))
        self.assertEqual(len(self.bars.symbol_list), len(self.bars.symbol_data))
        self.assertIsInstance(self.bars.events, Queue)
        self.assertIsInstance(self.bars.symbol_list, list)
        self.assertIsInstance(self.bars.symbol_data, dict)
        self.assertIsInstance(self.bars.latest_symbol_data, dict)

    def test_empty_latest_bars(self):
        symbol = self.bars.symbol_list[0]
        no_bar = self.bars.get_latest_bars(symbol)
        self.assertEqual(no_bar, [])

    def test_update_bars(self):
        symbol = self.bars.symbol_list[0]
        self.bars.update_bars()
        latest_bars = self.bars.get_latest_bars(symbol)

        self.assertEqual(len(latest_bars), 1)

        for i in range(4):
            self.bars.update_bars()

        latest_bars = self.bars.get_latest_bars(symbol=symbol, N=5)
        self.assertEqual(len(latest_bars), 5)
        self.assertEqual(len(self.bars.events.queue), 5)

    @classmethod
    def tearDownClass(cls):
        if os.path.exists(CSV_DIR/"AAPL.csv"):
            os.remove(CSV_DIR/"AAPL.csv")


if __name__ == "__main__":
    unittest.main()