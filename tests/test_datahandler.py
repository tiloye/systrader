import unittest
from pathlib import Path
from queue import Queue
from margin_trader.data_source.data_handler import HistoricCSVDataHandler

event_queue = Queue()
CSV_DIR = Path(__file__).parent.joinpath("data")
symbols = ["AAPL"]

class TestHistoricalCSVDataHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bars = HistoricCSVDataHandler(
            events = event_queue,
            csv_dir = CSV_DIR,
            symbol_list = symbols
        )
    
    def test_initialisation(self):
        self.assertEqual(CSV_DIR, self.bars.csv_dir)
        self.assertEqual(len(self.bars.latest_symbol_data), len(self.bars.symbol_list))
        self.assertIsInstance(self.bars.events, Queue)
        self.assertIsInstance(self.bars.symbol_list, list)
        self.assertIsInstance(self.bars.symbol_data, dict)
        self.assertIsInstance(self.bars.latest_symbol_data, dict)
    
    def test_open_convert_csv(self):
        self.assertEqual(len(self.bars.symbol_list), len(self.bars.symbol_data))

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



if __name__ == "__main__":
    unittest.main()