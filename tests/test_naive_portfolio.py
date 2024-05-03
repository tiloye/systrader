import unittest
from pathlib import Path
from queue import Queue
from margin_trader.data_source.csv_data_handler import HistoricCSVDataHandler
from margin_trader.broker import NaivePortfolio

event_queue = Queue()
CSV_DIR = Path(__file__).parent.joinpath("data")
symbols = ["AAPL"]

class TestNaivePortfolio(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bars = HistoricCSVDataHandler(
            events = event_queue,
            csv_dir = CSV_DIR,
            symbol_list = symbols
        )

        cls.portfolio = NaivePortfolio(
            bars = cls.bars,
            events = event_queue,
            start_date = cls.bars.start_date
        )

    def test_initialisation(self):
        self.assertEqual(self.bars.start_date, self.portfolio.start_date)
        self.assertIsInstance(self.portfolio.events, Queue)
        self.assertIsInstance(self.portfolio.all_positions, list)
        self.assertIsInstance(self.portfolio.all_positions[0], dict)
        self.assertIsInstance(self.portfolio.current_positions, dict)
        self.assertIsInstance(self.portfolio.all_holdings, list)
        self.assertIsInstance(self.portfolio.all_holdings[0], dict)
        self.assertIsInstance(self.portfolio.current_holdings, dict)

if __name__ == "__main__":
    unittest.main()