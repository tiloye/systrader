import unittest
from queue import Queue
from margin_trader.data_source import YahooDataHandler


class TestYahooDataHandler(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.event_queue = Queue()
        cls.symbols = "AAPL"
        cls.bars = YahooDataHandler(
            symbols=cls.symbols,
            start_date="2024-05-06",
            end_date="2024-05-11",
            use_cols="volume",
        )
        cls.bars._add_event_queue(cls.event_queue)

    def test_initialisation(self):
        self.assertIsInstance(self.bars.symbols, list)
        self.assertEqual(len(self.bars.latest_symbol_data), len(self.bars.symbols))
        self.assertEqual(len(self.bars.symbols), len(self.bars.symbol_data))
        self.assertEqual(self.bars.start_date, "2024-05-06")
        self.assertEqual(self.bars.end_date, "2024-05-11")

    def test_bars(self):
        for i in range(5):
            self.bars.update_bars()

        latest_bars = self.bars.get_latest_bars("AAPL", 5)
        self.assertEqual(len(self.bars.events.queue), 5)
        self.assertEqual(self.bars.current_datetime.strftime("%Y-%m-%d"), "2024-05-10")
        self.assertTupleEqual(
            latest_bars[-1]._fields, ("Index", "open", "high", "low", "close", "volume")
        )
        self.assertAlmostEqual(latest_bars[-2].open, 182.31, 2)
        self.assertAlmostEqual(latest_bars[-2].high, 184.41, 2)
        self.assertAlmostEqual(latest_bars[-2].low, 181.86, 2)
        self.assertAlmostEqual(latest_bars[-2].close, 184.32, 2)
        self.assertAlmostEqual(latest_bars[-2].volume, 49_049_437.45, 0)

    def test_invalid_symbol(self):
        with self.assertRaises(ValueError):
            YahooDataHandler(
                symbols=["INVALID_SYMBOL"],
                start_date="2024-05-06",
                end_date="2024-05-11",
                use_cols=["volume"],
            )
        with self.assertRaises(ValueError):
            YahooDataHandler(
                symbols=["INVALID_SYMBOL1", "INVALID_SYMBOL2"],
                start_date="2024-05-06",
                end_date="2024-05-11",
                use_cols=["volume"],
            )


if __name__ == "__main__":
    unittest.main()
