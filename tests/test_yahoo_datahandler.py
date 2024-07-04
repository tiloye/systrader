import unittest
from queue import Queue
from unittest.mock import patch

import pandas as pd

from margin_trader.data_source import YahooDataHandler

data = {
    "Open": [100.0, 102.0, 104.0],
    "High": [110.0, 112.0, 114.0],
    "Low": [90.0, 92.0, 94.0],
    "Close": [105.0, 107.0, 109.0],
    "Adj Close": [105.0, 106.5, 108.0],
    "Volume": [1000, 1500, 2000],
}


class TestYahooDataHandler(unittest.TestCase):
    @classmethod
    @patch("margin_trader.data_source.yahoo_data_handler.yf")
    def setUpClass(cls, mock_yf) -> None:
        mock_yf.download.return_value = pd.DataFrame(
            data,
            index=pd.date_range("2024-05-06", "2024-05-08"),
        )

        cls.event_queue = Queue()
        cls.symbols = "AAPL"
        cls.bars = YahooDataHandler(
            symbols=cls.symbols,
            start_date="2024-05-06",
            end_date="2024-05-09",
            use_cols=["volume"],
        )
        cls.bars.add_event_queue(cls.event_queue)

    def test_initialisation(self):
        self.assertIsInstance(self.bars.symbols, list)
        self.assertEqual(len(self.bars.latest_symbol_data), len(self.bars.symbols))
        self.assertEqual(len(self.bars.symbols), len(self.bars.symbol_data))
        self.assertEqual(self.bars.start_date, "2024-05-06")
        self.assertEqual(self.bars.end_date, "2024-05-09")

    def test_price_adjustment(self):
        df = pd.DataFrame(data)
        expected_data = {
            "open": [100, 101.52, 103.05],
            "high": [110, 111.48, 112.95],
            "low": [90, 91.57, 93.14],
            "close": [105, 106.5, 108.0],
            "adj close": [105, 106.5, 108],
            "volume": [1000.0, 1507.04, 2018.52],
        }
        expected_df = pd.DataFrame(expected_data)

        result_df = self.bars._adjust(df).round(2)
        pd.testing.assert_frame_equal(result_df, expected_df)
        self.assertTrue(True)

    def test_bars(self):
        for i in range(5):
            self.bars.update_bars()

        latest_bars = self.bars.get_latest_bars("AAPL", 3)
        self.assertEqual(len(self.bars.events.queue), 3)
        self.assertEqual(self.bars.current_datetime.strftime("%Y-%m-%d"), "2024-05-08")
        self.assertTupleEqual(
            latest_bars[-1]._fields, ("Index", "open", "high", "low", "close", "volume")
        )

    @patch("margin_trader.data_source.yahoo_data_handler.yf")
    @patch("margin_trader.data_source.yahoo_data_handler.shared")
    def test_invalid_symbol(self, mock_shared, mock_yf):
        # Testing for single asset
        mock_yf.download.return_value = pd.DataFrame(
            columns=["Open", "High", "Low", "Close", "Adj Close", "Volume"]
        )
        mock_shared._ERRORS = {"INVALID_SYMBOL": ...}
        with self.assertRaises(ValueError):
            YahooDataHandler(
                symbols=["INVALID_SYMBOL"],
                start_date="2024-05-06",
                end_date="2024-05-09",
                use_cols=["volume"],
            )

        # Testing for multiple assets
        symbols = ["INVALID_SYMBOL1", "INVALID_SYMBOL2"]
        columns = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
        col_index = pd.MultiIndex.from_product([symbols, columns])
        mock_yf.download.return_value = pd.DataFrame(columns=col_index)
        mock_shared._ERRORS = {"INVALID_SYMBOL1": ..., "INVALID_SYMBOL2": ...}
        with self.assertRaises(ValueError):
            YahooDataHandler(
                symbols=["INVALID_SYMBOL1", "INVALID_SYMBOL2"],
                start_date="2024-05-06",
                end_date="2024-05-09",
                use_cols=["volume"],
            )


if __name__ == "__main__":
    unittest.main()
