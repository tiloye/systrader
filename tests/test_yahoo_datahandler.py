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
    @patch("margin_trader.data_source.yahoo_data_handler.yf")
    def test_valid_symbols(self, mock_yfinance):
        mock_yfinance.download.return_value = pd.DataFrame(
            data,
            index=pd.date_range("2024-05-06", "2024-05-08"),
        )

        event_queue = Queue()
        symbols = ["VALID_SYMBOL1"]
        bars = YahooDataHandler(
            symbols=symbols,
            start_date="2024-05-06",
            end_date="2024-05-09",
            use_cols=["volume"],
        )
        bars.add_event_queue(event_queue)

        with self.subTest("initialisation"):
            self.assertIsInstance(bars.symbols, list)
            self.assertEqual(len(bars.latest_symbol_data), len(bars.symbols))
            self.assertEqual(len(bars.symbols), len(bars.symbol_data))
            self.assertEqual(bars.start_date, "2024-05-06")
            self.assertEqual(bars.end_date, "2024-05-09")

        with self.subTest("price_adjustment"):
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

            result_df = bars._adjust(df).round(2)
            pd.testing.assert_frame_equal(result_df, expected_df)
            self.assertTrue(True)

        with self.subTest("bar_updates"):
            for i in range(5):
                bars.update_bars()

            latest_bars = bars.get_latest_bars(symbols[0], 3)
            self.assertEqual(len(bars.events.queue), 3)
            self.assertEqual(bars.current_datetime.strftime("%Y-%m-%d"), "2024-05-08")
            self.assertTupleEqual(
                latest_bars[-1]._fields,
                ("Index", "open", "high", "low", "close", "volume"),
            )

    @patch("margin_trader.data_source.yahoo_data_handler.yf")
    @patch("margin_trader.data_source.yahoo_data_handler.shared")
    def test_invalid_symbol(self, mock_shared, mock_yf):
        with self.subTest("single_invalid_symbol"):
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

        with self.subTest("mutiple_invalid_symbols"):
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
