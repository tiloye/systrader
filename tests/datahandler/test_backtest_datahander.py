import datetime as dt
import unittest
from collections import namedtuple
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from systrader.datahandler import (
    BacktestDataHandler,
    HistoricCSVDataHandler,
    PandasDataHandler,
    YahooDataHandler,
)
from systrader.event import EventManager


class BacktestDataHandlerStub(BacktestDataHandler):
    def _load_data(self, symbols, start, end):
        data = [[1, 2, 3, 4, 100]] * 3
        df = pd.DataFrame(
            data,
            columns=["open", "high", "low", "close", "volume"],
            index=[
                dt.datetime(2025, 1, 1),
                dt.datetime(2025, 1, 2),
                dt.datetime(2025, 1, 3),
            ],
        )
        return [df]


class TestBacktestDataHandler(unittest.TestCase):
    def setUp(self) -> None:
        self.data_handler = BacktestDataHandlerStub(
            "TEST_SYMBOL", "2025-01-01", "2025-01-03"
        )
        self.data_handler.add_event_manager(EventManager())

    def test_init_raises_not_implemented_error(self):
        with self.assertRaises(NotImplementedError):
            BacktestDataHandler("TEST_SYMBOL", "2025-01-01", "2025-01-01")

    def test_init(self):
        data_handler = BacktestDataHandlerStub(
            "TEST_SYMBOL", "2025-01-01", "2025-01-03"
        )

        self.assertEqual(data_handler.symbols, ["TEST_SYMBOL"])
        self.assertEqual(data_handler.start_date, "2025-01-01")
        self.assertEqual(data_handler.end_date, "2025-01-03")
        self.assertEqual(
            len(data_handler.latest_symbol_data), len(data_handler.symbols)
        )
        self.assertEqual(len(data_handler.symbol_data), len(data_handler.symbols))
        self.assertEqual(data_handler.latest_symbol_data, {"TEST_SYMBOL": []})
        self.assertIsInstance(data_handler.symbol_data["TEST_SYMBOL"], map)

    def test_request_bars(self):
        data_handler = BacktestDataHandlerStub(
            "TEST_SYMBOL", "2025-01-01", "2025-01-03"
        )
        data_handler.request_bars(2)

        self.assertEqual(len(data_handler.latest_symbol_data["TEST_SYMBOL"]), 2)

    def test_update_bars(self):
        Bar = namedtuple(
            "Test_SYMBOL", ["timestamp", "open", "high", "low", "close", "volume"]
        )
        expected_bar = Bar(dt.datetime(2025, 1, 1), 1, 2, 3, 4, 100)

        self.data_handler.update_bars()

        self.assertEqual(
            self.data_handler.latest_symbol_data["TEST_SYMBOL"], [expected_bar]
        )
        self.assertEqual(
            self.data_handler.latest_symbol_data["TEST_SYMBOL"][0]._fields, Bar._fields
        )
        self.assertEqual(self.data_handler.timestamp, dt.datetime(2025, 1, 1))

    def test_get_latest_price(self):
        self.data_handler.update_bars()
        ohlc = ["open", "high", "low", "close"]
        symbol = self.data_handler.symbols[0]
        bar = [
            self.data_handler.get_latest_price(symbol, price=price) for price in ohlc
        ]
        self.assertListEqual(bar, [1, 2, 3, 4])


CSV_DIR = Path(__file__).parent.parent.joinpath("data")


class TestHistoricalCSVDataHandler(unittest.TestCase):
    def setUp(self):
        event_manager = EventManager()

        self.symbols = ["SYMBOL1"]
        self.bars = HistoricCSVDataHandler(
            csv_dir=CSV_DIR,
            symbols=self.symbols,
            start_date="2024-05-03",
            end_date="2024-05-07",
        )
        self.bars.add_event_manager(event_manager)

    def test_initialisation(self):
        self.assertEqual(CSV_DIR, self.bars.csv_dir)
        self.assertEqual(len(self.bars.latest_symbol_data), len(self.bars.symbols))
        self.assertEqual(len(self.bars.symbols), len(self.bars.symbol_data))
        self.assertEqual(self.bars.start_date, "2024-05-03")
        self.assertEqual(self.bars.end_date, "2024-05-07")


class TestPandasDataHandler(unittest.TestCase):
    def test_initialisation(self):
        data = [
            ["2024-05-03", 100.0, 105.0, 98.0, 102.0, 0],
            ["2024-05-04", 102.0, 108.0, 100.0, 106.0, 0],
            ["2024-05-05", 106.0, 110.0, 104.0, 108.0, 0],
            ["2024-05-06", 108.0, 112.0, 106.0, 110.0, 0],
            ["2024-05-07", 110.0, 115.0, 108.0, 112.0, 0],
        ]
        df = pd.DataFrame(
            data, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df.set_index("timestamp", inplace=True)

        symbols_dfs = {"AAPL": df}
        bars = PandasDataHandler(
            symbol_dfs=symbols_dfs,
            start_date="2024-05-03",
            end_date="2024-05-07",
        )
        bars.add_event_manager(EventManager())

        self.assertEqual(len(bars.latest_symbol_data), len(bars.symbols))
        self.assertEqual(len(bars.symbols), len(bars.symbol_data))
        self.assertEqual(bars.start_date, "2024-05-03")
        self.assertEqual(bars.end_date, "2024-05-07")


data = {
    "Open": [100.0, 102.0, 104.0],
    "High": [110.0, 112.0, 114.0],
    "Low": [90.0, 92.0, 94.0],
    "Close": [105.0, 107.0, 109.0],
    "Adj Close": [105.0, 106.5, 108.0],
    "Volume": [1000, 1500, 2000],
}
MOCK_SOURCE = "systrader.datahandler.backtest"


class TestYahooDataHandler(unittest.TestCase):
    @patch(MOCK_SOURCE + ".yf")
    def test_valid_symbols(self, mock_yfinance):
        mock_yfinance.download.return_value = pd.DataFrame(
            data,
            index=pd.date_range("2024-05-06", "2024-05-08"),
        )

        symbols = ["VALID_SYMBOL1"]
        bars = YahooDataHandler(
            symbols=symbols,
            start_date="2024-05-06",
            end_date="2024-05-09",
        )
        bars.add_event_manager(EventManager())

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
            self.assertEqual(bars.timestamp, dt.datetime(2024, 5, 8))
            self.assertTupleEqual(
                latest_bars[-1]._fields,
                ("timestamp", "open", "high", "low", "close", "volume"),
            )

    @patch(MOCK_SOURCE + ".yf")
    @patch(MOCK_SOURCE + ".shared")
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
                )


if __name__ == "__main__":
    unittest.main()
