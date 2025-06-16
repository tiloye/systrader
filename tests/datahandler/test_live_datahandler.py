import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from systrader.datahandler.live import AlpacaDataHandler
from systrader.datahandler.utils import convert_bar_df_to_tuple, transform_data

DATA_DIR = Path(__file__).parent.parent.joinpath("data")


def get_symbol_df(n_of_symbols=1):
    df1 = pd.read_csv(DATA_DIR / "SYMBOL1.csv", parse_dates=True)
    df1["symbol"] = "SYMBOL1"
    if n_of_symbols == 1:
        df = df1
    else:
        df2 = pd.read_csv(DATA_DIR / "SYMBOL3.csv", parse_dates=True)
        df2["symbol"] = "SYMBOL3"
        df = pd.concat([df1, df2], axis=0)
    df.columns = df.columns.str.lower()
    df.rename(columns={"date": "timestamp"}, inplace=True)
    df.set_index(["symbol", "timestamp"], inplace=True)

    return df


class TestAlpacaDataHandler(unittest.TestCase):
    def test_initialization(self):
        with self.subTest("One instrument"):
            handler = AlpacaDataHandler(
                symbols="EQ.SYMBOL1",
                time_frame="1D",
                api_key="api-key",
                secret_key="secret-key",
            )

            self.assertEqual(handler.symbols, ["SYMBOL1"])
            self.assertDictEqual(handler._asset_class, {"EQ": ["SYMBOL1"], "CC": []})
            self.assertEqual(handler.time_frame, "1D")
            self.assertEqual(handler.feed, "iex")
            self.assertEqual(handler.latest_symbol_data, {})

        with self.subTest("Multiple instruments"):
            handler = AlpacaDataHandler(
                symbols=["EQ.SYMBOL1", "EQ.SYMBOL2", "CC.SYMBOL3"],
                time_frame="1D",
                api_key="api-key",
                secret_key="secret-key",
            )

            self.assertEqual(handler.symbols, ["SYMBOL1", "SYMBOL2", "SYMBOL3"])
            self.assertEqual(
                handler._asset_class, {"EQ": ["SYMBOL1", "SYMBOL2"], "CC": ["SYMBOL3"]}
            )
            self.assertEqual(handler.time_frame, "1D")
            self.assertEqual(handler.feed, "iex")
            self.assertEqual(handler.latest_symbol_data, {})

    @patch("systrader.datahandler.live.StockHistoricalDataClient")
    def test_request_stock_data(self, mock_client):
        with self.subTest("One instrument"):
            mock_client.return_value.get_stock_bars.return_value.df = get_symbol_df()

            handler = AlpacaDataHandler(
                symbols="EQ.SYMBOL1",
                time_frame="1D",
                api_key="api-key",
                secret_key="secret-key",
            )

            handler.request_bars(5)

            self.assertTrue(
                all(
                    [
                        bar._fields[i] in ("timestamp", "open", "high", "low", "close")
                        for i in range(5)
                        for bar in handler.latest_symbol_data["SYMBOL1"]
                    ]
                )
            )

        with self.subTest("Two instrument"):
            mock_client.return_value.get_stock_bars.return_value.df = get_symbol_df(2)

            handler = AlpacaDataHandler(
                symbols=["EQ.SYMBOL1", "EQ.SYMBOL3"],
                time_frame="1D",
                api_key="api-key",
                secret_key="secret-key",
            )

            handler.request_bars(5)

            self.assertTrue(
                all(
                    [
                        bar._fields[i] in ("timestamp", "open", "high", "low", "close")
                        for i in range(5)
                        for bar in handler.latest_symbol_data["SYMBOL1"]
                    ]
                )
            )
            self.assertTrue(
                all(
                    [
                        bar._fields[i] in ("timestamp", "open", "high", "low", "close")
                        for i in range(5)
                        for bar in handler.latest_symbol_data["SYMBOL3"]
                    ]
                )
            )

    @patch("systrader.datahandler.live.CryptoHistoricalDataClient")
    def test_request_crypto_data(self, mock_client):
        with self.subTest("One instrument"):
            mock_client.return_value.get_crypto_bars.return_value.df = get_symbol_df()

            handler = AlpacaDataHandler(
                symbols="CC.SYMBOL1",
                time_frame="1D",
                api_key="api-key",
                secret_key="secret-key",
            )

            handler.request_bars(5)

            self.assertTrue(
                all(
                    [
                        bar._fields[i] in ("timestamp", "open", "high", "low", "close")
                        for i in range(5)
                        for bar in handler.latest_symbol_data["SYMBOL1"]
                    ]
                )
            )

        with self.subTest("Two instrument"):
            mock_client.return_value.get_stock_bars.return_value.df = get_symbol_df(2)
            mock_client.return_value.get_crypto_bars.return_value.df = get_symbol_df(2)

            handler = AlpacaDataHandler(
                symbols=["CC.SYMBOL1", "CC.SYMBOL3"],
                time_frame="1D",
                api_key="api-key",
                secret_key="secret-key",
            )

            handler.request_bars(5)

            self.assertTrue(
                all(
                    [
                        bar._fields[i] in ("timestamp", "open", "high", "low", "close")
                        for i in range(5)
                        for bar in handler.latest_symbol_data["SYMBOL1"]
                    ]
                )
            )
            self.assertTrue(
                all(
                    [
                        bar._fields[i] in ("timestamp", "open", "high", "low", "close")
                        for i in range(5)
                        for bar in handler.latest_symbol_data["SYMBOL3"]
                    ]
                )
            )

    @patch("systrader.datahandler.live.CryptoHistoricalDataClient")
    @patch("systrader.datahandler.live.StockHistoricalDataClient")
    def test_request_stock_crypto_bars(self, mock_stock_client, mock_crypto_client):
        bars = get_symbol_df(2)
        stock_bars = bars.iloc[:-5]
        crypto_bars = bars.iloc[-5:]
        mock_stock_client.return_value.get_stock_bars.return_value.df = stock_bars
        mock_crypto_client.return_value.get_crypto_bars.return_value.df = crypto_bars

        handler = AlpacaDataHandler(
            symbols=["EQ.SYMBOL1", "CC.SYMBOL3"],
            time_frame="1D",
            api_key="api-key",
            secret_key="secret-key",
        )

        handler.request_bars(5)

        self.assertTrue(
            all(
                [
                    bar._fields[i] in ("timestamp", "open", "high", "low", "close")
                    for i in range(5)
                    for bar in handler.latest_symbol_data["SYMBOL1"]
                ]
            )
        )
        self.assertTrue(
            all(
                [
                    bar._fields[i] in ("timestamp", "open", "high", "low", "close")
                    for i in range(5)
                    for bar in handler.latest_symbol_data["SYMBOL3"]
                ]
            )
        )

    @patch("systrader.datahandler.live.CryptoHistoricalDataClient")
    @patch("systrader.datahandler.live.StockHistoricalDataClient")
    def test_update_bars(self, mock_stock_client, mock_crypto_client):
        bars = get_symbol_df(2)
        stock_bars = bars.iloc[:-5]
        crypto_bars = bars.iloc[-5:]

        with self.subTest("One instrument"):
            mock_stock_client.return_value.get_stock_bars.return_value.df = stock_bars
            mock_crypto_client.return_value.get_crypto_bars.return_value.df = (
                crypto_bars
            )
            handler = AlpacaDataHandler(
                symbols="EQ.SYMBOL1",
                time_frame="1D",
                api_key="api-key",
                secret_key="secret-key",
            )
            handler.request_bars(5)

            bar_df = pd.DataFrame(
                [[106, 110, 102, 105, 1000]],
                columns=["open", "high", "low", "close", "volume"],
                index=pd.MultiIndex.from_tuples(
                    [("SYMBOL1", datetime(2024, 5, 8))], names=["symbol", "timestamp"]
                ),
            )
            mock_stock_client.return_value.get_stock_bars.return_value.df = bar_df

            handler.update_bars()

            bar_df = transform_data(bar_df.loc["SYMBOL1"])
            bar_tuple = convert_bar_df_to_tuple("SYMBOL1", bar_df)

            self.assertEqual(len(handler.latest_symbol_data["SYMBOL1"]), 6)
            self.assertEqual(handler.latest_symbol_data["SYMBOL1"][-1], bar_tuple)

        with self.subTest("Two Instruments"):
            mock_stock_client.return_value.get_stock_bars.return_value.df = stock_bars
            mock_crypto_client.return_value.get_crypto_bars.return_value.df = (
                crypto_bars
            )

            handler = AlpacaDataHandler(
                symbols=["EQ.SYMBOL1", "CC.SYMBOL3"],
                time_frame="1D",
                api_key="api-key",
                secret_key="secret-key",
            )
            handler.request_bars(5)

            symbol1_new_bar_df = pd.DataFrame(
                [[106, 110, 102, 105, 1000]],
                columns=["open", "high", "low", "close", "volume"],
                index=pd.MultiIndex.from_tuples(
                    [("SYMBOL1", datetime(2024, 5, 8))], names=["symbol", "timestamp"]
                ),
            )

            symbol3_new_bar_df = pd.DataFrame(
                [[103, 108, 102, 107, 1200]],
                columns=["open", "high", "low", "close", "volume"],
                index=pd.MultiIndex.from_tuples(
                    [("SYMBOL3", datetime(2024, 5, 8))], names=["symbol", "timestamp"]
                ),
            )
            mock_stock_client.return_value.get_stock_bars.return_value.df = (
                symbol1_new_bar_df
            )
            mock_crypto_client.return_value.get_crypto_bars.return_value.df = (
                symbol3_new_bar_df
            )

            handler.update_bars()

            bar_df1 = transform_data(symbol1_new_bar_df.loc["SYMBOL1"])
            bar_df2 = transform_data(symbol3_new_bar_df.loc["SYMBOL3"])
            bar_tuple1 = convert_bar_df_to_tuple("SYMBOL1", bar_df1)
            bar_tuple2 = convert_bar_df_to_tuple("SYMBOL3", bar_df2)

            self.assertEqual(len(handler.latest_symbol_data["SYMBOL1"]), 6)
            self.assertEqual(len(handler.latest_symbol_data["SYMBOL3"]), 6)
            self.assertEqual(handler.latest_symbol_data["SYMBOL1"][-1], bar_tuple1)
            self.assertEqual(handler.latest_symbol_data["SYMBOL3"][-1], bar_tuple2)


if __name__ == "__main__":
    unittest.main()
