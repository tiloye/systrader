# mypy: disable-error-code=union-attr
import unittest
from queue import Queue

import pandas as pd

from margin_trader.data_source import PandasDataHandler


class TestHistoricalCSVDataHandler(unittest.TestCase):
    def test_initialisation(self):
        data = [
            ["2024-05-03", 100.0, 105.0, 98.0, 102.0, 0],
            ["2024-05-04", 102.0, 108.0, 100.0, 106.0, 0],
            ["2024-05-05", 106.0, 110.0, 104.0, 108.0, 0],
            ["2024-05-06", 108.0, 112.0, 106.0, 110.0, 0],
            ["2024-05-07", 110.0, 115.0, 108.0, 112.0, 0],
        ]
        df = pd.DataFrame(
            data, columns=["timeindex", "open", "high", "low", "close", "volume"]
        )
        df["timeindex"] = pd.to_datetime(df["timeindex"])
        df.set_index("timeindex", inplace=True)

        event_queue = Queue()
        symbols_dfs = {"AAPL": df}
        bars = PandasDataHandler(
            symbol_dfs=symbols_dfs,
            start_date="2024-05-03",
            end_date="2024-05-07",
        )
        bars.add_event_queue(event_queue)

        self.assertEqual(len(bars.latest_symbol_data), len(bars.symbols))
        self.assertEqual(len(bars.symbols), len(bars.symbol_data))
        self.assertEqual(bars.start_date, "2024-05-03")
        self.assertEqual(bars.end_date, "2024-05-07")


if __name__ == "__main__":
    unittest.main()
