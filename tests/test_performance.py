import unittest
import pandas as pd
import margin_trader.performance as perf

class TestPerformance(unittest.TestCase):
    def setUp(self) -> None:
        self.data = pd.Series(
            data = [100.0, 106.0, 102.0, 98.0, 103.0, 99.0, 105.0, 104.0, 107.0, 112.0],
            index = ["2024-05-03", "2024-05-04", "2024-05-07", "2024-05-08",
                     "2024-05-09", "2024-05-10", "2024-05-11", "2024-05-12",
                     "2024-05-13", "2024-05-14"] 
        )
        self.returns = self.data.pct_change().fillna(0.0)

    def test_calculate_total_return(self):
        gross_return = perf.calculate_total_return(self.returns)
        gross_return = round(gross_return, 4)
        self.assertEqual(gross_return, 0.1200)

    def test_calculate_sharpe_ratio(self):
        sr = perf.calculate_sharpe_ratio(self.returns)
        sr = round(sr, 4)
        self.assertEqual(round(sr, 4), 4.5939)

if __name__ == "__main__":
    unittest.main()