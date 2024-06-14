import unittest

import pandas as pd

import margin_trader.performance.metric as perf


class TestPerformance(unittest.TestCase):
    def setUp(self) -> None:
        self.data = pd.Series(
            data=[100.0, 106.0, 102.0, 98.0, 103.0, 99.0, 105.0, 104.0, 107.0, 112.0],
            index=[
                "2024-05-03",
                "2024-05-04",
                "2024-05-07",
                "2024-05-08",
                "2024-05-09",
                "2024-05-10",
                "2024-05-11",
                "2024-05-12",
                "2024-05-13",
                "2024-05-14",
            ],
        )
        self.returns = self.data.pct_change().fillna(0.0)

    def test_calculate_total_return(self):
        gross_return = perf.total_return(self.returns)
        gross_return = round(gross_return, 4)
        self.assertEqual(gross_return, 0.1200)

    def test_calculate_annual_return(self):
        ann_ret = perf.annual_return(self.returns)
        ann_ret = round(ann_ret, 4)
        self.assertEqual(ann_ret, 16.3898)

    def test_calculate_annual_volatility(self):
        ann_vol = perf.annual_volatility(self.returns)
        ann_vol = round(ann_vol, 4)
        self.assertEqual(ann_vol, 0.6686)

    def test_calculate_max_drawdown(self):
        dd = perf.max_drawdown(self.returns)
        dd = round(dd, 4)
        self.assertEqual(dd, -0.0755)

    def test_calculate_longest_drawdown_period(self):
        dd_duration = perf.longest_dd_period(self.returns)
        self.assertEqual(dd_duration, 6)

    def test_calculate_sharpe_ratio(self):
        sr = perf.sharpe_ratio(self.returns)
        sr = round(sr, 4)
        self.assertEqual(round(sr, 4), 4.5939)

    def test_calculate_var(self):
        var = perf.var(self.returns)
        var = round(var, 4)
        self.assertEqual(var, -0.0390)

    def test_calculate_win_rate(self):
        self.data.iloc[1::3] = -1 * self.data.iloc[1::3]
        win_rate = perf.win_rate(self.data)
        self.assertEqual(win_rate, 0.7)

    def test_calculate_expectancy(self):
        self.data.iloc[1::3] = -1 * self.data.iloc[1::3]
        expectancy = perf.expectancy(self.data)
        self.assertAlmostEqual(expectancy, 41.0)

    def test_calcualte_profit_factor(self):
        self.data.iloc[1::3] = -1 * self.data.iloc[1::3]
        pfactor = perf.profit_factor(self.data)
        self.assertAlmostEqual(pfactor, 2.3099, 4)


if __name__ == "__main__":
    unittest.main()
