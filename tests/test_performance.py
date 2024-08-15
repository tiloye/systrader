import unittest

import pandas as pd

import margin_trader.performance.metric as metrics
import margin_trader.performance.utils as putils


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
        self.data.index = pd.DatetimeIndex(self.data.index)
        self.returns = self.data.pct_change().fillna(0.0)

    def test_total_return(self):
        gross_return = metrics.total_return(self.returns)
        gross_return = round(gross_return, 4)
        self.assertEqual(gross_return, 0.1200)

    def test_annual_return(self):
        ann_ret = metrics.annual_return(self.returns)
        ann_ret = round(ann_ret, 4)
        self.assertEqual(ann_ret, 16.3898)

    def test_annual_volatility(self):
        ann_vol = metrics.annual_volatility(self.returns)
        ann_vol = round(ann_vol, 4)
        self.assertEqual(ann_vol, 0.6686)

    def test_max_drawdown(self):
        dd = metrics.max_drawdown(self.returns)
        dd = round(dd, 4)
        self.assertEqual(dd, -0.0755)

    def test_longest_drawdown_period(self):
        dd_duration = metrics.longest_dd_period(self.returns)
        self.assertEqual(dd_duration, 6)

    def test_sharpe_ratio(self):
        sr = metrics.sharpe_ratio(self.returns)
        sr = round(sr, 4)
        self.assertEqual(round(sr, 4), 4.5939)

    def test_var(self):
        var = metrics.var(self.returns)
        var = round(var, 4)
        self.assertEqual(var, -0.0390)

    def test_win_rate(self):
        self.data.iloc[1::3] = -1 * self.data.iloc[1::3]
        win_rate = metrics.win_rate(self.data)
        self.assertEqual(win_rate, 0.7)

    def test_expectancy(self):
        self.data.iloc[1::3] = -1 * self.data.iloc[1::3]
        expectancy = metrics.expectancy(self.data)
        self.assertAlmostEqual(expectancy, 41.0)

    def test_profit_factor(self):
        self.data.iloc[1::3] = -1 * self.data.iloc[1::3]
        pfactor = metrics.profit_factor(self.data)
        self.assertAlmostEqual(pfactor, 2.3099, 4)

    def test_returns_stats(self):
        rets = metrics.returns_stats(self.returns)

        self.assertIsInstance(rets, pd.DataFrame)
        self.assertTupleEqual(rets.shape, (13, 1))


class TestPerformanceUtils(unittest.TestCase):
    def setUp(self):
        dates = pd.date_range("2024-01-01", "2024-01-05")
        self.bl_eq_history = pd.DataFrame(
            {
                "balance": [1000.0, 1000.0, 1010.0, 1010.0, 1005.0],
                "equity": [1000.0, 1001.0, 1010.0, 1006.0, 1005.0],
            },
            index=dates,
        )
        self.trade_history = pd.DataFrame(
            {
                "symbol": ["A", "B"],
                "units": [10, 10],
                "open_price": [10, 20],
                "close_price": [11.0, 19.5],
                "commission": [0.0, 0.0],
                "side": ["buy", "buy"],
                "open_time": [dates[0], dates[2]],
                "close_time": [dates[2], dates[4]],
                "pnl": [10.0, -5.0],
                "id": [1, 2],
            }
        )

    def test_get_trade_roi(self):
        trade_history = self.trade_history
        balance_history = self.bl_eq_history["balance"]
        expected = pd.Series([0.01, -0.0050])
        rets = putils.get_trade_roi(trade_history, balance_history).round(4)

        pd.testing.assert_series_equal(rets, expected)

    def test_get_pyfolio_roundtrips(self):
        account_history = {
            "balance_equity": self.bl_eq_history,
            "positions": self.trade_history,
        }

        expected_dataframe = pd.DataFrame(
            data={
                "pnl": [10.0, -5.0],
                "open_dt": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03")],
                "close_dt": [pd.Timestamp("2024-01-03"), pd.Timestamp("2024-01-05")],
                "long": [True, True],
                "symbol": ["A", "B"],
                "duration": [pd.Timedelta(days=2), pd.Timedelta(days=2)],
                "returns": [0.01, -0.005],
            }
        )
        pyfolio_rts = putils.get_pyfolio_roundtrips(account_history)

        pd.testing.assert_frame_equal(pyfolio_rts, expected_dataframe, rtol=1e-2)


if __name__ == "__main__":
    unittest.main()
