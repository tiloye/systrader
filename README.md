SysTrader is a Python framework for backtesting algorithmic trading strategies. The framework is based on [QuantStart's](https://www.quantstart.com/articles/Event-Driven-Backtesting-with-Python-Part-I/) articles on event-driven backtesting system.

## Features

- **Margin Trading Simulation**: Supports backtesting for margin trading with adjustable leverage.
- **Custom Strategy Implementation**: Easily implement and test your own trading strategies.
- **Historical Data Support**: Use historical bar data to simulate trades.
- **Risk Management**: Simulate stop-loss and take-profit orders.
- **Different Order Types**: Supports a variety of order types (market, limit, and stop orders).

## Planned Feature

- **Live Trading Support**: Transition from backtesting to live trading with minimal modifications.

## How to Install
1. Install [Poetry](https://python-poetry.org/docs/#installation) (if you haven't already)

2. Clone the repository:
    ```bash
    git clone https://github.com/tiloye/systrader.git
    cd systrader
    ```

3. Install the dependencies and the package:
    ```bash
    poetry install
    ```

## Getting Started

Check this [example notebook](https://github.com/tiloye/systrader/blob/main/examples/end2end_backtest.ipynb)
for how to get started with SysTrader.

## License

SysTrader is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
