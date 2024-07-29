MarginTrader is a Python backtesting framework for simulating algorithmic trading strategies in a margin trading environment. The framework is based on [QuantStart's](https://www.quantstart.com/articles/Event-Driven-Backtesting-with-Python-Part-I/) articles on event-driven backtesting system.

## Disclaimer
Note: This project is a practice project for software development. It may not be fully functional and should be used with caution. Review the code and conduct your own testing before relying on it for any critical tasks.

## Features

- **Margin Trading Simulation**: Supports backtesting for margin trading with adjustable leverage.
- **Custom Strategy Implementation**: Easily implement and test your own trading strategies.
- **Historical Data Support**: Use historical market data to simulate trades.

## Planned Features
- **Risk Management**: Simulating stop-loss, take-profit, and other risk management techniques.
- **MetaTrader Integration**: Seamless integration with MetaTrader 5 (MT5) for data retrieval and trade execution.
- **Advanced Order Types**: Support for a variety of order types (e.g., limit, stop orders).
- **Live Trading Support**: Transition from backtesting to live trading with minimal modifications.

## Installation
1. Install [Poetry]("https://python-poetry.org/docs/#installation") (if you haven't already)

2. Clone the repository:
    ```bash
    git clone https://github.com/tiloye/margin_trader.git
    cd margin_trader
    ```

3. Install the dependencies and the package:
    ```bash
    poetry install
    ```

## Getting Started

Check this [example notebook](https://github.com/tiloye/margin_trader/blob/main/examples/end2end_backtest.ipynb)
for how to get started with MarginTrader.

## Contributing
Contributions are welcome! Whether it's reporting bugs, suggesting features, or writing code, your help is appreciated.

## License

MarginTrader is licensed under the MIT License. See the [LICENSE](LICENSE) file for more details.
