from typing import Any

from krx_alpha.contracts.backtest_contract import (
    validate_backtest_metrics,
    validate_backtest_trades,
)


class BacktestReportGenerator:
    """Generate a Markdown report for simple backtest results."""

    def generate(self, trades: Any, metrics: Any) -> str:
        validate_backtest_trades(trades)
        validate_backtest_metrics(metrics)
        metric = metrics.iloc[0]

        return "\n".join(
            [
                f"# Backtest Report: {metric['ticker']}",
                "",
                "## Summary",
                "",
                f"- Trade count: {int(metric['trade_count'])}",
                f"- Exposure count: {int(metric['exposure_count'])}",
                f"- Win rate: {_format_percent(metric['win_rate'])}",
                f"- Average return: {_format_percent(metric['average_return'])}",
                f"- Cumulative return: {_format_percent(metric['cumulative_return'])}",
                f"- Max drawdown: {_format_percent(metric['max_drawdown'])}",
                f"- Sharpe ratio: {float(metric['sharpe_ratio']):.2f}",
                "",
                "## Method",
                "",
                "- Uses `buy_candidate` final signals only.",
                "- Enters on the next trading day's open.",
                "- Exits after the configured holding period at close.",
                "- Applies transaction cost and slippage assumptions.",
                "- Skips trades without enough future bars.",
                "",
                "## Risk Note",
                "",
                "This is a simple MVP backtest. It is not a production-grade execution simulator.",
                "",
            ]
        )


def _format_percent(value: Any) -> str:
    return f"{float(value) * 100:.2f}%"
