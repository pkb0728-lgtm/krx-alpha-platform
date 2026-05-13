from typing import Any

from krx_alpha.contracts.backtest_contract import (
    validate_backtest_metrics,
    validate_backtest_trades,
    validate_walk_forward_folds,
    validate_walk_forward_summary,
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


class WalkForwardReportGenerator:
    """Generate a Markdown report for walk-forward backtest validation."""

    def generate(self, folds: Any, summary: Any) -> str:
        validate_walk_forward_folds(folds)
        validate_walk_forward_summary(summary)
        metric = summary.iloc[0]

        return "\n".join(
            [
                f"# Walk-Forward Backtest Report: {metric['ticker']}",
                "",
                "## Summary",
                "",
                f"- Fold count: {int(metric['fold_count'])}",
                f"- Total trade count: {int(metric['total_trade_count'])}",
                f"- Total exposure count: {int(metric['total_exposure_count'])}",
                f"- Average win rate: {_format_percent(metric['average_win_rate'])}",
                f"- Average return: {_format_percent(metric['average_return'])}",
                f"- Compounded return: {_format_percent(metric['compounded_return'])}",
                f"- Worst max drawdown: {_format_percent(metric['worst_max_drawdown'])}",
                f"- Average Sharpe ratio: {float(metric['average_sharpe_ratio']):.2f}",
                f"- Positive fold ratio: {_format_percent(metric['positive_fold_ratio'])}",
                "",
                "## Fold Results",
                "",
                _format_fold_table(folds),
                "",
                "## Method",
                "",
                "- Uses rolling train and out-of-sample test windows.",
                "- Backtests only the test window signals in each fold.",
                "- Reuses the next-day-entry simple backtest engine.",
                "- Applies transaction cost and slippage assumptions.",
                "",
                "## Risk Note",
                "",
                "Walk-forward validation improves robustness review, but it is still "
                "a research backtest rather than live execution evidence.",
                "",
            ]
        )


def _format_fold_table(folds: Any) -> str:
    if folds.empty:
        return "No walk-forward folds were created."

    rows = [
        "| Fold | Train | Test | Trades | Win Rate | Return | MDD | Sharpe |",
        "| ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for _, row in folds.iterrows():
        rows.append(
            "| "
            f"{int(row['fold'])} | "
            f"{row['train_start']} to {row['train_end']} | "
            f"{row['test_start']} to {row['test_end']} | "
            f"{int(row['trade_count'])} | "
            f"{_format_percent(row['win_rate'])} | "
            f"{_format_percent(row['cumulative_return'])} | "
            f"{_format_percent(row['max_drawdown'])} | "
            f"{float(row['sharpe_ratio']):.2f} |"
        )
    return "\n".join(rows)


def _format_percent(value: Any) -> str:
    return f"{float(value) * 100:.2f}%"
