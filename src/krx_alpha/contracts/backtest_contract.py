from typing import Any

REQUIRED_BACKTEST_TRADE_COLUMNS = {
    "ticker",
    "signal_date",
    "entry_date",
    "exit_date",
    "entry_price",
    "exit_price",
    "gross_return",
    "net_return",
    "holding_days",
    "signal_confidence",
}

REQUIRED_BACKTEST_METRIC_COLUMNS = {
    "ticker",
    "trade_count",
    "win_rate",
    "average_return",
    "cumulative_return",
    "max_drawdown",
    "sharpe_ratio",
    "exposure_count",
}


def validate_backtest_trades(frame: Any) -> None:
    missing_columns = REQUIRED_BACKTEST_TRADE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required backtest trade columns: {sorted(missing_columns)}")

    if frame.empty:
        return

    if frame[["signal_date", "entry_date", "exit_date"]].isna().any().any():
        raise ValueError("Backtest trades contain null dates.")

    if (frame[["entry_price", "exit_price"]] <= 0).any().any():
        raise ValueError("Backtest trades contain non-positive prices.")


def validate_backtest_metrics(frame: Any) -> None:
    missing_columns = REQUIRED_BACKTEST_METRIC_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required backtest metric columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Backtest metrics frame is empty.")
