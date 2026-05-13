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

REQUIRED_WALK_FORWARD_FOLD_COLUMNS = {
    "ticker",
    "fold",
    "train_start",
    "train_end",
    "test_start",
    "test_end",
    "signal_count",
    "trade_count",
    "win_rate",
    "average_return",
    "cumulative_return",
    "max_drawdown",
    "sharpe_ratio",
    "exposure_count",
}

REQUIRED_WALK_FORWARD_SUMMARY_COLUMNS = {
    "ticker",
    "fold_count",
    "total_trade_count",
    "total_exposure_count",
    "average_win_rate",
    "average_return",
    "compounded_return",
    "worst_max_drawdown",
    "average_sharpe_ratio",
    "positive_fold_ratio",
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


def validate_walk_forward_folds(frame: Any) -> None:
    missing_columns = REQUIRED_WALK_FORWARD_FOLD_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required walk-forward fold columns: {sorted(missing_columns)}")

    if frame.empty:
        return

    if frame[["train_start", "train_end", "test_start", "test_end"]].isna().any().any():
        raise ValueError("Walk-forward folds contain null dates.")

    if (frame[["trade_count", "signal_count", "exposure_count"]] < 0).any().any():
        raise ValueError("Walk-forward folds contain negative counts.")


def validate_walk_forward_summary(frame: Any) -> None:
    missing_columns = REQUIRED_WALK_FORWARD_SUMMARY_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required walk-forward summary columns: {sorted(missing_columns)}"
        )

    if frame.empty:
        raise ValueError("Walk-forward summary frame is empty.")
