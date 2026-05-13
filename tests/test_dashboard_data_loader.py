from pathlib import Path

import pandas as pd

from krx_alpha.dashboard.data_loader import (
    action_counts,
    find_latest_backtest_metrics,
    find_latest_drift_result,
    find_latest_universe_summary,
    find_latest_walk_forward_summary,
    load_backtest_metrics,
    load_backtest_trades,
    load_drift_result,
    load_markdown,
    load_universe_summary,
    load_walk_forward_folds,
    load_walk_forward_summary,
)


def test_dashboard_data_loader_reads_latest_summary(tmp_path: Path) -> None:
    summary_dir = tmp_path / "data" / "signals" / "universe_summary_daily"
    summary_dir.mkdir(parents=True)
    summary_path = summary_dir / "universe_20240101_20240131.parquet"
    pd.DataFrame(
        {
            "ticker": ["005930", "005380"],
            "status": ["success", "success"],
            "latest_action": ["watch", "buy_candidate"],
            "latest_confidence_score": [63.0, 72.0],
            "error": ["", ""],
        }
    ).to_parquet(summary_path, index=False)

    latest_path = find_latest_universe_summary(tmp_path)
    assert latest_path == summary_path

    frame = load_universe_summary(summary_path)
    assert frame.loc[0, "ticker"] == "005380"

    counts = action_counts(frame)
    assert set(counts["latest_action"]) == {"watch", "buy_candidate"}


def test_dashboard_load_markdown(tmp_path: Path) -> None:
    report_path = tmp_path / "report.md"
    report_path.write_text("# Report", encoding="utf-8")

    assert load_markdown(report_path) == "# Report"


def test_dashboard_data_loader_reads_latest_backtest(tmp_path: Path) -> None:
    metrics_dir = tmp_path / "data" / "backtest" / "metrics"
    trades_dir = tmp_path / "data" / "backtest" / "trades"
    metrics_dir.mkdir(parents=True)
    trades_dir.mkdir(parents=True)
    metrics_path = metrics_dir / "005380_20240101_20240331.parquet"
    trades_path = trades_dir / "005380_20240101_20240331.parquet"

    pd.DataFrame(
        {
            "ticker": ["005380"],
            "trade_count": [7],
            "win_rate": [0.5714],
            "average_return": [0.08],
            "cumulative_return": [0.7867],
            "max_drawdown": [-0.1035],
            "sharpe_ratio": [4.33],
            "exposure_count": [8],
        }
    ).to_parquet(metrics_path, index=False)
    pd.DataFrame(
        {
            "ticker": ["005380"],
            "signal_date": ["2024-01-10"],
            "entry_date": ["2024-01-11"],
            "exit_date": ["2024-01-18"],
            "entry_price": [100.0],
            "exit_price": [110.0],
            "net_return": [0.0975],
            "signal_confidence": [72.0],
        }
    ).to_parquet(trades_path, index=False)

    latest_path = find_latest_backtest_metrics(tmp_path)
    assert latest_path == metrics_path

    metrics = load_backtest_metrics(metrics_path)
    trades = load_backtest_trades(metrics_path)

    assert metrics.loc[0, "ticker"] == "005380"
    assert metrics.loc[0, "trade_count"] == 7
    assert trades.loc[0, "net_return"] == 0.0975


def test_dashboard_data_loader_reads_latest_walk_forward(tmp_path: Path) -> None:
    summary_dir = tmp_path / "data" / "backtest" / "walk_forward_summary"
    folds_dir = tmp_path / "data" / "backtest" / "walk_forward_folds"
    summary_dir.mkdir(parents=True)
    folds_dir.mkdir(parents=True)
    summary_path = summary_dir / "005380_20240101_20240331.parquet"
    folds_path = folds_dir / "005380_20240101_20240331.parquet"

    pd.DataFrame(
        {
            "ticker": ["005380"],
            "fold_count": [3],
            "total_trade_count": [2],
            "total_exposure_count": [3],
            "average_win_rate": [0.3333],
            "average_return": [0.012],
            "compounded_return": [0.0364],
            "worst_max_drawdown": [-0.052],
            "average_sharpe_ratio": [1.25],
            "positive_fold_ratio": [0.6667],
        }
    ).to_parquet(summary_path, index=False)
    pd.DataFrame(
        {
            "ticker": ["005380", "005380"],
            "fold": [2, 1],
            "train_start": ["2024-01-08", "2024-01-01"],
            "train_end": ["2024-02-02", "2024-01-26"],
            "test_start": ["2024-02-05", "2024-01-29"],
            "test_end": ["2024-02-09", "2024-02-02"],
            "signal_count": [5, 5],
            "trade_count": [1, 1],
            "win_rate": [1.0, 0.0],
            "average_return": [0.03, -0.01],
            "cumulative_return": [0.03, -0.01],
            "max_drawdown": [0.0, -0.02],
            "sharpe_ratio": [2.0, -1.0],
            "exposure_count": [1, 1],
        }
    ).to_parquet(folds_path, index=False)

    latest_path = find_latest_walk_forward_summary(tmp_path)
    assert latest_path == summary_path

    summary = load_walk_forward_summary(summary_path)
    folds = load_walk_forward_folds(summary_path)

    assert summary.loc[0, "ticker"] == "005380"
    assert summary.loc[0, "fold_count"] == 3
    assert folds["fold"].tolist() == [1, 2]


def test_dashboard_data_loader_reads_latest_drift_result(tmp_path: Path) -> None:
    drift_dir = tmp_path / "data" / "signals" / "drift"
    drift_dir.mkdir(parents=True)
    drift_path = drift_dir / "data_drift_demo.parquet"

    pd.DataFrame(
        {
            "feature": ["rsi_14", "volatility_5d"],
            "mean_shift_score": [3.0, 0.2],
            "std_ratio": [1.1, 1.0],
            "missing_rate_delta": [0.0, 0.0],
            "drift_detected": [True, False],
            "drift_reason": ["mean_shift", "stable"],
        }
    ).to_parquet(drift_path, index=False)

    latest_path = find_latest_drift_result(tmp_path)
    assert latest_path == drift_path

    frame = load_drift_result(drift_path)
    assert bool(frame.loc[0, "drift_detected"]) is True
    assert frame.loc[0, "feature"] == "rsi_14"
