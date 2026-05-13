import pandas as pd

from krx_alpha.dashboard.data_loader import (
    action_counts,
    find_latest_backtest_metrics,
    find_latest_universe_summary,
    load_backtest_metrics,
    load_backtest_trades,
    load_markdown,
    load_universe_summary,
)


def test_dashboard_data_loader_reads_latest_summary(tmp_path) -> None:
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


def test_dashboard_load_markdown(tmp_path) -> None:
    report_path = tmp_path / "report.md"
    report_path.write_text("# Report", encoding="utf-8")

    assert load_markdown(report_path) == "# Report"


def test_dashboard_data_loader_reads_latest_backtest(tmp_path) -> None:
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
