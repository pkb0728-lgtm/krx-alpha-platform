from pathlib import Path

import pandas as pd

from krx_alpha.dashboard.data_loader import (
    action_counts,
    find_latest_backtest_metrics,
    find_latest_drift_result,
    find_latest_macro_features,
    find_latest_ml_metrics,
    find_latest_news_sentiment,
    find_latest_paper_portfolio_summary,
    find_latest_paper_summary,
    find_latest_universe_summary,
    find_latest_walk_forward_summary,
    load_backtest_metrics,
    load_backtest_trades,
    load_drift_result,
    load_macro_features,
    load_markdown,
    load_ml_metrics,
    load_ml_predictions,
    load_news_sentiment,
    load_paper_portfolio_summary,
    load_paper_portfolio_trades,
    load_paper_summary,
    load_paper_trades,
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


def test_dashboard_data_loader_reads_latest_paper_trading(tmp_path: Path) -> None:
    summary_dir = tmp_path / "data" / "backtest" / "paper_summary"
    ledger_dir = tmp_path / "data" / "backtest" / "paper_trade_ledger"
    summary_dir.mkdir(parents=True)
    ledger_dir.mkdir(parents=True)
    summary_path = summary_dir / "005930_20240101_20240131.parquet"
    ledger_path = ledger_dir / "005930_20240101_20240131.parquet"

    pd.DataFrame(
        {
            "ticker": ["005930"],
            "initial_cash": [10_000_000.0],
            "ending_cash": [9_000_000.0],
            "ending_position_value": [1_100_000.0],
            "ending_equity": [10_100_000.0],
            "cumulative_return": [0.01],
            "realized_pnl": [0.0],
            "unrealized_pnl": [100_000.0],
            "trade_count": [1],
            "buy_count": [1],
            "sell_count": [0],
            "exposure_count": [1],
            "win_rate": [0.0],
            "mode": ["paper"],
            "generated_at": [pd.Timestamp("2026-05-14T00:00:00Z")],
        }
    ).to_parquet(summary_path, index=False)
    pd.DataFrame(
        {
            "date": ["2024-01-03"],
            "execution_date": ["2024-01-04"],
            "ticker": ["005930"],
            "side": ["buy"],
            "status": ["filled"],
            "shares": [10],
            "execution_price": [100.0],
            "gross_amount": [1000.0],
            "fees": [1.5],
            "realized_pnl": [0.0],
            "cash_after": [9998.5],
            "position_qty_after": [10],
            "position_value_after": [1000.0],
            "equity_after": [9998.5],
            "signal_action": ["buy_candidate"],
            "confidence_score": [75.0],
            "reason": ["paper buy"],
            "mode": ["paper"],
        }
    ).to_parquet(ledger_path, index=False)

    latest_path = find_latest_paper_summary(tmp_path)
    assert latest_path == summary_path

    summary = load_paper_summary(summary_path)
    trades = load_paper_trades(summary_path)

    assert summary.loc[0, "ending_equity"] == 10_100_000.0
    assert trades.loc[0, "side"] == "buy"


def test_dashboard_data_loader_reads_latest_paper_portfolio(tmp_path: Path) -> None:
    summary_dir = tmp_path / "data" / "backtest" / "paper_portfolio_summary"
    ledger_dir = tmp_path / "data" / "backtest" / "paper_portfolio_trade_ledger"
    summary_dir.mkdir(parents=True)
    ledger_dir.mkdir(parents=True)
    summary_path = summary_dir / "demo_20240101_20240131.parquet"
    ledger_path = ledger_dir / "demo_20240101_20240131.parquet"

    pd.DataFrame(
        {
            "universe": ["demo"],
            "ticker": ["005930,005380"],
            "initial_cash": [10_000_000.0],
            "ending_cash": [8_500_000.0],
            "ending_position_value": [1_700_000.0],
            "ending_equity": [10_200_000.0],
            "cumulative_return": [0.02],
            "realized_pnl": [0.0],
            "unrealized_pnl": [200_000.0],
            "trade_count": [2],
            "buy_count": [2],
            "sell_count": [0],
            "exposure_count": [2],
            "win_rate": [0.0],
            "mode": ["paper"],
            "generated_at": [pd.Timestamp("2026-05-14T00:00:00Z")],
            "requested_ticker_count": [3],
            "loaded_ticker_count": [2],
            "skipped_tickers": ["000660"],
            "active_position_count": [2],
            "gross_exposure_pct": [16.67],
            "cash_pct": [83.33],
        }
    ).to_parquet(summary_path, index=False)
    pd.DataFrame(
        {
            "date": ["2024-01-03", "2024-01-03"],
            "execution_date": ["2024-01-04", "2024-01-04"],
            "ticker": ["005930", "005380"],
            "side": ["buy", "buy"],
            "status": ["filled", "filled"],
            "shares": [10, 5],
            "execution_price": [100.0, 200.0],
            "gross_amount": [1000.0, 1000.0],
            "fees": [1.5, 1.5],
            "realized_pnl": [0.0, 0.0],
            "cash_after": [9_998_998.5, 9_997_997.0],
            "position_qty_after": [10, 5],
            "position_value_after": [1000.0, 1000.0],
            "equity_after": [9_999_998.5, 9_999_997.0],
            "signal_action": ["buy_candidate", "buy_candidate"],
            "confidence_score": [75.0, 72.0],
            "reason": ["paper buy", "paper buy"],
            "mode": ["paper", "paper"],
        }
    ).to_parquet(ledger_path, index=False)

    latest_path = find_latest_paper_portfolio_summary(tmp_path)
    assert latest_path == summary_path

    summary = load_paper_portfolio_summary(summary_path)
    trades = load_paper_portfolio_trades(summary_path)

    assert summary.loc[0, "universe"] == "demo"
    assert summary.loc[0, "loaded_ticker_count"] == 2
    assert trades["ticker"].tolist() == ["005380", "005930"]


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


def test_dashboard_data_loader_reads_latest_ml_baseline_outputs(tmp_path: Path) -> None:
    metrics_dir = tmp_path / "data" / "signals" / "ml_metrics"
    predictions_dir = tmp_path / "data" / "signals" / "ml_predictions"
    metrics_dir.mkdir(parents=True)
    predictions_dir.mkdir(parents=True)
    metrics_path = metrics_dir / "005380_20240101_20240331_h5.parquet"
    predictions_path = predictions_dir / "005380_20240101_20240331_h5.parquet"

    pd.DataFrame(
        {
            "split": ["train", "test"],
            "row_count": [39, 17],
            "positive_label_rate": [0.58, 0.52],
            "predicted_positive_rate": [0.61, 0.47],
            "accuracy": [0.7, 0.64],
            "precision": [0.71, 0.6],
            "recall": [0.72, 0.5],
            "f1_score": [0.715, 0.522],
            "roc_auc": [0.81, 0.652],
            "brier_score": [0.2, 0.24],
            "average_probability": [0.57, 0.53],
        }
    ).to_parquet(metrics_path, index=False)
    pd.DataFrame(
        {
            "date": ["2024-03-20", "2024-03-21"],
            "ticker": ["005380", "005380"],
            "split": ["test", "test"],
            "probability_positive_forward_return": [0.62, 0.78],
            "predicted_label": [1, 1],
            "target_positive_forward_return": [0, 1],
            "forward_return": [-0.01, 0.03],
            "label_end_date": ["2024-03-27", "2024-03-28"],
            "top_feature_reason": ["rsi_14_supports_positive_probability", "baseline"],
        }
    ).to_parquet(predictions_path, index=False)

    latest_path = find_latest_ml_metrics(tmp_path)
    assert latest_path == metrics_path

    metrics = load_ml_metrics(metrics_path)
    predictions = load_ml_predictions(metrics_path)

    assert metrics.loc[0, "split"] == "test"
    assert metrics.loc[0, "roc_auc"] == 0.652
    assert predictions.loc[0, "probability_positive_forward_return"] == 0.78


def test_dashboard_data_loader_reads_latest_news_sentiment(tmp_path: Path) -> None:
    news_dir = tmp_path / "data" / "features" / "news_sentiment_daily"
    news_dir.mkdir(parents=True)
    news_path = news_dir / "005930_20240101_20240131.parquet"

    pd.DataFrame(
        {
            "date": ["2024-01-30", "2024-01-31"],
            "as_of_date": ["2024-01-30", "2024-01-31"],
            "ticker": ["005930", "005930"],
            "news_count": [2, 3],
            "positive_news_count": [1, 2],
            "negative_news_count": [0, 1],
            "sentiment_score": [0.2, 0.4],
            "news_score": [59.0, 68.0],
            "news_reason": ["news_sentiment_positive", "news_sentiment_positive"],
            "top_headline": ["Headline A", "Headline B"],
            "summary": ["Summary A", "Summary B"],
            "source": ["news_sentiment", "gemini_news_sentiment"],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 2,
        }
    ).to_parquet(news_path, index=False)

    latest_path = find_latest_news_sentiment(tmp_path)
    assert latest_path == news_path

    frame = load_news_sentiment(news_path)
    assert frame.loc[0, "date"] == "2024-01-31"
    assert frame.loc[0, "news_score"] == 68.0


def test_dashboard_data_loader_reads_latest_macro_features(tmp_path: Path) -> None:
    macro_dir = tmp_path / "data" / "features" / "macro_fred_daily"
    macro_dir.mkdir(parents=True)
    macro_path = macro_dir / "macro_20240101_20240131_DGS10_DFF_DEXKOUS.parquet"

    pd.DataFrame(
        {
            "date": ["2024-01-30", "2024-01-31"],
            "as_of_date": ["2024-01-30", "2024-01-31"],
            "us_10y_yield": [4.1, 4.2],
            "fed_funds_rate": [5.33, 5.33],
            "usdkrw": [1320.0, 1335.0],
            "us_10y_yield_change_5d": [0.1, 0.2],
            "usdkrw_change_5d": [10.0, 15.0],
            "usdkrw_change_pct_5d": [0.01, 0.02],
            "macro_score": [45.0, 43.0],
            "macro_reason": ["macro_environment_neutral", "us_10y_yield_rising"],
            "source": ["macro_features", "macro_features"],
            "feature_created_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 2,
        }
    ).to_parquet(macro_path, index=False)

    latest_path = find_latest_macro_features(tmp_path)
    assert latest_path == macro_path

    frame = load_macro_features(macro_path)
    assert frame.loc[0, "date"] == "2024-01-31"
    assert frame.loc[0, "macro_score"] == 43.0
