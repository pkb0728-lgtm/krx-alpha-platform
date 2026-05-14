from pathlib import Path
from typing import Any

DATA_LAYERS = ("raw", "processed", "features", "signals", "backtest")


def ensure_project_dirs(project_root: Path) -> None:
    for layer in DATA_LAYERS:
        (project_root / "data" / layer).mkdir(parents=True, exist_ok=True)

    for directory in ("logs", "models", "notebooks", "docs", "experiments"):
        (project_root / directory).mkdir(parents=True, exist_ok=True)


def dataset_dir(project_root: Path, layer: str, dataset_name: str) -> Path:
    if layer not in DATA_LAYERS:
        raise ValueError(f"Unknown data layer: {layer}")

    path = project_root / "data" / layer / dataset_name
    path.mkdir(parents=True, exist_ok=True)
    return path


def raw_dataset_dir(project_root: Path, dataset_name: str) -> Path:
    return dataset_dir(project_root, "raw", dataset_name)


def processed_dataset_dir(project_root: Path, dataset_name: str) -> Path:
    return dataset_dir(project_root, "processed", dataset_name)


def features_dataset_dir(project_root: Path, dataset_name: str) -> Path:
    return dataset_dir(project_root, "features", dataset_name)


def signals_dataset_dir(project_root: Path, dataset_name: str) -> Path:
    return dataset_dir(project_root, "signals", dataset_name)


def backtest_dataset_dir(project_root: Path, dataset_name: str) -> Path:
    return dataset_dir(project_root, "backtest", dataset_name)


def raw_price_file_path(project_root: Path, ticker: str, start_date: str, end_date: str) -> Path:
    dataset_dir = raw_dataset_dir(project_root, "prices_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def raw_investor_flow_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = raw_dataset_dir(project_root, "investor_flow_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def raw_news_file_path(project_root: Path, ticker: str, start_date: str, end_date: str) -> Path:
    dataset_dir = raw_dataset_dir(project_root, "news_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def raw_macro_file_path(
    project_root: Path,
    start_date: str,
    end_date: str,
    series_slug: str,
) -> Path:
    dataset_dir = raw_dataset_dir(project_root, "macro_fred_daily")
    return dataset_dir / f"macro_{start_date}_{end_date}_{series_slug}.parquet"


def dart_company_file_path(project_root: Path, corp_code: str) -> Path:
    dataset_dir = raw_dataset_dir(project_root, "dart_company")
    return dataset_dir / f"{corp_code}.parquet"


def dart_financial_file_path(
    project_root: Path,
    corp_code: str,
    bsns_year: str,
    reprt_code: str,
) -> Path:
    dataset_dir = raw_dataset_dir(project_root, "dart_financials")
    return dataset_dir / f"{corp_code}_{bsns_year}_{reprt_code}.parquet"


def dart_disclosure_file_path(
    project_root: Path,
    corp_code: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = raw_dataset_dir(project_root, "dart_disclosures")
    return dataset_dir / f"{corp_code}_{start_date}_{end_date}.parquet"


def dart_disclosure_event_file_path(
    project_root: Path,
    corp_code: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = features_dataset_dir(project_root, "dart_disclosure_events")
    return dataset_dir / f"{corp_code}_{start_date}_{end_date}.parquet"


def processed_price_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = processed_dataset_dir(project_root, "prices_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def price_feature_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = features_dataset_dir(project_root, "prices_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def ml_training_dataset_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
    holding_days: int,
) -> Path:
    dataset_dir = features_dataset_dir(project_root, "ml_training")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}_h{holding_days}.parquet"


def ml_prediction_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
    holding_days: int,
) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "ml_predictions")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}_h{holding_days}.parquet"


def ml_metrics_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
    holding_days: int,
) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "ml_metrics")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}_h{holding_days}.parquet"


def ml_model_artifact_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
    holding_days: int,
) -> Path:
    path = project_root / "models" / "probability_baseline"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{ticker}_{start_date}_{end_date}_h{holding_days}.json"


def ml_model_report_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
    holding_days: int,
) -> Path:
    return (
        project_root
        / "reports"
        / "modeling"
        / f"probability_baseline_{ticker}_{start_date}_{end_date}_h{holding_days}.md"
    )


def investor_flow_feature_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = features_dataset_dir(project_root, "investor_flow_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def news_sentiment_feature_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = features_dataset_dir(project_root, "news_sentiment_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def macro_feature_file_path(
    project_root: Path,
    start_date: str,
    end_date: str,
    series_slug: str,
) -> Path:
    dataset_dir = features_dataset_dir(project_root, "macro_fred_daily")
    return dataset_dir / f"macro_{start_date}_{end_date}_{series_slug}.parquet"


def dart_financial_feature_file_path(
    project_root: Path,
    corp_code: str,
    bsns_year: str,
    reprt_code: str,
) -> Path:
    dataset_dir = features_dataset_dir(project_root, "dart_financials")
    return dataset_dir / f"{corp_code}_{bsns_year}_{reprt_code}.parquet"


def daily_score_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "scores_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def final_signal_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "final_signals_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def market_regime_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "market_regime_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def universe_summary_file_path(project_root: Path, start_date: str, end_date: str) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "universe_summary_daily")
    return dataset_dir / f"universe_{start_date}_{end_date}.parquet"


def universe_summary_csv_path(project_root: Path, start_date: str, end_date: str) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "universe_summary_daily")
    return dataset_dir / f"universe_{start_date}_{end_date}.csv"


def universe_file_path(project_root: Path, universe_name: str) -> Path:
    dataset_dir = processed_dataset_dir(project_root, "universe")
    return dataset_dir / f"{universe_name}.parquet"


def universe_csv_path(project_root: Path, universe_name: str) -> Path:
    dataset_dir = processed_dataset_dir(project_root, "universe")
    return dataset_dir / f"{universe_name}.csv"


def daily_report_file_path(project_root: Path, ticker: str, report_date: str) -> Path:
    return project_root / "reports" / "daily" / f"{ticker}_{report_date}.md"


def universe_report_file_path(project_root: Path, start_date: str, end_date: str) -> Path:
    return project_root / "reports" / "universe" / f"universe_{start_date}_{end_date}.md"


def market_regime_report_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    return project_root / "reports" / "regime" / f"{ticker}_{start_date}_{end_date}.md"


def backtest_report_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    return project_root / "reports" / "backtest" / f"{ticker}_{start_date}_{end_date}.md"


def backtest_trades_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "trades")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def backtest_metrics_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "metrics")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def walk_forward_folds_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "walk_forward_folds")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def walk_forward_summary_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "walk_forward_summary")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def paper_trade_ledger_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "paper_trade_ledger")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def paper_position_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "paper_positions")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def paper_summary_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "paper_summary")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


def paper_portfolio_trade_ledger_file_path(
    project_root: Path,
    portfolio_name: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "paper_portfolio_trade_ledger")
    return dataset_dir / f"{portfolio_name}_{start_date}_{end_date}.parquet"


def paper_portfolio_position_file_path(
    project_root: Path,
    portfolio_name: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "paper_portfolio_positions")
    return dataset_dir / f"{portfolio_name}_{start_date}_{end_date}.parquet"


def paper_portfolio_summary_file_path(
    project_root: Path,
    portfolio_name: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = backtest_dataset_dir(project_root, "paper_portfolio_summary")
    return dataset_dir / f"{portfolio_name}_{start_date}_{end_date}.parquet"


def walk_forward_report_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    return (
        project_root / "reports" / "backtest" / f"walk_forward_{ticker}_{start_date}_{end_date}.md"
    )


def paper_trading_report_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    return project_root / "reports" / "paper_trading" / f"{ticker}_{start_date}_{end_date}.md"


def paper_portfolio_report_file_path(
    project_root: Path,
    portfolio_name: str,
    start_date: str,
    end_date: str,
) -> Path:
    return (
        project_root
        / "reports"
        / "paper_trading"
        / f"portfolio_{portfolio_name}_{start_date}_{end_date}.md"
    )


def drift_result_file_path(project_root: Path, report_name: str) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "drift")
    return dataset_dir / f"{report_name}.parquet"


def data_quality_file_path(project_root: Path, report_name: str) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "data_quality")
    return dataset_dir / f"{report_name}.parquet"


def operations_health_file_path(project_root: Path, report_name: str) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "operations_health")
    return dataset_dir / f"{report_name}.parquet"


def api_health_file_path(project_root: Path, report_name: str) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "api_health")
    return dataset_dir / f"{report_name}.parquet"


def screening_result_file_path(project_root: Path, report_name: str) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "screening_daily")
    return dataset_dir / f"{report_name}.parquet"


def screening_result_csv_path(project_root: Path, report_name: str) -> Path:
    dataset_dir = signals_dataset_dir(project_root, "screening_daily")
    return dataset_dir / f"{report_name}.csv"


def screening_report_file_path(project_root: Path, report_name: str) -> Path:
    return project_root / "reports" / "screening" / f"{report_name}.md"


def monitoring_report_file_path(project_root: Path, report_name: str) -> Path:
    return project_root / "reports" / "monitoring" / f"{report_name}.md"


def experiment_log_file_path(project_root: Path) -> Path:
    return project_root / "experiments" / "experiment_log.csv"


def read_parquet(path: Path) -> Any:
    import pandas as pd

    return pd.read_parquet(path)


def write_parquet(frame: Any, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)
    return path


def write_csv(frame: Any, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return path


def write_text(text: str, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path
