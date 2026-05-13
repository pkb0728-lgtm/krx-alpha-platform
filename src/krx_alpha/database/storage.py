from pathlib import Path
from typing import Any

DATA_LAYERS = ("raw", "processed", "features", "signals", "backtest")


def ensure_project_dirs(project_root: Path) -> None:
    for layer in DATA_LAYERS:
        (project_root / "data" / layer).mkdir(parents=True, exist_ok=True)

    for directory in ("logs", "models", "notebooks", "docs"):
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


def investor_flow_feature_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    dataset_dir = features_dataset_dir(project_root, "investor_flow_daily")
    return dataset_dir / f"{ticker}_{start_date}_{end_date}.parquet"


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


def walk_forward_report_file_path(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
) -> Path:
    return (
        project_root / "reports" / "backtest" / f"walk_forward_{ticker}_{start_date}_{end_date}.md"
    )


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
