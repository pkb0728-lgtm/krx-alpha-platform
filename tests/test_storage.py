from pathlib import Path

from krx_alpha.database.storage import (
    DATA_LAYERS,
    dart_company_file_path,
    dart_disclosure_event_file_path,
    dart_disclosure_file_path,
    dart_financial_feature_file_path,
    dart_financial_file_path,
    drift_result_file_path,
    ensure_project_dirs,
    experiment_log_file_path,
    investor_flow_feature_file_path,
    market_regime_file_path,
    market_regime_report_file_path,
    ml_metrics_file_path,
    ml_model_artifact_file_path,
    ml_model_report_file_path,
    ml_prediction_file_path,
    ml_training_dataset_file_path,
    monitoring_report_file_path,
    operations_health_file_path,
    paper_portfolio_position_file_path,
    paper_portfolio_report_file_path,
    paper_portfolio_summary_file_path,
    paper_portfolio_trade_ledger_file_path,
    paper_position_file_path,
    paper_summary_file_path,
    paper_trade_ledger_file_path,
    paper_trading_report_file_path,
    raw_investor_flow_file_path,
    screening_report_file_path,
    screening_result_csv_path,
    screening_result_file_path,
    walk_forward_folds_file_path,
    walk_forward_report_file_path,
    walk_forward_summary_file_path,
)


def test_ensure_project_dirs(tmp_path: Path) -> None:
    ensure_project_dirs(tmp_path)

    for layer in DATA_LAYERS:
        assert (tmp_path / "data" / layer).is_dir()
    assert (tmp_path / "experiments").is_dir()


def test_market_regime_paths(tmp_path: Path) -> None:
    assert (
        market_regime_file_path(
            tmp_path,
            "005930",
            "20240101",
            "20240331",
        )
        .as_posix()
        .endswith("data/signals/market_regime_daily/005930_20240101_20240331.parquet")
    )
    assert (
        market_regime_report_file_path(
            tmp_path,
            "005930",
            "20240101",
            "20240331",
        )
        .as_posix()
        .endswith("reports/regime/005930_20240101_20240331.md")
    )


def test_walk_forward_backtest_paths(tmp_path: Path) -> None:
    assert (
        walk_forward_folds_file_path(tmp_path, "005930", "20240101", "20240331")
        .as_posix()
        .endswith("data/backtest/walk_forward_folds/005930_20240101_20240331.parquet")
    )
    assert (
        walk_forward_summary_file_path(tmp_path, "005930", "20240101", "20240331")
        .as_posix()
        .endswith("data/backtest/walk_forward_summary/005930_20240101_20240331.parquet")
    )
    assert (
        walk_forward_report_file_path(tmp_path, "005930", "20240101", "20240331")
        .as_posix()
        .endswith("reports/backtest/walk_forward_005930_20240101_20240331.md")
    )


def test_paper_trading_paths(tmp_path: Path) -> None:
    assert (
        paper_trade_ledger_file_path(tmp_path, "005930", "20240101", "20240131")
        .as_posix()
        .endswith("data/backtest/paper_trade_ledger/005930_20240101_20240131.parquet")
    )
    assert (
        paper_position_file_path(tmp_path, "005930", "20240101", "20240131")
        .as_posix()
        .endswith("data/backtest/paper_positions/005930_20240101_20240131.parquet")
    )
    assert (
        paper_summary_file_path(tmp_path, "005930", "20240101", "20240131")
        .as_posix()
        .endswith("data/backtest/paper_summary/005930_20240101_20240131.parquet")
    )
    assert (
        paper_trading_report_file_path(tmp_path, "005930", "20240101", "20240131")
        .as_posix()
        .endswith("reports/paper_trading/005930_20240101_20240131.md")
    )
    assert (
        paper_portfolio_trade_ledger_file_path(tmp_path, "demo", "20240101", "20240131")
        .as_posix()
        .endswith("data/backtest/paper_portfolio_trade_ledger/demo_20240101_20240131.parquet")
    )
    assert (
        paper_portfolio_position_file_path(tmp_path, "demo", "20240101", "20240131")
        .as_posix()
        .endswith("data/backtest/paper_portfolio_positions/demo_20240101_20240131.parquet")
    )
    assert (
        paper_portfolio_summary_file_path(tmp_path, "demo", "20240101", "20240131")
        .as_posix()
        .endswith("data/backtest/paper_portfolio_summary/demo_20240101_20240131.parquet")
    )
    assert (
        paper_portfolio_report_file_path(tmp_path, "demo", "20240101", "20240131")
        .as_posix()
        .endswith("reports/paper_trading/portfolio_demo_20240101_20240131.md")
    )


def test_dart_storage_paths(tmp_path: Path) -> None:
    assert (
        dart_company_file_path(tmp_path, "00126380")
        .as_posix()
        .endswith("data/raw/dart_company/00126380.parquet")
    )
    assert (
        raw_investor_flow_file_path(tmp_path, "005930", "20240101", "20240131")
        .as_posix()
        .endswith("data/raw/investor_flow_daily/005930_20240101_20240131.parquet")
    )
    assert (
        investor_flow_feature_file_path(tmp_path, "005930", "20240101", "20240131")
        .as_posix()
        .endswith("data/features/investor_flow_daily/005930_20240101_20240131.parquet")
    )
    assert (
        dart_financial_file_path(tmp_path, "00126380", "2023", "11011")
        .as_posix()
        .endswith("data/raw/dart_financials/00126380_2023_11011.parquet")
    )
    assert (
        dart_financial_feature_file_path(tmp_path, "00126380", "2023", "11011")
        .as_posix()
        .endswith("data/features/dart_financials/00126380_2023_11011.parquet")
    )
    assert (
        dart_disclosure_file_path(
            tmp_path,
            "00126380",
            "20240101",
            "20240131",
        )
        .as_posix()
        .endswith("data/raw/dart_disclosures/00126380_20240101_20240131.parquet")
    )
    assert (
        dart_disclosure_event_file_path(
            tmp_path,
            "00126380",
            "20240101",
            "20240131",
        )
        .as_posix()
        .endswith("data/features/dart_disclosure_events/00126380_20240101_20240131.parquet")
    )


def test_experiment_log_path(tmp_path: Path) -> None:
    assert experiment_log_file_path(tmp_path).as_posix().endswith("experiments/experiment_log.csv")


def test_monitoring_paths(tmp_path: Path) -> None:
    assert (
        drift_result_file_path(tmp_path, "data_drift_demo")
        .as_posix()
        .endswith("data/signals/drift/data_drift_demo.parquet")
    )
    assert (
        operations_health_file_path(tmp_path, "operations_health_latest")
        .as_posix()
        .endswith("data/signals/operations_health/operations_health_latest.parquet")
    )
    assert (
        screening_result_file_path(tmp_path, "screening_universe_demo")
        .as_posix()
        .endswith("data/signals/screening_daily/screening_universe_demo.parquet")
    )
    assert (
        screening_result_csv_path(tmp_path, "screening_universe_demo")
        .as_posix()
        .endswith("data/signals/screening_daily/screening_universe_demo.csv")
    )
    assert (
        screening_report_file_path(tmp_path, "screening_universe_demo")
        .as_posix()
        .endswith("reports/screening/screening_universe_demo.md")
    )
    assert (
        monitoring_report_file_path(tmp_path, "data_drift_demo")
        .as_posix()
        .endswith("reports/monitoring/data_drift_demo.md")
    )


def test_ml_training_dataset_path(tmp_path: Path) -> None:
    assert (
        ml_training_dataset_file_path(tmp_path, "005930", "20240101", "20240131", 5)
        .as_posix()
        .endswith("data/features/ml_training/005930_20240101_20240131_h5.parquet")
    )


def test_ml_probability_baseline_paths(tmp_path: Path) -> None:
    assert (
        ml_prediction_file_path(tmp_path, "005930", "20240101", "20240131", 5)
        .as_posix()
        .endswith("data/signals/ml_predictions/005930_20240101_20240131_h5.parquet")
    )
    assert (
        ml_metrics_file_path(tmp_path, "005930", "20240101", "20240131", 5)
        .as_posix()
        .endswith("data/signals/ml_metrics/005930_20240101_20240131_h5.parquet")
    )
    assert (
        ml_model_artifact_file_path(tmp_path, "005930", "20240101", "20240131", 5)
        .as_posix()
        .endswith("models/probability_baseline/005930_20240101_20240131_h5.json")
    )
    assert (
        ml_model_report_file_path(tmp_path, "005930", "20240101", "20240131", 5)
        .as_posix()
        .endswith("reports/modeling/probability_baseline_005930_20240101_20240131_h5.md")
    )
