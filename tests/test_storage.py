from pathlib import Path

from krx_alpha.database.storage import (
    DATA_LAYERS,
    dart_company_file_path,
    dart_disclosure_event_file_path,
    dart_disclosure_file_path,
    dart_financial_feature_file_path,
    dart_financial_file_path,
    ensure_project_dirs,
    investor_flow_feature_file_path,
    market_regime_file_path,
    market_regime_report_file_path,
    raw_investor_flow_file_path,
    walk_forward_folds_file_path,
    walk_forward_report_file_path,
    walk_forward_summary_file_path,
)


def test_ensure_project_dirs(tmp_path: Path) -> None:
    ensure_project_dirs(tmp_path)

    for layer in DATA_LAYERS:
        assert (tmp_path / "data" / layer).is_dir()


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
