from pathlib import Path

from krx_alpha.database.storage import (
    DATA_LAYERS,
    ensure_project_dirs,
    market_regime_file_path,
    market_regime_report_file_path,
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
