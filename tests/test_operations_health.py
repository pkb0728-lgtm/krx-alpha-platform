from pathlib import Path

import pandas as pd

from krx_alpha.monitoring.api_health import API_STATUS_OK, ApiCheckResult
from krx_alpha.monitoring.operations_health import (
    HEALTH_STATUS_EMPTY,
    HEALTH_STATUS_OK,
    HEALTH_STATUS_WARN,
    ArtifactSpec,
    OperationsHealthChecker,
    OperationsHealthConfig,
    format_operations_health_report,
    summarize_operations_health,
)


def test_operations_health_checker_reports_artifact_statuses(tmp_path: Path) -> None:
    signal_dir = tmp_path / "data" / "signals" / "universe_summary_daily"
    signal_dir.mkdir(parents=True)
    signal_path = signal_dir / "universe_20240101_20240131.parquet"
    pd.DataFrame({"ticker": ["005930"], "status": ["success"]}).to_parquet(
        signal_path,
        index=False,
    )

    specs = (
        ArtifactSpec(
            "Universe summary",
            "signals",
            "data/signals/universe_summary_daily/*.parquet",
        ),
        ArtifactSpec(
            "Optional ML metrics",
            "models",
            "data/signals/ml_metrics/*.parquet",
            required=False,
        ),
    )

    result = OperationsHealthChecker(
        project_root=tmp_path,
        config=OperationsHealthConfig(freshness_hours=999999.0),
        artifact_specs=specs,
    ).run(api_results=[ApiCheckResult("OpenDART", API_STATUS_OK, "reachable")])

    statuses = dict(zip(result["check_name"], result["status"], strict=True))
    assert statuses["Universe summary"] == HEALTH_STATUS_OK
    assert statuses["Optional ML metrics"] == HEALTH_STATUS_WARN
    assert statuses["OpenDART"] == HEALTH_STATUS_OK

    summary = summarize_operations_health(result)
    assert summary == {"total": 3, "ok": 2, "warnings": 1, "problems": 0}

    report = format_operations_health_report(result)
    assert "Operations Health Report" in report
    assert "Optional ML metrics" in report


def test_operations_health_checker_flags_empty_required_artifact(tmp_path: Path) -> None:
    feature_dir = tmp_path / "data" / "features" / "prices_daily"
    feature_dir.mkdir(parents=True)
    pd.DataFrame(columns=["date", "ticker"]).to_parquet(
        feature_dir / "005930_20240101_20240131.parquet",
        index=False,
    )

    result = OperationsHealthChecker(
        project_root=tmp_path,
        config=OperationsHealthConfig(freshness_hours=999999.0),
        artifact_specs=(
            ArtifactSpec(
                "Price features",
                "features",
                "data/features/prices_daily/*.parquet",
            ),
        ),
    ).run()

    assert result.loc[0, "status"] == HEALTH_STATUS_EMPTY
    assert summarize_operations_health(result)["problems"] == 1
