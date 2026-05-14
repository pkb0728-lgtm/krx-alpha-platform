from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from krx_alpha.monitoring.api_health import (
    API_STATUS_FAILED,
    API_STATUS_MISSING,
    API_STATUS_OK,
    ApiCheckResult,
)

HEALTH_STATUS_OK = "OK"
HEALTH_STATUS_WARN = "WARN"
HEALTH_STATUS_MISSING = "MISSING"
HEALTH_STATUS_EMPTY = "EMPTY"
HEALTH_STATUS_STALE = "STALE"
HEALTH_STATUS_FAILED = "FAILED"

OPERATIONS_HEALTH_COLUMNS = [
    "check_name",
    "category",
    "status",
    "severity",
    "path",
    "row_count",
    "modified_at",
    "age_hours",
    "detail",
    "action",
]


@dataclass(frozen=True)
class OperationsHealthConfig:
    freshness_hours: float = 36.0


@dataclass(frozen=True)
class ArtifactSpec:
    check_name: str
    category: str
    relative_glob: str
    required: bool = True
    min_rows: int | None = 1
    freshness_required: bool = True


DEFAULT_ARTIFACT_SPECS = (
    ArtifactSpec("Universe summary", "signals", "data/signals/universe_summary_daily/*.parquet"),
    ArtifactSpec("Final signals", "signals", "data/signals/final_signals_daily/*.parquet"),
    ArtifactSpec("Price features", "features", "data/features/prices_daily/*.parquet"),
    ArtifactSpec("Daily report", "reports", "reports/daily/*.md", required=False, min_rows=None),
    ArtifactSpec(
        "Universe report",
        "reports",
        "reports/universe/*.md",
        required=False,
        min_rows=None,
    ),
    ArtifactSpec(
        "Auto screener result",
        "signals",
        "data/signals/screening_daily/*.parquet",
        required=False,
    ),
    ArtifactSpec("Backtest metrics", "backtest", "data/backtest/metrics/*.parquet", required=False),
    ArtifactSpec(
        "Walk-forward summary",
        "backtest",
        "data/backtest/walk_forward_summary/*.parquet",
        required=False,
    ),
    ArtifactSpec(
        "Paper portfolio summary",
        "paper",
        "data/backtest/paper_portfolio_summary/*.parquet",
        required=False,
    ),
    ArtifactSpec(
        "Paper portfolio ledger",
        "paper",
        "data/backtest/paper_portfolio_trade_ledger/*.parquet",
        required=False,
        min_rows=0,
    ),
    ArtifactSpec(
        "ML metrics",
        "models",
        "data/signals/ml_metrics/*.parquet",
        required=False,
    ),
    ArtifactSpec(
        "News sentiment features",
        "features",
        "data/features/news_sentiment_daily/*.parquet",
        required=False,
    ),
    ArtifactSpec(
        "Macro features",
        "features",
        "data/features/macro_fred_daily/*.parquet",
        required=False,
    ),
    ArtifactSpec("Drift result", "monitoring", "data/signals/drift/*.parquet", required=False),
    ArtifactSpec(
        "Experiment log",
        "monitoring",
        "experiments/experiment_log.csv",
        required=False,
    ),
)


class OperationsHealthChecker:
    """Check whether recent local pipeline artifacts are present and readable."""

    def __init__(
        self,
        project_root: Path,
        config: OperationsHealthConfig | None = None,
        artifact_specs: tuple[ArtifactSpec, ...] = DEFAULT_ARTIFACT_SPECS,
        current_time: pd.Timestamp | None = None,
    ) -> None:
        self.project_root = project_root
        self.config = config or OperationsHealthConfig()
        self.artifact_specs = artifact_specs
        self.current_time = _as_utc_timestamp(current_time or pd.Timestamp.now(tz="UTC"))

    def run(self, api_results: list[ApiCheckResult] | None = None) -> Any:
        rows = [self._check_artifact(spec) for spec in self.artifact_specs]
        rows.extend(_api_result_to_row(result) for result in api_results or [])
        return pd.DataFrame(rows, columns=OPERATIONS_HEALTH_COLUMNS)

    def _check_artifact(self, spec: ArtifactSpec) -> dict[str, object]:
        latest_path = _latest_file(self.project_root, spec.relative_glob)
        if latest_path is None:
            status = HEALTH_STATUS_MISSING if spec.required else HEALTH_STATUS_WARN
            detail = (
                "required artifact is missing" if spec.required else "optional artifact not found"
            )
            return _health_row(
                check_name=spec.check_name,
                category=spec.category,
                status=status,
                path="",
                row_count=None,
                modified_at=None,
                age_hours=None,
                detail=detail,
            )

        try:
            row_count = _read_row_count(latest_path)
        except Exception as exc:
            return _health_row(
                check_name=spec.check_name,
                category=spec.category,
                status=HEALTH_STATUS_FAILED,
                path=str(latest_path),
                row_count=None,
                modified_at=None,
                age_hours=None,
                detail=f"failed to read artifact: {exc}",
            )

        modified_at = _as_utc_timestamp(pd.Timestamp(latest_path.stat().st_mtime, unit="s"))
        age_hours = (self.current_time - modified_at).total_seconds() / 3600
        status = HEALTH_STATUS_OK
        detail = "artifact is present and readable"

        if spec.min_rows is not None and row_count < spec.min_rows:
            status = HEALTH_STATUS_EMPTY
            detail = f"row_count {row_count} is below minimum {spec.min_rows}"
        elif (
            spec.freshness_required
            and self.config.freshness_hours > 0
            and age_hours > self.config.freshness_hours
        ):
            status = HEALTH_STATUS_STALE
            detail = f"artifact is older than {self.config.freshness_hours:.1f} hour(s)"

        return _health_row(
            check_name=spec.check_name,
            category=spec.category,
            status=status,
            path=str(latest_path),
            row_count=row_count,
            modified_at=modified_at.isoformat(),
            age_hours=age_hours,
            detail=detail,
        )


def summarize_operations_health(result_frame: Any) -> dict[str, int]:
    if result_frame.empty:
        return {
            "total": 0,
            "ok": 0,
            "warnings": 0,
            "problems": 0,
        }

    statuses = result_frame["status"].astype(str)
    warning_statuses = {HEALTH_STATUS_WARN, HEALTH_STATUS_STALE}
    problem_statuses = {HEALTH_STATUS_MISSING, HEALTH_STATUS_EMPTY, HEALTH_STATUS_FAILED}
    return {
        "total": int(len(result_frame)),
        "ok": int((statuses == HEALTH_STATUS_OK).sum()),
        "warnings": int(statuses.isin(warning_statuses).sum()),
        "problems": int(statuses.isin(problem_statuses).sum()),
    }


def format_operations_health_report(result_frame: Any) -> str:
    summary = summarize_operations_health(result_frame)
    rows = [
        "# Operations Health Report",
        "",
        f"- Total checks: {summary['total']}",
        f"- OK: {summary['ok']}",
        f"- Warnings: {summary['warnings']}",
        f"- Problems: {summary['problems']}",
        "",
        "## Checks",
        "",
        "| Check | Category | Status | Rows | Age Hours | Detail |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]
    for _, row in result_frame.iterrows():
        detail = str(row["detail"])
        action = str(row.get("action", "")).strip()
        detail_with_action = f"{detail}; action: {action}" if action else detail
        rows.append(
            "| "
            f"{row['check_name']} | "
            f"{row['category']} | "
            f"{row['status']} | "
            f"{_format_optional_int(row['row_count'])} | "
            f"{_format_optional_float(row['age_hours'])} | "
            f"{detail_with_action} |"
        )

    rows.extend(
        [
            "",
            "## Reading Guide",
            "",
            "- `OK` means the latest local artifact exists and can be read.",
            "- `WARN` usually means an optional artifact has not been generated yet.",
            "- `STALE` means the latest artifact is older than the configured freshness window.",
            "- `MISSING`, `EMPTY`, and `FAILED` should be reviewed before relying on the pipeline.",
            "- This report checks platform operations only. It is not investment advice.",
            "",
        ]
    )
    return "\n".join(rows)


def _latest_file(project_root: Path, relative_glob: str) -> Path | None:
    files = [
        path
        for path in project_root.glob(relative_glob)
        if path.is_file() and not path.name.startswith(".")
    ]
    if not files:
        return None
    return max(files, key=lambda path: path.stat().st_mtime)


def _read_row_count(path: Path) -> int:
    if path.suffix == ".parquet":
        return int(len(pd.read_parquet(path)))
    if path.suffix == ".csv":
        return int(len(pd.read_csv(path)))
    if path.suffix in {".md", ".txt"}:
        text = path.read_text(encoding="utf-8")
        return 1 if text.strip() else 0
    return 1 if path.stat().st_size > 0 else 0


def _api_result_to_row(result: ApiCheckResult) -> dict[str, object]:
    status = result.status
    if status not in {API_STATUS_OK, API_STATUS_MISSING, API_STATUS_FAILED}:
        status = HEALTH_STATUS_FAILED
    return _health_row(
        check_name=result.name,
        category="api",
        status=status,
        path="",
        row_count=None,
        modified_at=None,
        age_hours=None,
        detail=result.detail,
        action=result.action,
    )


def _health_row(
    *,
    check_name: str,
    category: str,
    status: str,
    path: str,
    row_count: int | None,
    modified_at: str | None,
    age_hours: float | None,
    detail: str,
    action: str = "",
) -> dict[str, object]:
    return {
        "check_name": check_name,
        "category": category,
        "status": status,
        "severity": _status_severity(status),
        "path": path,
        "row_count": row_count,
        "modified_at": modified_at,
        "age_hours": age_hours,
        "detail": detail,
        "action": action,
    }


def _status_severity(status: str) -> int:
    if status == HEALTH_STATUS_OK:
        return 0
    if status in {HEALTH_STATUS_WARN, HEALTH_STATUS_STALE}:
        return 1
    if status in {HEALTH_STATUS_MISSING, HEALTH_STATUS_EMPTY}:
        return 2
    return 3


def _as_utc_timestamp(value: pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _format_optional_int(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(int(cast(Any, value)))


def _format_optional_float(value: object) -> str:
    if pd.isna(value):
        return ""
    return f"{float(cast(Any, value)):.2f}"
