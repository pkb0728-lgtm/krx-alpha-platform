import json
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

DEFAULT_EXCLUDED_COLUMNS = {
    "date",
    "as_of_date",
    "ticker",
    "source",
    "collected_at",
    "processed_at",
    "generated_at",
}

HIGHER_IS_BETTER_METRICS = {
    "accuracy",
    "precision",
    "recall",
    "f1_score",
    "roc_auc",
    "win_rate",
    "average_return",
    "cumulative_return",
    "compounded_return",
    "sharpe_ratio",
    "average_sharpe_ratio",
    "positive_fold_ratio",
    "success_rate",
}

LOWER_IS_BETTER_METRICS = {
    "max_drawdown",
    "worst_max_drawdown",
}

DRIFT_RESULT_COLUMNS = [
    "feature",
    "reference_count",
    "current_count",
    "reference_mean",
    "current_mean",
    "mean_shift_score",
    "reference_std",
    "current_std",
    "std_ratio",
    "reference_missing_rate",
    "current_missing_rate",
    "missing_rate_delta",
    "drift_detected",
    "drift_reason",
]

PERFORMANCE_DRIFT_COLUMNS = [
    "run_type",
    "metric",
    "direction",
    "baseline_window",
    "recent_window",
    "baseline_mean",
    "recent_mean",
    "absolute_change",
    "relative_change",
    "drift_detected",
    "drift_reason",
]


@dataclass(frozen=True)
class DataDriftConfig:
    mean_shift_threshold: float = 1.0
    std_ratio_upper_threshold: float = 2.0
    std_ratio_lower_threshold: float = 0.5
    missing_rate_delta_threshold: float = 0.2


@dataclass(frozen=True)
class PerformanceDriftConfig:
    run_type: str = "backtest"
    metric: str = "cumulative_return"
    baseline_window: int = 5
    recent_window: int = 3
    absolute_change_threshold: float = 0.05
    relative_change_threshold: float = 0.3


class DataDriftDetector:
    """Compare numeric feature distributions between reference and current datasets."""

    def __init__(self, config: DataDriftConfig | None = None) -> None:
        self.config = config or DataDriftConfig()

    def detect(
        self,
        reference_frame: Any,
        current_frame: Any,
        columns: list[str] | None = None,
    ) -> Any:
        selected_columns = columns or _common_numeric_columns(reference_frame, current_frame)
        rows = [
            self._build_row(reference_frame, current_frame, column)
            for column in selected_columns
            if column in reference_frame.columns and column in current_frame.columns
        ]
        return pd.DataFrame(rows, columns=DRIFT_RESULT_COLUMNS)

    def _build_row(
        self, reference_frame: Any, current_frame: Any, column: str
    ) -> dict[str, object]:
        reference = pd.to_numeric(reference_frame[column], errors="coerce")
        current = pd.to_numeric(current_frame[column], errors="coerce")
        reference_mean = float(reference.mean()) if not reference.dropna().empty else 0.0
        current_mean = float(current.mean()) if not current.dropna().empty else 0.0
        reference_std = _safe_std(reference)
        current_std = _safe_std(current)
        mean_shift_score = abs(current_mean - reference_mean) / max(reference_std, 1e-9)
        std_ratio = current_std / max(reference_std, 1e-9)
        reference_missing_rate = float(reference.isna().mean())
        current_missing_rate = float(current.isna().mean())
        missing_rate_delta = abs(current_missing_rate - reference_missing_rate)

        reasons = _data_drift_reasons(
            mean_shift_score=mean_shift_score,
            std_ratio=std_ratio,
            missing_rate_delta=missing_rate_delta,
            config=self.config,
        )
        return {
            "feature": column,
            "reference_count": int(reference.notna().sum()),
            "current_count": int(current.notna().sum()),
            "reference_mean": reference_mean,
            "current_mean": current_mean,
            "mean_shift_score": mean_shift_score,
            "reference_std": reference_std,
            "current_std": current_std,
            "std_ratio": std_ratio,
            "reference_missing_rate": reference_missing_rate,
            "current_missing_rate": current_missing_rate,
            "missing_rate_delta": missing_rate_delta,
            "drift_detected": bool(reasons),
            "drift_reason": ",".join(reasons) if reasons else "stable",
        }


class PerformanceDriftDetector:
    """Compare baseline and recent experiment metrics from the local experiment log."""

    def __init__(self, config: PerformanceDriftConfig | None = None) -> None:
        self.config = config or PerformanceDriftConfig()

    def detect(self, experiment_frame: Any) -> Any:
        filtered = experiment_frame[experiment_frame["run_type"] == self.config.run_type].copy()
        if filtered.empty:
            return _performance_result(
                config=self.config,
                baseline_mean=0.0,
                recent_mean=0.0,
                absolute_change=0.0,
                relative_change=0.0,
                drift_detected=False,
                drift_reason="insufficient_history",
            )

        filtered["metric_value"] = filtered["metrics_json"].apply(
            lambda value: _metric_from_json(value, self.config.metric)
        )
        filtered = filtered.dropna(subset=["metric_value"]).reset_index(drop=True)
        needed = self.config.baseline_window + self.config.recent_window
        if len(filtered) < needed:
            return _performance_result(
                config=self.config,
                baseline_mean=0.0,
                recent_mean=0.0,
                absolute_change=0.0,
                relative_change=0.0,
                drift_detected=False,
                drift_reason="insufficient_history",
            )

        baseline = filtered.iloc[-needed : -self.config.recent_window]["metric_value"].astype(float)
        recent = filtered.iloc[-self.config.recent_window :]["metric_value"].astype(float)
        baseline_mean = float(baseline.mean())
        recent_mean = float(recent.mean())
        absolute_change = recent_mean - baseline_mean
        relative_change = absolute_change / max(abs(baseline_mean), 1e-9)
        direction = metric_direction(self.config.metric)
        drift_detected, reason = _performance_drift_status(
            direction=direction,
            absolute_change=absolute_change,
            relative_change=relative_change,
            config=self.config,
        )

        return _performance_result(
            config=self.config,
            baseline_mean=baseline_mean,
            recent_mean=recent_mean,
            absolute_change=absolute_change,
            relative_change=relative_change,
            drift_detected=drift_detected,
            drift_reason=reason,
        )


def format_data_drift_report(result_frame: Any, title: str = "Data Drift Report") -> str:
    drift_count = int(result_frame["drift_detected"].sum()) if not result_frame.empty else 0
    rows = [
        f"# {title}",
        "",
        f"- Checked features: {len(result_frame)}",
        f"- Drifted features: {drift_count}",
        "",
        "## Feature Results",
        "",
        "| Feature | Mean Shift | Std Ratio | Missing Delta | Drift | Reason |",
        "| --- | ---: | ---: | ---: | --- | --- |",
    ]
    for _, row in result_frame.iterrows():
        rows.append(
            "| "
            f"{row['feature']} | "
            f"{float(row['mean_shift_score']):.2f} | "
            f"{float(row['std_ratio']):.2f} | "
            f"{float(row['missing_rate_delta']):.2%} | "
            f"{bool(row['drift_detected'])} | "
            f"{row['drift_reason']} |"
        )
    rows.extend(
        [
            "",
            "## Reading Guide",
            "",
            "- Mean shift uses reference standard deviation as the scale.",
            "- Std ratio compares current volatility with reference volatility.",
            "- Missing delta compares missing data rates.",
            "- This MVP is a monitoring signal, not an automatic trading decision.",
            "",
        ]
    )
    return "\n".join(rows)


def format_performance_drift_report(result_frame: Any) -> str:
    row = result_frame.iloc[0]
    return "\n".join(
        [
            "# Performance Drift Report",
            "",
            f"- Run type: {row['run_type']}",
            f"- Metric: {row['metric']}",
            f"- Direction: {row['direction']}",
            f"- Baseline window: {int(row['baseline_window'])}",
            f"- Recent window: {int(row['recent_window'])}",
            f"- Baseline mean: {float(row['baseline_mean']):.4f}",
            f"- Recent mean: {float(row['recent_mean']):.4f}",
            f"- Absolute change: {float(row['absolute_change']):.4f}",
            f"- Relative change: {float(row['relative_change']):.2%}",
            f"- Drift detected: {bool(row['drift_detected'])}",
            f"- Reason: {row['drift_reason']}",
            "",
            "## Reading Guide",
            "",
            "This report compares recent experiment metrics with an earlier baseline window.",
            "Use it as an operations warning, then inspect the underlying trades and data.",
            "",
        ]
    )


def metric_direction(metric: str) -> str:
    if metric in LOWER_IS_BETTER_METRICS:
        return "lower_is_better"
    if metric in HIGHER_IS_BETTER_METRICS:
        return "higher_is_better"
    return "higher_is_better"


def _common_numeric_columns(reference_frame: Any, current_frame: Any) -> list[str]:
    common_columns = [
        column
        for column in reference_frame.columns
        if column in current_frame.columns and column not in DEFAULT_EXCLUDED_COLUMNS
    ]
    return [
        column
        for column in common_columns
        if pd.api.types.is_numeric_dtype(reference_frame[column])
        or pd.api.types.is_numeric_dtype(current_frame[column])
    ]


def _safe_std(series: Any) -> float:
    clean = series.dropna()
    if len(clean) < 2:
        return 0.0
    value = float(clean.std(ddof=1))
    return value if np.isfinite(value) else 0.0


def _data_drift_reasons(
    mean_shift_score: float,
    std_ratio: float,
    missing_rate_delta: float,
    config: DataDriftConfig,
) -> list[str]:
    reasons: list[str] = []
    if mean_shift_score > config.mean_shift_threshold:
        reasons.append("mean_shift")
    if std_ratio > config.std_ratio_upper_threshold:
        reasons.append("std_increase")
    if std_ratio < config.std_ratio_lower_threshold:
        reasons.append("std_decrease")
    if missing_rate_delta > config.missing_rate_delta_threshold:
        reasons.append("missing_rate_change")
    return reasons


def _metric_from_json(value: Any, metric: str) -> float | None:
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return None
    metric_value = parsed.get(metric)
    return float(metric_value) if metric_value is not None else None


def _performance_drift_status(
    direction: str,
    absolute_change: float,
    relative_change: float,
    config: PerformanceDriftConfig,
) -> tuple[bool, str]:
    if direction == "lower_is_better":
        worsened = absolute_change > config.absolute_change_threshold or (
            relative_change > config.relative_change_threshold
        )
        return (True, "metric_increase") if worsened else (False, "stable")

    worsened = absolute_change < -config.absolute_change_threshold or (
        relative_change < -config.relative_change_threshold
    )
    return (True, "metric_decrease") if worsened else (False, "stable")


def _performance_result(
    config: PerformanceDriftConfig,
    baseline_mean: float,
    recent_mean: float,
    absolute_change: float,
    relative_change: float,
    drift_detected: bool,
    drift_reason: str,
) -> Any:
    return pd.DataFrame(
        [
            {
                "run_type": config.run_type,
                "metric": config.metric,
                "direction": metric_direction(config.metric),
                "baseline_window": config.baseline_window,
                "recent_window": config.recent_window,
                "baseline_mean": baseline_mean,
                "recent_mean": recent_mean,
                "absolute_change": absolute_change,
                "relative_change": relative_change,
                "drift_detected": drift_detected,
                "drift_reason": drift_reason,
            }
        ],
        columns=PERFORMANCE_DRIFT_COLUMNS,
    )
