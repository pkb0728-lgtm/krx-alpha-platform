import json

import pandas as pd

from krx_alpha.monitoring.drift import (
    DataDriftConfig,
    DataDriftDetector,
    PerformanceDriftConfig,
    PerformanceDriftDetector,
    format_data_drift_report,
    format_performance_drift_report,
)


def test_data_drift_detector_flags_mean_shift() -> None:
    reference = pd.DataFrame(
        {
            "ticker": ["005930"] * 5,
            "rsi_14": [40.0, 41.0, 42.0, 43.0, 44.0],
            "volatility_5d": [0.01, 0.011, 0.012, 0.013, 0.014],
        }
    )
    current = pd.DataFrame(
        {
            "ticker": ["005930"] * 5,
            "rsi_14": [70.0, 71.0, 72.0, 73.0, 74.0],
            "volatility_5d": [0.011, 0.012, 0.013, 0.014, 0.015],
        }
    )

    result = DataDriftDetector(DataDriftConfig(mean_shift_threshold=1.0)).detect(
        reference,
        current,
    )

    drifted = result[result["feature"] == "rsi_14"].iloc[0]
    stable = result[result["feature"] == "volatility_5d"].iloc[0]
    assert bool(drifted["drift_detected"]) is True
    assert "mean_shift" in str(drifted["drift_reason"])
    assert bool(stable["drift_detected"]) is False


def test_data_drift_report_contains_summary() -> None:
    result = pd.DataFrame(
        {
            "feature": ["rsi_14"],
            "reference_count": [5],
            "current_count": [5],
            "reference_mean": [40.0],
            "current_mean": [70.0],
            "mean_shift_score": [10.0],
            "reference_std": [1.0],
            "current_std": [1.0],
            "std_ratio": [1.0],
            "reference_missing_rate": [0.0],
            "current_missing_rate": [0.0],
            "missing_rate_delta": [0.0],
            "drift_detected": [True],
            "drift_reason": ["mean_shift"],
        }
    )

    report = format_data_drift_report(result)

    assert "Data Drift Report" in report
    assert "Drifted features: 1" in report


def test_performance_drift_detector_flags_metric_decrease() -> None:
    rows = []
    for value in [0.2, 0.22, 0.21, 0.01, 0.0]:
        rows.append(
            {
                "run_type": "backtest",
                "metrics_json": json.dumps({"cumulative_return": value}),
            }
        )
    frame = pd.DataFrame(rows)

    result = PerformanceDriftDetector(
        PerformanceDriftConfig(
            run_type="backtest",
            metric="cumulative_return",
            baseline_window=3,
            recent_window=2,
            absolute_change_threshold=0.05,
        )
    ).detect(frame)

    row = result.iloc[0]
    assert bool(row["drift_detected"]) is True
    assert row["drift_reason"] == "metric_decrease"
    assert float(row["recent_mean"]) < float(row["baseline_mean"])


def test_performance_drift_detector_handles_insufficient_history() -> None:
    frame = pd.DataFrame(
        [{"run_type": "backtest", "metrics_json": json.dumps({"cumulative_return": 0.1})}]
    )

    result = PerformanceDriftDetector(
        PerformanceDriftConfig(baseline_window=3, recent_window=2)
    ).detect(frame)

    row = result.iloc[0]
    assert bool(row["drift_detected"]) is False
    assert row["drift_reason"] == "insufficient_history"
    assert "Performance Drift Report" in format_performance_drift_report(result)
