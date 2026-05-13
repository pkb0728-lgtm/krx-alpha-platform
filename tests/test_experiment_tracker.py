import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from krx_alpha.backtest.simple_backtester import BacktestConfig
from krx_alpha.backtest.walk_forward import WalkForwardConfig
from krx_alpha.experiments.tracker import (
    ExperimentRecord,
    ExperimentTracker,
    build_backtest_experiment_record,
    build_ml_baseline_experiment_record,
    build_walk_forward_experiment_record,
)
from krx_alpha.models.probability_baseline import MLProbabilityBaselineConfig


def test_experiment_tracker_appends_records(tmp_path: Path) -> None:
    tracker = ExperimentTracker(tmp_path)
    record = ExperimentRecord(
        run_id="run-1",
        created_at="2026-05-14T00:00:00+00:00",
        experiment_name="simple_backtest",
        run_type="backtest",
        ticker="005930",
        universe="",
        start_date="2024-01-01",
        end_date="2024-01-31",
        model_name="rule_based_signal",
        model_version="v0",
        params_json="{}",
        metrics_json='{"trade_count": 1}',
        artifact_path="reports/backtest/005930.md",
    )

    tracker.log(record)
    tracker.log(record)
    frame = tracker.load()

    assert tracker.path.exists()
    assert len(frame) == 2
    assert frame.loc[0, "run_id"] == "run-1"


def test_build_backtest_experiment_record_extracts_metrics(tmp_path: Path) -> None:
    metrics = pd.DataFrame(
        {
            "ticker": ["005930"],
            "trade_count": [7],
            "win_rate": [0.5714],
            "average_return": [0.08],
            "cumulative_return": [0.7867],
            "max_drawdown": [-0.1035],
            "sharpe_ratio": [4.33],
            "exposure_count": [8],
        }
    )

    record = build_backtest_experiment_record(
        metrics=metrics,
        config=BacktestConfig(holding_days=3),
        start_date="2024-01-01",
        end_date="2024-01-31",
        artifact_path=tmp_path / "backtest.md",
        created_at=datetime(2026, 5, 14, tzinfo=UTC),
        run_id="fixed",
    )

    params = json.loads(record.params_json)
    logged_metrics = json.loads(record.metrics_json)
    assert record.run_id == "fixed"
    assert record.experiment_name == "simple_backtest"
    assert params["holding_days"] == 3
    assert logged_metrics["trade_count"] == 7
    assert logged_metrics["cumulative_return"] == 0.7867


def test_build_walk_forward_experiment_record_extracts_metrics(tmp_path: Path) -> None:
    summary = pd.DataFrame(
        {
            "ticker": ["005930"],
            "fold_count": [3],
            "total_trade_count": [2],
            "total_exposure_count": [4],
            "average_win_rate": [0.3333],
            "average_return": [0.01],
            "compounded_return": [0.0364],
            "worst_max_drawdown": [-0.052],
            "average_sharpe_ratio": [1.25],
            "positive_fold_ratio": [0.6667],
        }
    )

    record = build_walk_forward_experiment_record(
        summary=summary,
        config=WalkForwardConfig(train_size=20, test_size=5, step_size=5),
        start_date="2024-01-01",
        end_date="2024-01-31",
        artifact_path=tmp_path / "walk_forward.md",
        run_id="fixed",
    )

    params = json.loads(record.params_json)
    logged_metrics = json.loads(record.metrics_json)
    assert record.experiment_name == "walk_forward_validation"
    assert params["train_size"] == 20
    assert logged_metrics["fold_count"] == 3
    assert logged_metrics["positive_fold_ratio"] == 0.6667


def test_build_ml_baseline_experiment_record_extracts_test_metrics(tmp_path: Path) -> None:
    metrics = pd.DataFrame(
        {
            "split": ["train", "test"],
            "row_count": [30, 12],
            "positive_label_rate": [0.6, 0.5],
            "predicted_positive_rate": [0.7, 0.4],
            "accuracy": [0.8, 0.75],
            "precision": [0.81, 0.8],
            "recall": [0.82, 0.6],
            "f1_score": [0.815, 0.6857],
            "roc_auc": [0.9, 0.77],
            "brier_score": [0.12, 0.19],
            "average_probability": [0.64, 0.52],
        }
    )

    record = build_ml_baseline_experiment_record(
        metrics=metrics,
        config=MLProbabilityBaselineConfig(train_fraction=0.7),
        ticker="005930",
        start_date="2024-01-01",
        end_date="2024-03-31",
        artifact_path=tmp_path / "ml_report.md",
        run_id="fixed",
    )

    params = json.loads(record.params_json)
    logged_metrics = json.loads(record.metrics_json)
    assert record.experiment_name == "ml_probability_baseline"
    assert record.run_type == "ml_baseline"
    assert record.model_name == "scorecard_probability_baseline"
    assert params["train_fraction"] == 0.7
    assert logged_metrics["roc_auc"] == 0.77
