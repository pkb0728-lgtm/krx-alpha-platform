import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pandas as pd

from krx_alpha.backtest.simple_backtester import BacktestConfig
from krx_alpha.backtest.walk_forward import WalkForwardConfig
from krx_alpha.database.storage import experiment_log_file_path
from krx_alpha.models.probability_baseline import (
    ML_PROBABILITY_BASELINE_MODEL_NAME,
    ML_PROBABILITY_BASELINE_MODEL_VERSION,
    MLProbabilityBaselineConfig,
)

EXPERIMENT_COLUMNS = [
    "run_id",
    "created_at",
    "experiment_name",
    "run_type",
    "ticker",
    "universe",
    "start_date",
    "end_date",
    "model_name",
    "model_version",
    "params_json",
    "metrics_json",
    "artifact_path",
    "notes",
]

DEFAULT_MODEL_NAME = "rule_based_signal"
DEFAULT_MODEL_VERSION = "v0"


@dataclass(frozen=True)
class ExperimentRecord:
    run_id: str
    created_at: str
    experiment_name: str
    run_type: str
    ticker: str
    universe: str
    start_date: str
    end_date: str
    model_name: str
    model_version: str
    params_json: str
    metrics_json: str
    artifact_path: str
    notes: str = ""


class ExperimentTracker:
    """Append lightweight experiment metadata to a local CSV log."""

    def __init__(self, project_root: Path) -> None:
        self.path = experiment_log_file_path(project_root)

    def log(self, record: ExperimentRecord) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record_frame = pd.DataFrame([asdict(record)], columns=EXPERIMENT_COLUMNS)
        if self.path.exists():
            record_frame.to_csv(
                self.path,
                mode="a",
                header=False,
                index=False,
                encoding="utf-8-sig",
            )
        else:
            record_frame.to_csv(self.path, index=False, encoding="utf-8-sig")
        return self.path

    def load(self) -> Any:
        if not self.path.exists():
            return pd.DataFrame(columns=EXPERIMENT_COLUMNS)
        return pd.read_csv(self.path)


def build_backtest_experiment_record(
    metrics: Any,
    config: BacktestConfig,
    start_date: str,
    end_date: str,
    artifact_path: Path,
    created_at: datetime | None = None,
    run_id: str | None = None,
) -> ExperimentRecord:
    metric = metrics.iloc[0]
    params = {
        "holding_days": config.holding_days,
        "transaction_cost_bps": config.transaction_cost_bps,
        "slippage_bps": config.slippage_bps,
        "tradable_action": config.tradable_action,
    }
    metric_values = {
        "trade_count": int(metric["trade_count"]),
        "exposure_count": int(metric["exposure_count"]),
        "win_rate": float(metric["win_rate"]),
        "average_return": float(metric["average_return"]),
        "cumulative_return": float(metric["cumulative_return"]),
        "max_drawdown": float(metric["max_drawdown"]),
        "sharpe_ratio": float(metric["sharpe_ratio"]),
    }
    return _build_record(
        experiment_name="simple_backtest",
        run_type="backtest",
        ticker=str(metric["ticker"]),
        universe="",
        start_date=start_date,
        end_date=end_date,
        params=params,
        metrics=metric_values,
        artifact_path=artifact_path,
        created_at=created_at,
        run_id=run_id,
    )


def build_walk_forward_experiment_record(
    summary: Any,
    config: WalkForwardConfig,
    start_date: str,
    end_date: str,
    artifact_path: Path,
    created_at: datetime | None = None,
    run_id: str | None = None,
) -> ExperimentRecord:
    metric = summary.iloc[0]
    params = {
        "train_size": config.train_size,
        "test_size": config.test_size,
        "step_size": config.step_size,
        "holding_days": config.holding_days,
        "transaction_cost_bps": config.transaction_cost_bps,
        "slippage_bps": config.slippage_bps,
    }
    metric_values = {
        "fold_count": int(metric["fold_count"]),
        "total_trade_count": int(metric["total_trade_count"]),
        "total_exposure_count": int(metric["total_exposure_count"]),
        "average_win_rate": float(metric["average_win_rate"]),
        "average_return": float(metric["average_return"]),
        "compounded_return": float(metric["compounded_return"]),
        "worst_max_drawdown": float(metric["worst_max_drawdown"]),
        "average_sharpe_ratio": float(metric["average_sharpe_ratio"]),
        "positive_fold_ratio": float(metric["positive_fold_ratio"]),
    }
    return _build_record(
        experiment_name="walk_forward_validation",
        run_type="walk_forward",
        ticker=str(metric["ticker"]),
        universe="",
        start_date=start_date,
        end_date=end_date,
        params=params,
        metrics=metric_values,
        artifact_path=artifact_path,
        created_at=created_at,
        run_id=run_id,
    )


def build_daily_job_experiment_record(
    universe: str,
    start_date: str,
    end_date: str,
    total_count: int,
    success_count: int,
    failed_count: int,
    report_path: Path,
    telegram_sent: bool,
    telegram_dry_run: bool,
    paper_trade_enabled: bool = False,
    paper_trade_count: int = 0,
    paper_cumulative_return: float = 0.0,
    paper_summary_path: Path | None = None,
    screening_enabled: bool = False,
    screening_checked_count: int = 0,
    screening_passed_count: int = 0,
    screening_result_path: Path | None = None,
    created_at: datetime | None = None,
    run_id: str | None = None,
) -> ExperimentRecord:
    params = {
        "telegram_sent": telegram_sent,
        "telegram_dry_run": telegram_dry_run,
        "paper_trade_enabled": paper_trade_enabled,
        "paper_summary_path": str(paper_summary_path) if paper_summary_path else "",
        "screening_enabled": screening_enabled,
        "screening_result_path": str(screening_result_path) if screening_result_path else "",
    }
    metrics = {
        "total_count": total_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "success_rate": success_count / total_count if total_count else 0.0,
        "paper_trade_count": paper_trade_count,
        "paper_cumulative_return": paper_cumulative_return,
        "screening_checked_count": screening_checked_count,
        "screening_passed_count": screening_passed_count,
        "screening_pass_rate": screening_passed_count / screening_checked_count
        if screening_checked_count
        else 0.0,
    }
    return _build_record(
        experiment_name="daily_job",
        run_type="operations",
        ticker="",
        universe=universe,
        start_date=start_date,
        end_date=end_date,
        params=params,
        metrics=metrics,
        artifact_path=report_path,
        created_at=created_at,
        run_id=run_id,
    )


def build_ml_baseline_experiment_record(
    metrics: Any,
    config: MLProbabilityBaselineConfig,
    ticker: str,
    start_date: str,
    end_date: str,
    artifact_path: Path,
    created_at: datetime | None = None,
    run_id: str | None = None,
) -> ExperimentRecord:
    test_metric = metrics[metrics["split"] == "test"].iloc[0]
    params = asdict(config)
    metric_values = {
        "row_count": int(test_metric["row_count"]),
        "positive_label_rate": float(test_metric["positive_label_rate"]),
        "predicted_positive_rate": float(test_metric["predicted_positive_rate"]),
        "accuracy": float(test_metric["accuracy"]),
        "precision": float(test_metric["precision"]),
        "recall": float(test_metric["recall"]),
        "f1_score": float(test_metric["f1_score"]),
        "roc_auc": float(test_metric["roc_auc"]),
        "brier_score": float(test_metric["brier_score"]),
    }
    return _build_record(
        experiment_name="ml_probability_baseline",
        run_type="ml_baseline",
        ticker=ticker,
        universe="",
        start_date=start_date,
        end_date=end_date,
        params=params,
        metrics=metric_values,
        artifact_path=artifact_path,
        created_at=created_at,
        run_id=run_id,
        model_name=ML_PROBABILITY_BASELINE_MODEL_NAME,
        model_version=ML_PROBABILITY_BASELINE_MODEL_VERSION,
    )


def _build_record(
    experiment_name: str,
    run_type: str,
    ticker: str,
    universe: str,
    start_date: str,
    end_date: str,
    params: Mapping[str, object],
    metrics: Mapping[str, object],
    artifact_path: Path,
    created_at: datetime | None,
    run_id: str | None,
    model_name: str = DEFAULT_MODEL_NAME,
    model_version: str = DEFAULT_MODEL_VERSION,
) -> ExperimentRecord:
    timestamp = created_at or datetime.now(UTC)
    return ExperimentRecord(
        run_id=run_id or _new_run_id(experiment_name, timestamp),
        created_at=timestamp.isoformat(),
        experiment_name=experiment_name,
        run_type=run_type,
        ticker=ticker,
        universe=universe,
        start_date=start_date,
        end_date=end_date,
        model_name=model_name,
        model_version=model_version,
        params_json=_to_json(params),
        metrics_json=_to_json(metrics),
        artifact_path=str(artifact_path),
    )


def _new_run_id(experiment_name: str, created_at: datetime) -> str:
    timestamp = created_at.strftime("%Y%m%dT%H%M%S")
    return f"{experiment_name}_{timestamp}_{uuid4().hex[:8]}"


def _to_json(value: Mapping[str, object]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
