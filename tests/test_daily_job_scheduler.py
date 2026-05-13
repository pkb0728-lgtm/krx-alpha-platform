from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from krx_alpha.database.storage import (
    drift_result_file_path,
    universe_summary_csv_path,
    universe_summary_file_path,
    write_csv,
    write_parquet,
)
from krx_alpha.pipelines.universe_pipeline import UniversePipelineResult
from krx_alpha.scheduler.daily_job import (
    DailyJobConfig,
    DailyJobRunner,
    resolve_daily_job_date_range,
)
from krx_alpha.telegram.notifier import TelegramSendResult


class FakeUniversePipeline:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root
        self.calls: list[dict[str, Any]] = []

    def run(self, tickers: list[str], start_date: str, end_date: str) -> UniversePipelineResult:
        self.calls.append(
            {
                "tickers": tickers,
                "start_date": start_date,
                "end_date": end_date,
            }
        )
        frame = pd.DataFrame(
            {
                "ticker": ["005930", "005380"],
                "status": ["success", "success"],
                "latest_action": ["watch", "buy_candidate"],
                "latest_confidence_score": [63.78, 72.83],
                "latest_financial_score": [50.0, 80.0],
                "latest_event_score": [50.0, 55.0],
                "latest_flow_score": [85.0, 70.0],
                "latest_market_regime": ["neutral", "bull"],
                "signal_path": ["signal-a.parquet", "signal-b.parquet"],
                "report_path": ["report-a.md", "report-b.md"],
                "error": ["", ""],
            }
        )
        start_compact = start_date.replace("-", "")
        end_compact = end_date.replace("-", "")
        summary_path = universe_summary_file_path(self.project_root, start_compact, end_compact)
        summary_csv_path = universe_summary_csv_path(
            self.project_root,
            start_compact,
            end_compact,
        )
        write_parquet(frame, summary_path)
        write_csv(frame, summary_csv_path)
        write_parquet(
            pd.DataFrame(
                {
                    "feature": ["rsi_14"],
                    "drift_detected": [True],
                    "drift_reason": ["mean_shift"],
                }
            ),
            drift_result_file_path(self.project_root, "latest_drift"),
        )
        return UniversePipelineResult(
            summary_path=summary_path,
            summary_csv_path=summary_csv_path,
            total_count=2,
            success_count=2,
            failed_count=0,
        )


class FakeTelegramSender:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def send_message(self, message: str, dry_run: bool = False) -> TelegramSendResult:
        self.messages.append(message)
        return TelegramSendResult(
            sent=not dry_run,
            dry_run=dry_run,
            status_code=None if dry_run else 200,
            message=message,
        )


def test_daily_job_runner_creates_summary_report_and_telegram_preview(tmp_path: Path) -> None:
    pipeline = FakeUniversePipeline(tmp_path)
    sender = FakeTelegramSender()
    runner = DailyJobRunner(
        project_root=tmp_path,
        universe_pipeline=pipeline,  # type: ignore[arg-type]
        telegram_sender=sender,
    )

    result = runner.run(
        DailyJobConfig(
            universe="demo",
            start_date="2024-01-01",
            end_date="2024-01-31",
            telegram_dry_run=True,
        )
    )

    assert pipeline.calls[0]["start_date"] == "2024-01-01"
    assert pipeline.calls[0]["end_date"] == "2024-01-31"
    assert result.summary_path.exists()
    assert result.summary_csv_path.exists()
    assert result.report_path.exists()
    assert result.experiment_log_path.exists()
    assert result.telegram_sent is False
    assert result.telegram_dry_run is True
    assert "005380 | buy_candidate" in result.telegram_message
    assert "Data drift: 1/1 features flagged" in result.telegram_message
    assert sender.messages == [result.telegram_message]


def test_daily_job_date_range_defaults_to_today_with_lookback() -> None:
    start_date, end_date = resolve_daily_job_date_range(
        DailyJobConfig(lookback_days=10),
        today=date(2026, 5, 14),
    )

    assert start_date == "2026-05-04"
    assert end_date == "2026-05-14"


def test_daily_job_date_range_rejects_invalid_lookback() -> None:
    with pytest.raises(ValueError, match="lookback_days"):
        resolve_daily_job_date_range(
            DailyJobConfig(lookback_days=0),
            today=date(2026, 5, 14),
        )
