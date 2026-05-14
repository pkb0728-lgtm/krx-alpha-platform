from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from krx_alpha.database.storage import (
    drift_result_file_path,
    final_signal_file_path,
    processed_price_file_path,
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
        frame["signal_path"] = [
            str(
                final_signal_file_path(
                    self.project_root,
                    ticker,
                    start_compact,
                    end_compact,
                )
            )
            for ticker in frame["ticker"]
        ]
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
        for ticker in ["005930", "005380"]:
            _write_paper_inputs(
                self.project_root,
                ticker=ticker,
                start_date=start_compact,
                end_date=end_compact,
                buy_candidate=ticker == "005380",
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


class FakeKISPaperCandidateSource:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def build_candidates(
        self,
        screening_frame: Any,
        *,
        max_candidates: int,
        cash_buffer_pct: float,
    ) -> pd.DataFrame:
        self.calls.append(
            {
                "screening_rows": len(screening_frame),
                "max_candidates": max_candidates,
                "cash_buffer_pct": cash_buffer_pct,
            }
        )
        return pd.DataFrame(
            {
                "ticker": ["005380", "005930"],
                "candidate_action": ["review_buy", "skip"],
                "candidate_type": ["new_buy_candidate", "screen_blocked_or_rejected"],
                "estimated_quantity": [3, 0],
                "estimated_amount": [300_000.0, 0.0],
                "target_position_pct": [10.0, 0.0],
                "confidence_score": [72.0, 58.0],
                "screen_score": [75.0, 50.0],
                "reason": ["passed_buy_candidate_signal", "confidence_below_threshold"],
                "orders_sent": [0, 0],
            }
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
    assert result.paper_summary_path is not None
    assert result.paper_summary_path.exists()
    assert result.paper_report_path is not None
    assert result.paper_report_path.exists()
    assert result.paper_trade_count == 2
    assert result.paper_cumulative_return > 0
    assert result.screening_result_path is not None
    assert result.screening_result_path.exists()
    assert result.screening_csv_path is not None
    assert result.screening_csv_path.exists()
    assert result.screening_report_path is not None
    assert result.screening_report_path.exists()
    assert result.screening_checked_count == 2
    assert result.operations_health_path.exists()
    assert result.operations_health_report_path.exists()
    assert result.telegram_sent is False
    assert result.telegram_dry_run is True
    assert "005380 | buy_candidate" in result.telegram_message
    assert "Auto screener" in result.telegram_message
    assert "Paper portfolio" in result.telegram_message
    assert "trades 2" in result.telegram_message
    assert "Data drift: 1/1 features flagged" in result.telegram_message
    assert "Operations health" in result.telegram_message
    assert sender.messages == [result.telegram_message]


def test_daily_job_runner_can_create_kis_paper_candidate_outputs(tmp_path: Path) -> None:
    pipeline = FakeUniversePipeline(tmp_path)
    candidate_source = FakeKISPaperCandidateSource()
    runner = DailyJobRunner(
        project_root=tmp_path,
        universe_pipeline=pipeline,  # type: ignore[arg-type]
        telegram_sender=FakeTelegramSender(),
        kis_candidate_source=candidate_source,
    )

    result = runner.run(
        DailyJobConfig(
            universe="demo",
            start_date="2024-01-01",
            end_date="2024-01-31",
            telegram_dry_run=True,
            kis_paper_candidates=True,
            kis_candidate_max_candidates=5,
            kis_candidate_cash_buffer_pct=7.5,
        )
    )

    assert candidate_source.calls == [
        {
            "screening_rows": 2,
            "max_candidates": 5,
            "cash_buffer_pct": 7.5,
        }
    ]
    assert result.kis_candidate_result_path is not None
    assert result.kis_candidate_result_path.exists()
    assert result.kis_candidate_csv_path is not None
    assert result.kis_candidate_csv_path.exists()
    assert result.kis_candidate_report_path is not None
    assert result.kis_candidate_report_path.exists()
    assert result.kis_candidate_count == 2
    assert result.kis_candidate_review_count == 1
    assert result.kis_candidate_manual_price_count == 0


def _write_paper_inputs(
    project_root: Path,
    ticker: str,
    start_date: str,
    end_date: str,
    buy_candidate: bool,
) -> None:
    price_path = processed_price_file_path(project_root, ticker, start_date, end_date)
    signal_path = final_signal_file_path(project_root, ticker, start_date, end_date)
    write_parquet(_processed_price_frame(ticker), price_path)
    write_parquet(_final_signal_frame(ticker, buy_candidate), signal_path)


def _processed_price_frame(ticker: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=8, freq="D").date,
            "as_of_date": pd.date_range("2024-01-02", periods=8, freq="D").date,
            "ticker": [ticker] * 8,
            "open": [100, 101, 102, 103, 104, 105, 106, 107],
            "high": [101, 102, 103, 104, 105, 106, 107, 108],
            "low": [99, 100, 101, 102, 103, 104, 105, 106],
            "close": [100, 102, 104, 106, 108, 110, 112, 114],
            "volume": [1000] * 8,
            "trading_value": [100000] * 8,
            "trading_value_is_estimated": [False] * 8,
            "return_1d": [float("nan")] + [0.01] * 7,
            "log_return_1d": [float("nan")] + [0.00995] * 7,
            "range_pct": [0.02] * 8,
            "change_rate": [0.0] * 8,
            "source": ["test"] * 8,
            "collected_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 8,
            "processed_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 8,
        }
    )


def _final_signal_frame(ticker: str, buy_candidate: bool) -> pd.DataFrame:
    actions = ["buy_candidate", "avoid"] if buy_candidate else ["watch", "watch"]
    return pd.DataFrame(
        {
            "date": ["2024-01-03", "2024-01-06"],
            "as_of_date": ["2024-01-03", "2024-01-06"],
            "ticker": [ticker, ticker],
            "source_signal_label": actions,
            "financial_score": [50.0, 50.0],
            "financial_reason": ["neutral", "neutral"],
            "event_score": [50.0, 50.0],
            "event_risk_flag": [False, False],
            "event_reason": ["neutral", "neutral"],
            "flow_score": [50.0, 50.0],
            "flow_reason": ["neutral", "neutral"],
            "news_score": [50.0, 50.0],
            "news_reason": ["neutral", "neutral"],
            "macro_score": [50.0, 50.0],
            "macro_reason": ["neutral", "neutral"],
            "final_action": actions,
            "confidence_score": [75.0, 35.0],
            "risk_blocked": [False, False],
            "risk_flags": ["", ""],
            "suggested_position_pct": [10.0, 0.0],
            "signal_reason": ["paper buy", "paper exit"],
            "generated_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 2,
        }
    )


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
