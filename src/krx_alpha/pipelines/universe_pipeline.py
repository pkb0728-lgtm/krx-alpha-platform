from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from krx_alpha.collectors.price_collector import PriceRequest
from krx_alpha.database.storage import (
    daily_report_file_path,
    final_signal_file_path,
    read_parquet,
    universe_summary_csv_path,
    universe_summary_file_path,
    write_csv,
    write_parquet,
)
from krx_alpha.pipelines.daily_pipeline import DailyPipeline, DailyPipelineResult


@dataclass(frozen=True)
class UniversePipelineResult:
    summary_path: Path
    summary_csv_path: Path
    total_count: int
    success_count: int
    failed_count: int


class UniversePipeline:
    """Run the daily pipeline for multiple tickers and save a universe summary."""

    def __init__(self, project_root: Path, daily_pipeline: DailyPipeline | None = None) -> None:
        self.project_root = project_root
        self.daily_pipeline = daily_pipeline or DailyPipeline(project_root)

    def run(self, tickers: list[str], start_date: str, end_date: str) -> UniversePipelineResult:
        rows: list[dict[str, object]] = []

        for ticker in tickers:
            normalized_ticker = ticker.strip().zfill(6)
            if not normalized_ticker:
                continue

            try:
                request = PriceRequest.from_strings(
                    ticker=normalized_ticker,
                    start_date=start_date,
                    end_date=end_date,
                )
                result = self.daily_pipeline.run(request)
                rows.append(_success_row_from_pipeline(request, result))
            except Exception as exc:
                cached_row = self._cached_signal_row(
                    ticker=normalized_ticker,
                    start_date=start_date,
                    end_date=end_date,
                    original_error=exc,
                )
                rows.append(cached_row or _failed_row(normalized_ticker, exc))

        summary_frame = pd.DataFrame(rows).sort_values(
            ["status", "latest_confidence_score"],
            ascending=[False, False],
        )
        start_compact = start_date.replace("-", "")
        end_compact = end_date.replace("-", "")
        summary_path = universe_summary_file_path(self.project_root, start_compact, end_compact)
        summary_csv_path = universe_summary_csv_path(self.project_root, start_compact, end_compact)
        write_parquet(summary_frame, summary_path)
        write_csv(summary_frame, summary_csv_path)

        success_count = int((summary_frame["status"] == "success").sum())
        failed_count = int((summary_frame["status"] == "failed").sum())
        return UniversePipelineResult(
            summary_path=summary_path,
            summary_csv_path=summary_csv_path,
            total_count=len(summary_frame),
            success_count=success_count,
            failed_count=failed_count,
        )

    def _cached_signal_row(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        original_error: Exception,
    ) -> dict[str, object] | None:
        request = PriceRequest.from_strings(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        )
        signal_path = final_signal_file_path(
            self.project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        if not signal_path.exists():
            return None

        signal_frame = read_parquet(signal_path)
        if signal_frame.empty:
            return None

        latest_signal = signal_frame.copy()
        latest_signal["date"] = pd.to_datetime(latest_signal["date"], errors="coerce")
        row = latest_signal.sort_values("date").iloc[-1]
        return {
            "ticker": request.ticker,
            "status": "success",
            "latest_action": str(row["final_action"]),
            "latest_confidence_score": float(row["confidence_score"]),
            "latest_financial_score": _safe_float(row.get("financial_score")),
            "latest_event_score": _safe_float(row.get("event_score")),
            "latest_flow_score": _safe_float(row.get("flow_score")),
            "latest_news_score": _safe_float(row.get("news_score")),
            "latest_macro_score": _safe_float(row.get("macro_score")),
            "latest_market_regime": str(row.get("market_regime", "cached")),
            "signal_path": str(signal_path),
            "report_path": str(
                daily_report_file_path(
                    self.project_root,
                    request.ticker,
                    request.pykrx_end_date,
                )
            ),
            "error": f"used_cached_signal_after_failure: {original_error}",
        }


def _success_row_from_pipeline(
    request: PriceRequest,
    result: DailyPipelineResult,
) -> dict[str, object]:
    return {
        "ticker": request.ticker,
        "status": "success",
        "latest_action": result.latest_action,
        "latest_confidence_score": result.latest_confidence_score,
        "latest_financial_score": result.latest_financial_score,
        "latest_event_score": result.latest_event_score,
        "latest_flow_score": result.latest_flow_score,
        "latest_news_score": result.latest_news_score,
        "latest_macro_score": result.latest_macro_score,
        "latest_market_regime": result.latest_market_regime,
        "signal_path": str(result.signal_path),
        "report_path": str(result.report_path),
        "error": "",
    }


def _failed_row(ticker: str, exc: Exception) -> dict[str, object]:
    return {
        "ticker": ticker,
        "status": "failed",
        "latest_action": "",
        "latest_confidence_score": 0.0,
        "latest_financial_score": 0.0,
        "latest_event_score": 0.0,
        "latest_flow_score": 0.0,
        "latest_news_score": 0.0,
        "latest_macro_score": 0.0,
        "latest_market_regime": "",
        "signal_path": "",
        "report_path": "",
        "error": str(exc),
    }


def _safe_float(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(cast(Any, value))
