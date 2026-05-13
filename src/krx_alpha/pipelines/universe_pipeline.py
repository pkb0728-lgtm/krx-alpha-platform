from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from krx_alpha.collectors.price_collector import PriceRequest
from krx_alpha.database.storage import (
    universe_summary_csv_path,
    universe_summary_file_path,
    write_csv,
    write_parquet,
)
from krx_alpha.pipelines.daily_pipeline import DailyPipeline


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
                rows.append(
                    {
                        "ticker": request.ticker,
                        "status": "success",
                        "latest_action": result.latest_action,
                        "latest_confidence_score": result.latest_confidence_score,
                        "latest_financial_score": result.latest_financial_score,
                        "latest_event_score": result.latest_event_score,
                        "latest_flow_score": result.latest_flow_score,
                        "latest_market_regime": result.latest_market_regime,
                        "signal_path": str(result.signal_path),
                        "report_path": str(result.report_path),
                        "error": "",
                    }
                )
            except Exception as exc:
                rows.append(
                    {
                        "ticker": normalized_ticker,
                        "status": "failed",
                        "latest_action": "",
                        "latest_confidence_score": 0.0,
                        "latest_financial_score": 0.0,
                        "latest_event_score": 0.0,
                        "latest_flow_score": 0.0,
                        "latest_market_regime": "",
                        "signal_path": "",
                        "report_path": "",
                        "error": str(exc),
                    }
                )

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
