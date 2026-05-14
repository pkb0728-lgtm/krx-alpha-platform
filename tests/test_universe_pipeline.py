from pathlib import Path

import pandas as pd

from krx_alpha.collectors.price_collector import PriceRequest
from krx_alpha.database.storage import final_signal_file_path, write_parquet
from krx_alpha.pipelines.daily_pipeline import DailyPipelineResult
from krx_alpha.pipelines.universe_pipeline import UniversePipeline


class FakeDailyPipeline:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root

    def run(self, request: PriceRequest) -> DailyPipelineResult:
        if request.ticker == "000000":
            raise RuntimeError("mock failure")

        path = self.project_root / f"{request.ticker}.parquet"
        path.write_text("placeholder", encoding="utf-8")
        report_path = self.project_root / f"{request.ticker}.md"
        report_path.write_text("report", encoding="utf-8")
        return DailyPipelineResult(
            raw_path=path,
            processed_path=path,
            data_quality_path=path,
            data_quality_report_path=report_path,
            feature_path=path,
            regime_path=path,
            regime_report_path=report_path,
            score_path=path,
            signal_path=path,
            report_path=report_path,
            latest_action="watch",
            latest_confidence_score=60.0,
            latest_financial_score=50.0,
            latest_event_score=50.0,
            latest_flow_score=50.0,
            latest_news_score=50.0,
            latest_macro_score=50.0,
            latest_market_regime="neutral",
            data_quality_warning_count=1,
            data_quality_fail_count=0,
        )


def test_universe_pipeline_saves_summary(tmp_path: Path) -> None:
    pipeline = UniversePipeline(
        project_root=tmp_path,
        daily_pipeline=FakeDailyPipeline(tmp_path),  # type: ignore[arg-type]
    )

    result = pipeline.run(
        tickers=["005930", "000000"],
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    assert result.total_count == 2
    assert result.success_count == 1
    assert result.failed_count == 1
    assert result.summary_path.exists()
    assert result.summary_csv_path.exists()
    summary = pd.read_parquet(result.summary_path)
    assert summary.loc[0, "data_quality_warning_count"] == 1
    assert summary.loc[0, "data_quality_fail_count"] == 0


def test_universe_pipeline_uses_cached_signal_after_collection_failure(tmp_path: Path) -> None:
    signal_path = final_signal_file_path(tmp_path, "000000", "20240101", "20240131")
    write_parquet(
        pd.DataFrame(
            {
                "date": ["2024-01-31"],
                "ticker": ["000000"],
                "final_action": ["watch"],
                "confidence_score": [61.5],
                "financial_score": [50.0],
                "event_score": [50.0],
                "flow_score": [55.0],
                "news_score": [60.0],
                "macro_score": [50.0],
                "market_regime": ["neutral"],
            }
        ),
        signal_path,
    )
    pipeline = UniversePipeline(
        project_root=tmp_path,
        daily_pipeline=FakeDailyPipeline(tmp_path),  # type: ignore[arg-type]
    )

    result = pipeline.run(
        tickers=["000000"],
        start_date="2024-01-01",
        end_date="2024-01-31",
    )
    summary = pd.read_parquet(result.summary_path)

    assert result.success_count == 1
    assert result.failed_count == 0
    assert summary.loc[0, "latest_action"] == "watch"
    assert summary.loc[0, "signal_path"] == str(signal_path)
    assert "used_cached_signal_after_failure" in summary.loc[0, "error"]
