from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from krx_alpha.collectors.price_collector import PriceRequest, PykrxPriceCollector
from krx_alpha.database.storage import (
    daily_report_file_path,
    daily_score_file_path,
    final_signal_file_path,
    market_regime_file_path,
    market_regime_report_file_path,
    price_feature_file_path,
    processed_price_file_path,
    raw_price_file_path,
    read_parquet,
    write_parquet,
    write_text,
)
from krx_alpha.features.price_features import PriceFeatureBuilder
from krx_alpha.processors.price_processor import PriceProcessor
from krx_alpha.regime.market_regime import MarketRegimeAnalyzer
from krx_alpha.reports.daily_report import DailyReportGenerator
from krx_alpha.reports.regime_report import MarketRegimeReportGenerator
from krx_alpha.scoring.price_scorer import PriceScorer
from krx_alpha.signals.signal_engine import SignalEngine


@dataclass(frozen=True)
class DailyPipelineResult:
    raw_path: Path
    processed_path: Path
    feature_path: Path
    regime_path: Path
    regime_report_path: Path
    score_path: Path
    signal_path: Path
    report_path: Path
    latest_action: str
    latest_confidence_score: float
    latest_financial_score: float
    latest_market_regime: str


class DailyPipeline:
    """Run the daily single-stock pipeline from collection to report generation."""

    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root

    def run(
        self,
        request: PriceRequest,
        financial_feature_frame: pd.DataFrame | None = None,
    ) -> DailyPipelineResult:
        raw_frame = PykrxPriceCollector().collect(request)
        raw_path = raw_price_file_path(
            self.project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        write_parquet(raw_frame, raw_path)

        processed_frame = PriceProcessor().process(raw_frame)
        processed_path = processed_price_file_path(
            self.project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        write_parquet(processed_frame, processed_path)

        feature_frame = PriceFeatureBuilder().build(processed_frame)
        feature_path = price_feature_file_path(
            self.project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        write_parquet(feature_frame, feature_path)

        regime_frame = MarketRegimeAnalyzer().analyze(feature_frame)
        regime_path = market_regime_file_path(
            self.project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        write_parquet(regime_frame, regime_path)
        regime_report_path = market_regime_report_file_path(
            self.project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        write_text(MarketRegimeReportGenerator().generate(regime_frame), regime_report_path)

        score_frame = PriceScorer().score(feature_frame, financial_feature_frame)
        score_path = daily_score_file_path(
            self.project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        write_parquet(score_frame, score_path)

        signal_frame = SignalEngine().generate(score_frame, feature_frame, regime_frame)
        signal_path = final_signal_file_path(
            self.project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        write_parquet(signal_frame, signal_path)

        report = DailyReportGenerator().generate(score_frame, feature_frame)
        latest_date = score_frame.sort_values("date").iloc[-1]["date"]
        report_path = daily_report_file_path(
            self.project_root,
            request.ticker,
            str(pd.Timestamp(latest_date).strftime("%Y%m%d")),
        )
        write_text(report, report_path)

        latest_signal = read_parquet(signal_path).sort_values("date").iloc[-1]
        latest_regime = regime_frame.sort_values("date").iloc[-1]
        return DailyPipelineResult(
            raw_path=raw_path,
            processed_path=processed_path,
            feature_path=feature_path,
            regime_path=regime_path,
            regime_report_path=regime_report_path,
            score_path=score_path,
            signal_path=signal_path,
            report_path=report_path,
            latest_action=str(latest_signal["final_action"]),
            latest_confidence_score=float(latest_signal["confidence_score"]),
            latest_financial_score=float(
                score_frame.sort_values("date").iloc[-1]["financial_score"]
            ),
            latest_market_regime=str(latest_regime["regime"]),
        )
