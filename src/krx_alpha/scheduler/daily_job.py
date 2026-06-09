import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Protocol

import pandas as pd

from krx_alpha.backtest.simple_backtester import BacktestConfig, SimpleBacktester
from krx_alpha.backtest.walk_forward import WalkForwardBacktester, WalkForwardConfig
from krx_alpha.broker.kis_candidates import format_kis_paper_candidate_report
from krx_alpha.broker.manual_order_plan import (
    build_manual_order_plan,
    format_manual_order_plan_report,
)
from krx_alpha.collectors.macro_collector import FredMacroCollector, MacroRequest
from krx_alpha.collectors.news_collector import NaverNewsCollector, NewsSearchRequest
from krx_alpha.collectors.price_collector import PriceRequest
from krx_alpha.dashboard.data_loader import (
    find_latest_backtest_metrics,
    find_latest_drift_result,
    find_latest_walk_forward_summary,
)
from krx_alpha.database.storage import (
    backtest_metrics_file_path,
    backtest_report_file_path,
    backtest_trades_file_path,
    final_signal_file_path,
    kis_paper_candidate_csv_path,
    kis_paper_candidate_file_path,
    kis_paper_candidate_report_file_path,
    macro_feature_file_path,
    manual_order_plan_csv_path,
    manual_order_plan_file_path,
    manual_order_plan_report_file_path,
    ml_metrics_file_path,
    ml_model_artifact_file_path,
    ml_model_report_file_path,
    ml_prediction_file_path,
    ml_training_dataset_file_path,
    monitoring_report_file_path,
    news_sentiment_feature_file_path,
    operations_health_file_path,
    paper_position_file_path,
    paper_summary_file_path,
    paper_trade_ledger_file_path,
    paper_trading_report_file_path,
    price_feature_file_path,
    processed_price_file_path,
    raw_macro_file_path,
    raw_news_file_path,
    read_parquet,
    screening_report_file_path,
    screening_result_csv_path,
    screening_result_file_path,
    universe_report_file_path,
    walk_forward_folds_file_path,
    walk_forward_report_file_path,
    walk_forward_summary_file_path,
    write_csv,
    write_parquet,
    write_text,
)
from krx_alpha.experiments.tracker import (
    ExperimentTracker,
    build_backtest_experiment_record,
    build_daily_job_experiment_record,
    build_ml_baseline_experiment_record,
    build_walk_forward_experiment_record,
)
from krx_alpha.features.macro_features import MacroFeatureBuilder
from krx_alpha.features.news_sentiment import NewsSentimentConfig, NewsSentimentFeatureBuilder
from krx_alpha.journal.decision_journal import (
    DecisionJournalWriteResult,
    append_decision_journal,
    build_decision_journal_frame,
)
from krx_alpha.models.probability_baseline import (
    MLProbabilityBaselineConfig,
    MLProbabilityBaselineTrainer,
)
from krx_alpha.models.training_dataset import MLTrainingDatasetBuilder, MLTrainingDatasetConfig
from krx_alpha.monitoring.operations_health import (
    OperationsHealthChecker,
    format_operations_health_report,
)
from krx_alpha.paper_trading.portfolio import (
    PaperPortfolioConfig,
    PaperPortfolioResult,
    run_paper_portfolio,
)
from krx_alpha.paper_trading.simulator import PaperTradingConfig, PaperTradingSimulator
from krx_alpha.pipelines.universe_pipeline import UniversePipeline, UniversePipelineResult
from krx_alpha.reports.backtest_report import BacktestReportGenerator, WalkForwardReportGenerator
from krx_alpha.reports.ml_report import MLProbabilityBaselineReportGenerator
from krx_alpha.reports.paper_trading_report import PaperTradingReportGenerator
from krx_alpha.reports.universe_report import UniverseReportGenerator
from krx_alpha.screening.auto_screener import (
    AutoScreener,
    AutoScreenerConfig,
    format_screening_report,
)
from krx_alpha.telegram.notifier import (
    TelegramNotifier,
    TelegramSendResult,
    build_daily_telegram_message,
)
from krx_alpha.universe.static_universe import UniverseRegistry


class TelegramMessageSender(Protocol):
    def send_message(self, message: str, dry_run: bool = False) -> TelegramSendResult:
        """Send or preview a Telegram message."""
        ...


class KISPaperCandidateSource(Protocol):
    def build_candidates(
        self,
        screening_frame: Any,
        *,
        max_candidates: int,
        cash_buffer_pct: float,
    ) -> Any:
        """Build KIS paper review candidates without sending orders."""
        ...


@dataclass(frozen=True)
class DailyJobConfig:
    universe: str = "demo"
    start_date: str | None = None
    end_date: str | None = None
    lookback_days: int = 60
    notify: bool = True
    telegram_dry_run: bool = True
    telegram_top_n: int = 5
    paper_trade: bool = True
    paper_initial_cash: float = 10_000_000.0
    paper_max_position_pct: float = 10.0
    paper_transaction_cost_bps: float = 15.0
    paper_slippage_bps: float = 10.0
    paper_skip_missing: bool = True
    screening: bool = True
    screening_min_confidence: float = 60.0
    screening_min_score: float = 60.0
    kis_paper_candidates: bool = False
    kis_candidate_max_candidates: int = 10
    kis_candidate_cash_buffer_pct: float = 5.0
    refresh_dashboard_artifacts: bool = True
    score_external_features: bool = True
    dashboard_artifact_ticker: str | None = None
    dashboard_artifact_holding_days: int = 5
    dashboard_artifact_train_size: int = 40
    dashboard_artifact_test_size: int = 10
    dashboard_artifact_step_size: int = 10
    macro_series: str = "DGS10,DFF,DEXKOUS"
    macro_live: bool = True
    fred_api_key: str | None = None
    news_live: bool = True
    news_display: int = 5
    news_use_gemini: bool = False
    naver_client_id: str | None = None
    naver_client_secret: str | None = None
    gemini_api_key: str | None = None


@dataclass(frozen=True)
class DailyJobResult:
    universe: str
    start_date: str
    end_date: str
    summary_path: Path
    summary_csv_path: Path
    report_path: Path
    experiment_log_path: Path
    total_count: int
    success_count: int
    failed_count: int
    paper_summary_path: Path | None
    paper_report_path: Path | None
    paper_trade_count: int
    paper_cumulative_return: float
    screening_result_path: Path | None
    screening_csv_path: Path | None
    screening_report_path: Path | None
    screening_checked_count: int
    screening_passed_count: int
    kis_candidate_result_path: Path | None
    kis_candidate_csv_path: Path | None
    kis_candidate_report_path: Path | None
    kis_candidate_count: int
    kis_candidate_review_count: int
    kis_candidate_manual_price_count: int
    manual_order_plan_path: Path | None
    manual_order_plan_csv_path: Path | None
    manual_order_plan_report_path: Path | None
    manual_order_plan_review_count: int
    telegram_sent: bool
    telegram_dry_run: bool
    telegram_message: str
    operations_health_path: Path
    operations_health_report_path: Path
    decision_journal_path: Path
    decision_journal_csv_path: Path
    decision_journal_appended_count: int
    decision_journal_total_count: int
    scoring_macro_feature_path: Path | None
    scoring_news_feature_paths: tuple[Path, ...]
    scoring_feature_errors: tuple[str, ...]
    dashboard_artifact_ticker: str
    macro_feature_path: Path | None
    backtest_metrics_path: Path | None
    paper_single_summary_path: Path | None
    walk_forward_summary_path: Path | None
    ml_metrics_path: Path | None
    dashboard_artifact_errors: tuple[str, ...]


@dataclass(frozen=True)
class DailyJobScreeningResult:
    result_path: Path
    csv_path: Path
    report_path: Path
    frame: Any
    checked_count: int
    passed_count: int


@dataclass(frozen=True)
class DailyJobKISCandidateResult:
    result_path: Path
    csv_path: Path
    report_path: Path
    frame: Any
    candidate_count: int
    review_count: int
    manual_price_count: int
    manual_order_plan_path: Path
    manual_order_plan_csv_path: Path
    manual_order_plan_report_path: Path
    manual_order_plan_review_count: int


@dataclass(frozen=True)
class DailyJobDashboardArtifactResult:
    ticker: str
    macro_feature_path: Path | None
    backtest_metrics_path: Path | None
    paper_single_summary_path: Path | None
    walk_forward_summary_path: Path | None
    ml_metrics_path: Path | None
    errors: tuple[str, ...]


@dataclass(frozen=True)
class DailyJobScoringFeatureResult:
    macro_feature_frame: Any | None
    macro_feature_path: Path | None
    news_feature_frame: Any | None
    news_feature_paths: tuple[Path, ...]
    errors: tuple[str, ...]


class DailyJobRunner:
    """Run the after-market daily workflow for a named universe."""

    def __init__(
        self,
        project_root: Path,
        universe_pipeline: UniversePipeline | None = None,
        telegram_sender: TelegramMessageSender | None = None,
        kis_candidate_source: KISPaperCandidateSource | None = None,
        experiment_tracker: ExperimentTracker | None = None,
    ) -> None:
        self.project_root = project_root
        self.universe_pipeline = universe_pipeline or UniversePipeline(project_root)
        self.telegram_sender = telegram_sender
        self.kis_candidate_source = kis_candidate_source
        self.experiment_tracker = experiment_tracker or ExperimentTracker(project_root)

    def run(self, config: DailyJobConfig, today: date | None = None) -> DailyJobResult:
        start_date, end_date = resolve_daily_job_date_range(config, today or date.today())
        definition = UniverseRegistry().get(config.universe)
        scoring_feature_result = self._prepare_scoring_features(
            config=config,
            tickers=definition.tickers(),
            start_date=start_date,
            end_date=end_date,
        )
        pipeline_result = self.universe_pipeline.run(
            tickers=definition.tickers(),
            start_date=start_date,
            end_date=end_date,
            news_feature_frame=scoring_feature_result.news_feature_frame,
            macro_feature_frame=scoring_feature_result.macro_feature_frame,
        )

        summary_frame = read_parquet(pipeline_result.summary_path)
        start_compact = start_date.replace("-", "")
        end_compact = end_date.replace("-", "")
        report_path = universe_report_file_path(self.project_root, start_compact, end_compact)
        write_text(
            UniverseReportGenerator().generate(
                summary_frame, start_date=start_date, end_date=end_date
            ),
            report_path,
        )

        screening_result = self._run_screening(
            config=config,
            summary_frame=summary_frame,
            start_date=start_date,
            end_date=end_date,
        )
        kis_candidate_result = self._run_kis_candidates(
            config=config,
            screening_result=screening_result,
            start_date=start_date,
            end_date=end_date,
        )
        paper_result = self._run_paper_portfolio(
            config=config,
            tickers=definition.tickers(),
            start_date=start_date,
            end_date=end_date,
        )
        dashboard_artifact_result = self._refresh_dashboard_artifacts(
            config=config,
            summary_frame=summary_frame,
            start_date=start_date,
            end_date=end_date,
            macro_feature_path=scoring_feature_result.macro_feature_path,
        )
        journal_result = self._write_decision_journal(
            config=config,
            start_date=start_date,
            end_date=end_date,
            summary_frame=summary_frame,
            screening_frame=screening_result.frame if screening_result else None,
            kis_candidate_frame=kis_candidate_result.frame if kis_candidate_result else None,
        )
        paper_summary = paper_result.summary if paper_result is not None else None
        operations_health, operations_health_path, operations_health_report_path = (
            self._write_operations_health()
        )

        telegram_result = self._notify(
            config,
            summary_frame,
            screening_result.frame if screening_result else None,
            paper_summary,
            kis_candidate_result.frame if kis_candidate_result else None,
            operations_health,
        )
        experiment_log_path = self.experiment_tracker.log(
            build_daily_job_experiment_record(
                universe=config.universe,
                start_date=start_date,
                end_date=end_date,
                total_count=pipeline_result.total_count,
                success_count=pipeline_result.success_count,
                failed_count=pipeline_result.failed_count,
                report_path=report_path,
                telegram_sent=telegram_result.sent,
                telegram_dry_run=telegram_result.dry_run,
                paper_trade_enabled=config.paper_trade,
                paper_trade_count=_paper_trade_count(paper_summary),
                paper_cumulative_return=_paper_cumulative_return(paper_summary),
                paper_summary_path=paper_result.summary_path if paper_result else None,
                screening_enabled=config.screening,
                screening_checked_count=screening_result.checked_count if screening_result else 0,
                screening_passed_count=screening_result.passed_count if screening_result else 0,
                screening_result_path=screening_result.result_path if screening_result else None,
            )
        )
        return _build_result(
            config=config,
            start_date=start_date,
            end_date=end_date,
            pipeline_result=pipeline_result,
            report_path=report_path,
            screening_result=screening_result,
            kis_candidate_result=kis_candidate_result,
            paper_result=paper_result,
            telegram_result=telegram_result,
            experiment_log_path=experiment_log_path,
            operations_health_path=operations_health_path,
            operations_health_report_path=operations_health_report_path,
            journal_result=journal_result,
            dashboard_artifact_result=dashboard_artifact_result,
            scoring_feature_result=scoring_feature_result,
        )

    def _run_screening(
        self,
        config: DailyJobConfig,
        summary_frame: Any,
        start_date: str,
        end_date: str,
    ) -> DailyJobScreeningResult | None:
        if not config.screening:
            return None

        start_compact = start_date.replace("-", "")
        end_compact = end_date.replace("-", "")
        report_name = f"screening_universe_{start_compact}_{end_compact}"
        result_path = screening_result_file_path(self.project_root, report_name)
        csv_path = screening_result_csv_path(self.project_root, report_name)
        report_path = screening_report_file_path(self.project_root, report_name)

        result_frame = AutoScreener(
            self.project_root,
            AutoScreenerConfig(
                min_confidence=config.screening_min_confidence,
                min_screen_score=config.screening_min_score,
            ),
        ).screen(summary_frame)
        write_parquet(result_frame, result_path)
        write_csv(result_frame, csv_path)
        write_text(
            format_screening_report(
                result_frame,
                title=f"Auto Screener Report ({config.universe})",
            ),
            report_path,
        )
        return DailyJobScreeningResult(
            result_path=result_path,
            csv_path=csv_path,
            report_path=report_path,
            frame=result_frame,
            checked_count=len(result_frame),
            passed_count=int(result_frame["passed"].sum()) if not result_frame.empty else 0,
        )

    def _run_paper_portfolio(
        self,
        config: DailyJobConfig,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> PaperPortfolioResult | None:
        if not config.paper_trade:
            return None

        return run_paper_portfolio(
            self.project_root,
            PaperPortfolioConfig(
                name=config.universe,
                tickers=tuple(tickers),
                start_date=start_date,
                end_date=end_date,
                initial_cash=config.paper_initial_cash,
                max_position_pct=config.paper_max_position_pct,
                transaction_cost_bps=config.paper_transaction_cost_bps,
                slippage_bps=config.paper_slippage_bps,
                skip_missing=config.paper_skip_missing,
            ),
        )

    def _run_kis_candidates(
        self,
        config: DailyJobConfig,
        screening_result: DailyJobScreeningResult | None,
        start_date: str,
        end_date: str,
    ) -> DailyJobKISCandidateResult | None:
        if not config.kis_paper_candidates:
            return None
        if screening_result is None:
            raise ValueError("KIS paper candidates require screening to be enabled.")
        if self.kis_candidate_source is None:
            raise ValueError("KIS paper candidate source is not configured.")

        start_compact = start_date.replace("-", "")
        end_compact = end_date.replace("-", "")
        report_name = f"kis_paper_candidates_{config.universe}_{start_compact}_{end_compact}"
        result_path = kis_paper_candidate_file_path(self.project_root, report_name)
        csv_path = kis_paper_candidate_csv_path(self.project_root, report_name)
        report_path = kis_paper_candidate_report_file_path(self.project_root, report_name)
        manual_plan_name = f"manual_order_plan_{config.universe}_{start_compact}_{end_compact}"
        manual_plan_path = manual_order_plan_file_path(self.project_root, manual_plan_name)
        manual_plan_csv = manual_order_plan_csv_path(self.project_root, manual_plan_name)
        manual_plan_report = manual_order_plan_report_file_path(
            self.project_root,
            manual_plan_name,
        )

        result_frame = self.kis_candidate_source.build_candidates(
            screening_result.frame,
            max_candidates=config.kis_candidate_max_candidates,
            cash_buffer_pct=config.kis_candidate_cash_buffer_pct,
        )
        write_parquet(result_frame, result_path)
        write_csv(result_frame, csv_path)
        write_text(format_kis_paper_candidate_report(result_frame), report_path)
        manual_plan_frame = build_manual_order_plan(result_frame)
        write_parquet(manual_plan_frame, manual_plan_path)
        write_csv(manual_plan_frame, manual_plan_csv)
        write_text(format_manual_order_plan_report(manual_plan_frame), manual_plan_report)

        review_count = (
            int(result_frame["candidate_action"].isin(["review_buy", "review_add"]).sum())
            if not result_frame.empty
            else 0
        )
        manual_plan_review_count = (
            int(
                manual_plan_frame["manual_plan_action"]
                .isin(["manual_buy_review", "manual_add_review"])
                .sum()
            )
            if not manual_plan_frame.empty
            else 0
        )
        manual_price_count = (
            int((result_frame["candidate_action"] == "manual_price_required").sum())
            if not result_frame.empty
            else 0
        )
        return DailyJobKISCandidateResult(
            result_path=result_path,
            csv_path=csv_path,
            report_path=report_path,
            frame=result_frame,
            candidate_count=len(result_frame),
            review_count=review_count,
            manual_price_count=manual_price_count,
            manual_order_plan_path=manual_plan_path,
            manual_order_plan_csv_path=manual_plan_csv,
            manual_order_plan_report_path=manual_plan_report,
            manual_order_plan_review_count=manual_plan_review_count,
        )

    def _refresh_dashboard_artifacts(
        self,
        config: DailyJobConfig,
        summary_frame: Any,
        start_date: str,
        end_date: str,
        macro_feature_path: Path | None = None,
    ) -> DailyJobDashboardArtifactResult:
        if not config.refresh_dashboard_artifacts:
            return DailyJobDashboardArtifactResult("", None, None, None, None, None, ())

        ticker = _select_dashboard_artifact_ticker(
            summary_frame,
            config.dashboard_artifact_ticker,
            self.project_root,
            start_date,
            end_date,
        )
        errors: list[str] = []

        if macro_feature_path is None:
            macro_feature_path = self._refresh_macro_artifact(config, start_date, end_date, errors)
        if not ticker:
            errors.append("dashboard_artifacts: no successful ticker with price/signal inputs")
            return DailyJobDashboardArtifactResult(
                "",
                macro_feature_path,
                None,
                None,
                None,
                None,
                tuple(errors),
            )

        backtest_metrics_path = self._refresh_backtest_artifact(
            config,
            ticker,
            start_date,
            end_date,
            errors,
        )
        paper_single_summary_path = self._refresh_single_paper_artifact(
            config,
            ticker,
            start_date,
            end_date,
            errors,
        )
        walk_forward_summary_path = self._refresh_walk_forward_artifact(
            config,
            ticker,
            start_date,
            end_date,
            errors,
        )
        ml_metrics_path = self._refresh_ml_artifact(
            config,
            ticker,
            start_date,
            end_date,
            errors,
        )
        return DailyJobDashboardArtifactResult(
            ticker,
            macro_feature_path,
            backtest_metrics_path,
            paper_single_summary_path,
            walk_forward_summary_path,
            ml_metrics_path,
            tuple(errors),
        )

    def _refresh_macro_artifact(
        self,
        config: DailyJobConfig,
        start_date: str,
        end_date: str,
        errors: list[str],
    ) -> Path | None:
        try:
            use_live = config.macro_live and bool(config.fred_api_key)
            request = MacroRequest.from_strings(
                start_date=start_date,
                end_date=end_date,
                series_ids=config.macro_series,
                demo=not use_live,
            )
            try:
                macro_frame = FredMacroCollector(api_key=config.fred_api_key).collect(request)
            except Exception as exc:
                if not use_live:
                    raise
                errors.append(f"macro_live_failed_used_demo: {exc}")
                request = MacroRequest.from_strings(
                    start_date=start_date,
                    end_date=end_date,
                    series_ids=config.macro_series,
                    demo=True,
                )
                macro_frame = FredMacroCollector(api_key=None).collect(request)

            raw_path = raw_macro_file_path(
                self.project_root,
                request.compact_start_date,
                request.compact_end_date,
                request.series_slug,
            )
            write_parquet(macro_frame, raw_path)
            feature_frame = MacroFeatureBuilder().build(macro_frame)
            feature_path = macro_feature_file_path(
                self.project_root,
                request.compact_start_date,
                request.compact_end_date,
                request.series_slug,
            )
            write_parquet(feature_frame, feature_path)
            return feature_path
        except Exception as exc:
            errors.append(f"macro_refresh_failed: {exc}")
            return None

    def _prepare_scoring_features(
        self,
        config: DailyJobConfig,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> DailyJobScoringFeatureResult:
        if not config.score_external_features:
            return DailyJobScoringFeatureResult(None, None, None, (), ())

        errors: list[str] = []
        macro_feature_path = self._refresh_macro_artifact(config, start_date, end_date, errors)
        macro_feature_frame = read_parquet(macro_feature_path) if macro_feature_path else None
        news_feature_frame, news_feature_paths = self._refresh_news_feature_artifacts(
            config=config,
            tickers=tickers,
            start_date=start_date,
            end_date=end_date,
            errors=errors,
        )
        return DailyJobScoringFeatureResult(
            macro_feature_frame=macro_feature_frame,
            macro_feature_path=macro_feature_path,
            news_feature_frame=news_feature_frame,
            news_feature_paths=news_feature_paths,
            errors=tuple(errors),
        )

    def _refresh_news_feature_artifacts(
        self,
        config: DailyJobConfig,
        tickers: list[str],
        start_date: str,
        end_date: str,
        errors: list[str],
    ) -> tuple[Any | None, tuple[Path, ...]]:
        feature_frames: list[Any] = []
        feature_paths: list[Path] = []
        use_live = config.news_live and bool(config.naver_client_id and config.naver_client_secret)
        use_gemini = config.news_use_gemini and bool(config.gemini_api_key)
        if config.news_use_gemini and not use_gemini:
            errors.append("news_gemini_skipped_used_rule_based: missing Gemini API key")

        for ticker in tickers:
            request = NewsSearchRequest.from_strings(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                display=config.news_display,
                demo=not use_live,
            )
            try:
                try:
                    news_frame = NaverNewsCollector(
                        client_id=config.naver_client_id,
                        client_secret=config.naver_client_secret,
                    ).collect(request)
                except Exception as exc:
                    if not use_live:
                        raise
                    errors.append(f"news_live_failed_used_demo:{request.ticker}: {exc}")
                    request = NewsSearchRequest.from_strings(
                        ticker=ticker,
                        start_date=start_date,
                        end_date=end_date,
                        display=config.news_display,
                        demo=True,
                    )
                    news_frame = NaverNewsCollector().collect(request)

                raw_path = raw_news_file_path(
                    self.project_root,
                    request.ticker,
                    request.compact_start_date,
                    request.compact_end_date,
                )
                write_parquet(news_frame, raw_path)
                feature_frame = NewsSentimentFeatureBuilder(
                    api_key=config.gemini_api_key,
                    config=NewsSentimentConfig(
                        use_gemini=use_gemini,
                        allow_rule_fallback=True,
                    ),
                ).build(news_frame)
                feature_path = news_sentiment_feature_file_path(
                    self.project_root,
                    request.ticker,
                    request.compact_start_date,
                    request.compact_end_date,
                )
                write_parquet(feature_frame, feature_path)
                feature_frames.append(feature_frame)
                feature_paths.append(feature_path)
            except Exception as exc:
                errors.append(f"news_refresh_failed:{request.ticker}: {exc}")

        if not feature_frames:
            return None, tuple()
        return pd.concat(feature_frames, ignore_index=True), tuple(feature_paths)

    def _refresh_backtest_artifact(
        self,
        config: DailyJobConfig,
        ticker: str,
        start_date: str,
        end_date: str,
        errors: list[str],
    ) -> Path | None:
        try:
            request = PriceRequest.from_strings(
                ticker=ticker, start_date=start_date, end_date=end_date
            )
            price_frame, signal_frame = _load_validation_inputs(self.project_root, request)
            backtest_config = BacktestConfig(holding_days=config.dashboard_artifact_holding_days)
            trades, metrics = SimpleBacktester(backtest_config).run(price_frame, signal_frame)

            trades_path = backtest_trades_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            metrics_path = backtest_metrics_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            report_path = backtest_report_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            write_parquet(trades, trades_path)
            write_parquet(metrics, metrics_path)
            write_text(BacktestReportGenerator().generate(trades, metrics), report_path)
            self.experiment_tracker.log(
                build_backtest_experiment_record(
                    metrics=metrics,
                    config=backtest_config,
                    start_date=start_date,
                    end_date=end_date,
                    artifact_path=report_path,
                )
            )
            return metrics_path
        except Exception as exc:
            errors.append(f"backtest_refresh_failed:{ticker}: {exc}")
            return None

    def _refresh_single_paper_artifact(
        self,
        config: DailyJobConfig,
        ticker: str,
        start_date: str,
        end_date: str,
        errors: list[str],
    ) -> Path | None:
        try:
            request = PriceRequest.from_strings(
                ticker=ticker, start_date=start_date, end_date=end_date
            )
            price_frame, signal_frame = _load_validation_inputs(self.project_root, request)
            paper_config = PaperTradingConfig(
                initial_cash=config.paper_initial_cash,
                max_position_pct=config.paper_max_position_pct,
                transaction_cost_bps=config.paper_transaction_cost_bps,
                slippage_bps=config.paper_slippage_bps,
            )
            trades, positions, summary = PaperTradingSimulator(paper_config).run(
                price_frame,
                signal_frame,
            )
            ledger_path = paper_trade_ledger_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            positions_path = paper_position_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            summary_path = paper_summary_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            report_path = paper_trading_report_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            write_parquet(trades, ledger_path)
            write_parquet(positions, positions_path)
            write_parquet(summary, summary_path)
            write_text(
                PaperTradingReportGenerator().generate(trades, positions, summary), report_path
            )
            return summary_path
        except Exception as exc:
            errors.append(f"single_paper_refresh_failed:{ticker}: {exc}")
            return None

    def _refresh_walk_forward_artifact(
        self,
        config: DailyJobConfig,
        ticker: str,
        start_date: str,
        end_date: str,
        errors: list[str],
    ) -> Path | None:
        try:
            request = PriceRequest.from_strings(
                ticker=ticker, start_date=start_date, end_date=end_date
            )
            price_frame, signal_frame = _load_validation_inputs(self.project_root, request)
            walk_config = WalkForwardConfig(
                train_size=config.dashboard_artifact_train_size,
                test_size=config.dashboard_artifact_test_size,
                step_size=config.dashboard_artifact_step_size,
                holding_days=config.dashboard_artifact_holding_days,
            )
            folds, summary = WalkForwardBacktester(walk_config).run(price_frame, signal_frame)
            folds_path = walk_forward_folds_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            summary_path = walk_forward_summary_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            report_path = walk_forward_report_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            write_parquet(folds, folds_path)
            write_parquet(summary, summary_path)
            write_text(WalkForwardReportGenerator().generate(folds, summary), report_path)
            self.experiment_tracker.log(
                build_walk_forward_experiment_record(
                    summary=summary,
                    config=walk_config,
                    start_date=start_date,
                    end_date=end_date,
                    artifact_path=report_path,
                )
            )
            return summary_path
        except Exception as exc:
            errors.append(f"walk_forward_refresh_failed:{ticker}: {exc}")
            return None

    def _refresh_ml_artifact(
        self,
        config: DailyJobConfig,
        ticker: str,
        start_date: str,
        end_date: str,
        errors: list[str],
    ) -> Path | None:
        try:
            request = PriceRequest.from_strings(
                ticker=ticker, start_date=start_date, end_date=end_date
            )
            price_path = processed_price_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            feature_path = price_feature_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
            )
            if not feature_path.exists():
                errors.append(f"ml_refresh_skipped_missing_features:{ticker}: {feature_path}")
                return None

            holding_days = config.dashboard_artifact_holding_days
            training_frame = MLTrainingDatasetBuilder(
                MLTrainingDatasetConfig(holding_days=holding_days)
            ).build(
                feature_frame=read_parquet(feature_path),
                processed_price_frame=read_parquet(price_path),
            )
            dataset_path = ml_training_dataset_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
                holding_days,
            )
            write_parquet(training_frame, dataset_path)

            ml_config = MLProbabilityBaselineConfig()
            result = MLProbabilityBaselineTrainer(ml_config).train_evaluate(training_frame)
            predictions_path = ml_prediction_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
                holding_days,
            )
            metrics_path = ml_metrics_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
                holding_days,
            )
            artifact_path = ml_model_artifact_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
                holding_days,
            )
            report_path = ml_model_report_file_path(
                self.project_root,
                request.ticker,
                request.pykrx_start_date,
                request.pykrx_end_date,
                holding_days,
            )
            write_parquet(result.predictions, predictions_path)
            write_parquet(result.metrics, metrics_path)
            write_text(json.dumps(result.artifact, ensure_ascii=False, indent=2), artifact_path)
            write_text(
                MLProbabilityBaselineReportGenerator().generate(
                    result.metrics,
                    result.feature_importance,
                ),
                report_path,
            )
            self.experiment_tracker.log(
                build_ml_baseline_experiment_record(
                    metrics=result.metrics,
                    config=ml_config,
                    ticker=request.ticker,
                    start_date=start_date,
                    end_date=end_date,
                    artifact_path=report_path,
                )
            )
            return metrics_path
        except Exception as exc:
            errors.append(f"ml_refresh_failed:{ticker}: {exc}")
            return None

    def _notify(
        self,
        config: DailyJobConfig,
        summary_frame: object,
        screening_result: Any | None,
        paper_portfolio_summary: Any | None,
        kis_paper_candidates: Any | None,
        operations_health: Any | None,
    ) -> TelegramSendResult:
        message = build_daily_telegram_message(
            universe_summary=summary_frame,
            screening_result=screening_result,
            paper_portfolio_summary=paper_portfolio_summary,
            kis_paper_candidates=kis_paper_candidates,
            backtest_metrics=_load_latest_backtest_metrics(self.project_root),
            walk_forward_summary=_load_latest_walk_forward_summary(self.project_root),
            drift_result=_load_latest_drift_result(self.project_root),
            operations_health=operations_health,
            top_n=config.telegram_top_n,
        )

        if not config.notify:
            return TelegramSendResult(
                sent=False,
                dry_run=True,
                status_code=None,
                message=message,
            )

        sender = self.telegram_sender or TelegramNotifier(bot_token=None, chat_id=None)
        return sender.send_message(message, dry_run=config.telegram_dry_run)

    def _write_operations_health(self) -> tuple[Any, Path, Path]:
        result_frame = OperationsHealthChecker(self.project_root).run()
        report_name = "operations_health_latest"
        result_path = operations_health_file_path(self.project_root, report_name)
        report_path = monitoring_report_file_path(self.project_root, report_name)
        write_parquet(result_frame, result_path)
        write_text(format_operations_health_report(result_frame), report_path)
        return result_frame, result_path, report_path

    def _write_decision_journal(
        self,
        config: DailyJobConfig,
        start_date: str,
        end_date: str,
        summary_frame: Any,
        screening_frame: Any | None,
        kis_candidate_frame: Any | None,
    ) -> DecisionJournalWriteResult:
        journal_rows = build_decision_journal_frame(
            universe=config.universe,
            start_date=start_date,
            end_date=end_date,
            summary_frame=summary_frame,
            screening_frame=screening_frame,
            kis_candidate_frame=kis_candidate_frame,
        )
        return append_decision_journal(self.project_root, journal_rows)


def resolve_daily_job_date_range(config: DailyJobConfig, today: date) -> tuple[str, str]:
    if config.start_date and config.end_date:
        return config.start_date, config.end_date

    if config.lookback_days <= 0:
        raise ValueError("lookback_days must be positive.")

    end_date = config.end_date or today.isoformat()
    resolved_end = date.fromisoformat(end_date)
    start_date = (
        config.start_date or (resolved_end - timedelta(days=config.lookback_days)).isoformat()
    )
    return start_date, end_date


def _load_latest_backtest_metrics(project_root: Path) -> object | None:
    metrics_path = find_latest_backtest_metrics(project_root)
    return read_parquet(metrics_path) if metrics_path is not None else None


def _load_latest_walk_forward_summary(project_root: Path) -> object | None:
    summary_path = find_latest_walk_forward_summary(project_root)
    return read_parquet(summary_path) if summary_path is not None else None


def _load_latest_drift_result(project_root: Path) -> object | None:
    drift_path = find_latest_drift_result(project_root)
    return read_parquet(drift_path) if drift_path is not None else None


def _select_dashboard_artifact_ticker(
    summary_frame: Any,
    preferred_ticker: str | None,
    project_root: Path,
    start_date: str,
    end_date: str,
) -> str:
    if preferred_ticker:
        return preferred_ticker.strip().zfill(6)

    if summary_frame.empty:
        return ""

    frame = summary_frame.copy()
    if "status" in frame.columns:
        frame = frame[frame["status"].astype(str) == "success"]
    if frame.empty:
        return ""
    if "latest_confidence_score" in frame.columns:
        frame = frame.sort_values("latest_confidence_score", ascending=False)

    for ticker in frame["ticker"].astype(str).str.zfill(6):
        request = PriceRequest.from_strings(ticker=ticker, start_date=start_date, end_date=end_date)
        price_path = processed_price_file_path(
            project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        signal_path = final_signal_file_path(
            project_root,
            request.ticker,
            request.pykrx_start_date,
            request.pykrx_end_date,
        )
        if price_path.exists() and signal_path.exists():
            return request.ticker

    return str(frame.iloc[0]["ticker"]).zfill(6)


def _load_validation_inputs(project_root: Path, request: PriceRequest) -> tuple[Any, Any]:
    price_path = processed_price_file_path(
        project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    signal_path = final_signal_file_path(
        project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not price_path.exists():
        raise ValueError(f"Processed price file does not exist: {price_path}")
    if not signal_path.exists():
        raise ValueError(f"Final signal file does not exist: {signal_path}")
    return read_parquet(price_path), read_parquet(signal_path)


def _paper_trade_count(summary: Any | None) -> int:
    if summary is None or summary.empty:
        return 0
    return int(summary.iloc[0]["trade_count"])


def _paper_cumulative_return(summary: Any | None) -> float:
    if summary is None or summary.empty:
        return 0.0
    return float(summary.iloc[0]["cumulative_return"])


def _build_result(
    config: DailyJobConfig,
    start_date: str,
    end_date: str,
    pipeline_result: UniversePipelineResult,
    report_path: Path,
    screening_result: DailyJobScreeningResult | None,
    kis_candidate_result: DailyJobKISCandidateResult | None,
    paper_result: PaperPortfolioResult | None,
    telegram_result: TelegramSendResult,
    experiment_log_path: Path,
    operations_health_path: Path,
    operations_health_report_path: Path,
    journal_result: DecisionJournalWriteResult,
    dashboard_artifact_result: DailyJobDashboardArtifactResult,
    scoring_feature_result: DailyJobScoringFeatureResult,
) -> DailyJobResult:
    paper_summary = paper_result.summary if paper_result is not None else None
    return DailyJobResult(
        universe=config.universe,
        start_date=start_date,
        end_date=end_date,
        summary_path=pipeline_result.summary_path,
        summary_csv_path=pipeline_result.summary_csv_path,
        report_path=report_path,
        experiment_log_path=experiment_log_path,
        total_count=pipeline_result.total_count,
        success_count=pipeline_result.success_count,
        failed_count=pipeline_result.failed_count,
        paper_summary_path=paper_result.summary_path if paper_result else None,
        paper_report_path=paper_result.report_path if paper_result else None,
        paper_trade_count=_paper_trade_count(paper_summary),
        paper_cumulative_return=_paper_cumulative_return(paper_summary),
        screening_result_path=screening_result.result_path if screening_result else None,
        screening_csv_path=screening_result.csv_path if screening_result else None,
        screening_report_path=screening_result.report_path if screening_result else None,
        screening_checked_count=screening_result.checked_count if screening_result else 0,
        screening_passed_count=screening_result.passed_count if screening_result else 0,
        kis_candidate_result_path=(
            kis_candidate_result.result_path if kis_candidate_result else None
        ),
        kis_candidate_csv_path=kis_candidate_result.csv_path if kis_candidate_result else None,
        kis_candidate_report_path=(
            kis_candidate_result.report_path if kis_candidate_result else None
        ),
        kis_candidate_count=kis_candidate_result.candidate_count if kis_candidate_result else 0,
        kis_candidate_review_count=(
            kis_candidate_result.review_count if kis_candidate_result else 0
        ),
        kis_candidate_manual_price_count=(
            kis_candidate_result.manual_price_count if kis_candidate_result else 0
        ),
        manual_order_plan_path=(
            kis_candidate_result.manual_order_plan_path if kis_candidate_result else None
        ),
        manual_order_plan_csv_path=(
            kis_candidate_result.manual_order_plan_csv_path if kis_candidate_result else None
        ),
        manual_order_plan_report_path=(
            kis_candidate_result.manual_order_plan_report_path if kis_candidate_result else None
        ),
        manual_order_plan_review_count=(
            kis_candidate_result.manual_order_plan_review_count if kis_candidate_result else 0
        ),
        telegram_sent=telegram_result.sent,
        telegram_dry_run=telegram_result.dry_run,
        telegram_message=telegram_result.message,
        operations_health_path=operations_health_path,
        operations_health_report_path=operations_health_report_path,
        decision_journal_path=journal_result.parquet_path,
        decision_journal_csv_path=journal_result.csv_path,
        decision_journal_appended_count=journal_result.appended_count,
        decision_journal_total_count=journal_result.total_count,
        scoring_macro_feature_path=scoring_feature_result.macro_feature_path,
        scoring_news_feature_paths=scoring_feature_result.news_feature_paths,
        scoring_feature_errors=scoring_feature_result.errors,
        dashboard_artifact_ticker=dashboard_artifact_result.ticker,
        macro_feature_path=dashboard_artifact_result.macro_feature_path,
        backtest_metrics_path=dashboard_artifact_result.backtest_metrics_path,
        paper_single_summary_path=dashboard_artifact_result.paper_single_summary_path,
        walk_forward_summary_path=dashboard_artifact_result.walk_forward_summary_path,
        ml_metrics_path=dashboard_artifact_result.ml_metrics_path,
        dashboard_artifact_errors=dashboard_artifact_result.errors,
    )
