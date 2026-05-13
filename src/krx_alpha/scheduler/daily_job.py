from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Protocol

from krx_alpha.dashboard.data_loader import (
    find_latest_backtest_metrics,
    find_latest_drift_result,
    find_latest_walk_forward_summary,
)
from krx_alpha.database.storage import (
    read_parquet,
    universe_report_file_path,
    write_text,
)
from krx_alpha.experiments.tracker import (
    ExperimentTracker,
    build_daily_job_experiment_record,
)
from krx_alpha.pipelines.universe_pipeline import UniversePipeline, UniversePipelineResult
from krx_alpha.reports.universe_report import UniverseReportGenerator
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


@dataclass(frozen=True)
class DailyJobConfig:
    universe: str = "demo"
    start_date: str | None = None
    end_date: str | None = None
    lookback_days: int = 60
    notify: bool = True
    telegram_dry_run: bool = True
    telegram_top_n: int = 5


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
    telegram_sent: bool
    telegram_dry_run: bool
    telegram_message: str


class DailyJobRunner:
    """Run the after-market daily workflow for a named universe."""

    def __init__(
        self,
        project_root: Path,
        universe_pipeline: UniversePipeline | None = None,
        telegram_sender: TelegramMessageSender | None = None,
        experiment_tracker: ExperimentTracker | None = None,
    ) -> None:
        self.project_root = project_root
        self.universe_pipeline = universe_pipeline or UniversePipeline(project_root)
        self.telegram_sender = telegram_sender
        self.experiment_tracker = experiment_tracker or ExperimentTracker(project_root)

    def run(self, config: DailyJobConfig, today: date | None = None) -> DailyJobResult:
        start_date, end_date = resolve_daily_job_date_range(config, today or date.today())
        definition = UniverseRegistry().get(config.universe)
        pipeline_result = self.universe_pipeline.run(
            tickers=definition.tickers(),
            start_date=start_date,
            end_date=end_date,
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

        telegram_result = self._notify(config, summary_frame)
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
            )
        )
        return _build_result(
            config=config,
            start_date=start_date,
            end_date=end_date,
            pipeline_result=pipeline_result,
            report_path=report_path,
            telegram_result=telegram_result,
            experiment_log_path=experiment_log_path,
        )

    def _notify(self, config: DailyJobConfig, summary_frame: object) -> TelegramSendResult:
        message = build_daily_telegram_message(
            universe_summary=summary_frame,
            backtest_metrics=_load_latest_backtest_metrics(self.project_root),
            walk_forward_summary=_load_latest_walk_forward_summary(self.project_root),
            drift_result=_load_latest_drift_result(self.project_root),
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


def _build_result(
    config: DailyJobConfig,
    start_date: str,
    end_date: str,
    pipeline_result: UniversePipelineResult,
    report_path: Path,
    telegram_result: TelegramSendResult,
    experiment_log_path: Path,
) -> DailyJobResult:
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
        telegram_sent=telegram_result.sent,
        telegram_dry_run=telegram_result.dry_run,
        telegram_message=telegram_result.message,
    )
