import json
from datetime import date, timedelta
from pathlib import Path
from typing import Annotated, Any, cast

import pandas as pd
import typer
from rich.console import Console
from rich.table import Table

from krx_alpha.backtest.simple_backtester import BacktestConfig, SimpleBacktester
from krx_alpha.backtest.walk_forward import WalkForwardBacktester, WalkForwardConfig
from krx_alpha.broker.kis_candidates import (
    KISPaperCandidateBuilder,
    KISPaperCandidateConfig,
    enrich_screening_reference_prices,
    format_kis_paper_candidate_report,
)
from krx_alpha.broker.kis_paper import KISPaperClient, KISPaperCredentials
from krx_alpha.broker.manual_order_plan import (
    build_manual_order_plan,
    format_manual_order_plan_report,
)
from krx_alpha.collectors.dart_collector import (
    DartCompanyRequest,
    DartDisclosureSearchRequest,
    DartFinancialStatementRequest,
    OpenDartCollector,
    resolve_corp_code,
)
from krx_alpha.collectors.investor_flow_collector import (
    InvestorFlowRequest,
    PykrxInvestorFlowCollector,
)
from krx_alpha.collectors.macro_collector import FredMacroCollector, MacroRequest
from krx_alpha.collectors.news_collector import (
    NaverNewsCollector,
    NewsSearchRequest,
)
from krx_alpha.collectors.price_collector import PriceRequest, PykrxPriceCollector
from krx_alpha.configs.settings import settings
from krx_alpha.dashboard.data_loader import (
    find_latest_backtest_metrics,
    find_latest_drift_result,
    find_latest_kis_paper_candidates,
    find_latest_operations_health,
    find_latest_paper_portfolio_summary,
    find_latest_screening_result,
    find_latest_universe_summary,
    find_latest_walk_forward_summary,
    load_kis_paper_candidates,
    load_screening_result,
)
from krx_alpha.database.storage import (
    api_health_file_path,
    backtest_metrics_file_path,
    backtest_report_file_path,
    backtest_trades_file_path,
    daily_report_file_path,
    daily_score_file_path,
    dart_company_file_path,
    dart_disclosure_event_file_path,
    dart_disclosure_file_path,
    dart_financial_feature_file_path,
    dart_financial_file_path,
    data_quality_file_path,
    decision_journal_file_path,
    drift_result_file_path,
    ensure_project_dirs,
    final_signal_file_path,
    investor_flow_feature_file_path,
    kis_paper_candidate_csv_path,
    kis_paper_candidate_file_path,
    kis_paper_candidate_report_file_path,
    macro_feature_file_path,
    manual_order_plan_csv_path,
    manual_order_plan_file_path,
    manual_order_plan_report_file_path,
    market_regime_file_path,
    market_regime_report_file_path,
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
    raw_investor_flow_file_path,
    raw_macro_file_path,
    raw_news_file_path,
    raw_price_file_path,
    read_parquet,
    screening_report_file_path,
    screening_result_csv_path,
    screening_result_file_path,
    universe_csv_path,
    universe_file_path,
    universe_report_file_path,
    universe_summary_file_path,
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
    build_ml_baseline_experiment_record,
    build_walk_forward_experiment_record,
)
from krx_alpha.features.dart_disclosure_events import DartDisclosureEventBuilder
from krx_alpha.features.dart_financial_features import DartFinancialFeatureBuilder
from krx_alpha.features.investor_flow_features import InvestorFlowFeatureBuilder
from krx_alpha.features.macro_features import MacroFeatureBuilder
from krx_alpha.features.news_sentiment import NewsSentimentConfig, NewsSentimentFeatureBuilder
from krx_alpha.features.price_features import PriceFeatureBuilder
from krx_alpha.journal.decision_journal import (
    append_decision_journal,
    build_decision_journal_frame,
    evaluate_decision_journal,
)
from krx_alpha.models.probability_baseline import (
    MLProbabilityBaselineConfig,
    MLProbabilityBaselineTrainer,
)
from krx_alpha.models.training_dataset import MLTrainingDatasetBuilder, MLTrainingDatasetConfig
from krx_alpha.monitoring.api_health import (
    API_STATUS_FAILED,
    API_STATUS_MISSING,
    API_STATUS_OK,
    ApiCredentials,
    ApiHealthChecker,
    api_results_to_frame,
    format_api_health_report,
)
from krx_alpha.monitoring.data_quality import (
    PriceDataQualityChecker,
    PriceQualityConfig,
    format_data_quality_report,
    summarize_quality,
)
from krx_alpha.monitoring.drift import (
    DataDriftConfig,
    DataDriftDetector,
    PerformanceDriftConfig,
    PerformanceDriftDetector,
    format_data_drift_report,
    format_performance_drift_report,
)
from krx_alpha.monitoring.operations_health import (
    HEALTH_STATUS_EMPTY,
    HEALTH_STATUS_FAILED,
    HEALTH_STATUS_MISSING,
    HEALTH_STATUS_OK,
    HEALTH_STATUS_STALE,
    HEALTH_STATUS_WARN,
    OperationsHealthChecker,
    OperationsHealthConfig,
    format_operations_health_report,
    summarize_operations_health,
)
from krx_alpha.paper_trading.portfolio import PaperPortfolioConfig, run_paper_portfolio
from krx_alpha.paper_trading.simulator import PaperTradingConfig, PaperTradingSimulator
from krx_alpha.pipelines.daily_pipeline import DailyPipeline
from krx_alpha.pipelines.universe_pipeline import UniversePipeline
from krx_alpha.processors.price_processor import PriceProcessor
from krx_alpha.regime.market_regime import MarketRegimeAnalyzer
from krx_alpha.reports.backtest_report import BacktestReportGenerator, WalkForwardReportGenerator
from krx_alpha.reports.daily_report import DailyReportGenerator
from krx_alpha.reports.ml_report import MLProbabilityBaselineReportGenerator
from krx_alpha.reports.paper_trading_report import PaperTradingReportGenerator
from krx_alpha.reports.regime_report import MarketRegimeReportGenerator
from krx_alpha.reports.single_stock_analysis import (
    SOURCE_KO,
    AnalysisItem,
    build_single_stock_analysis,
    format_single_stock_analysis_message,
    label_with_raw,
)
from krx_alpha.reports.universe_report import UniverseReportGenerator
from krx_alpha.scheduler.daily_job import DailyJobConfig, DailyJobRunner
from krx_alpha.scoring.price_scorer import PriceScorer
from krx_alpha.screening.auto_screener import (
    AutoScreener,
    AutoScreenerConfig,
    format_screening_report,
)
from krx_alpha.signals.signal_engine import SignalEngine
from krx_alpha.telegram.notifier import (
    TelegramNotifier,
    build_daily_telegram_message,
    split_telegram_message,
)
from krx_alpha.universe.static_universe import UniverseRegistry
from krx_alpha.universe.stock_resolver import AmbiguousStockQuery, StockCandidate, StockResolver
from krx_alpha.utils.logger import configure_logger

app = typer.Typer(help="KRX Alpha Platform command line interface")
console = Console()


def _kis_token_cache_path() -> Path:
    return settings.project_root / ".cache" / "kis_paper_token.json"


class _CLIKISPaperCandidateSource:
    def __init__(
        self,
        project_root: Path,
        credentials: KISPaperCredentials,
        timeout_seconds: float,
        token_cache_path: Path | None = None,
    ) -> None:
        self.project_root = project_root
        self.credentials = credentials
        self.timeout_seconds = timeout_seconds
        self.token_cache_path = token_cache_path

    def build_candidates(
        self,
        screening_frame: Any,
        *,
        max_candidates: int,
        cash_buffer_pct: float,
    ) -> Any:
        enriched_frame = enrich_screening_reference_prices(screening_frame, self.project_root)
        client = KISPaperClient(
            self.credentials,
            timeout_seconds=self.timeout_seconds,
            token_cache_path=self.token_cache_path,
        )
        token = client.issue_access_token()
        balance = client.inquire_balance(token)
        return KISPaperCandidateBuilder(
            KISPaperCandidateConfig(
                max_candidates=max_candidates,
                cash_buffer_pct=cash_buffer_pct,
            )
        ).build(enriched_frame, balance)


def _load_financial_feature_frame(
    ticker: str,
    corp_code: str | None,
    financial_year: str | None,
    financial_report_code: str,
) -> pd.DataFrame | None:
    if financial_year is None:
        return None

    normalized_ticker = ticker.zfill(6)
    resolved_corp_code = resolve_corp_code(normalized_ticker, corp_code)
    financial_path = dart_financial_feature_file_path(
        settings.project_root,
        resolved_corp_code,
        financial_year,
        financial_report_code,
    )
    if not financial_path.exists():
        raise typer.BadParameter(
            "DART financial feature file does not exist. "
            "Run build-dart-financial-features first: "
            f"{financial_path}"
        )
    return read_parquet(financial_path)


def _load_disclosure_event_frame(
    ticker: str,
    corp_code: str | None,
    event_start: str | None,
    event_end: str,
) -> pd.DataFrame | None:
    if event_start is None:
        return None

    normalized_ticker = ticker.zfill(6)
    resolved_corp_code = resolve_corp_code(normalized_ticker, corp_code)
    event_path = dart_disclosure_event_file_path(
        settings.project_root,
        resolved_corp_code,
        event_start.replace("-", ""),
        event_end.replace("-", ""),
    )
    if not event_path.exists():
        raise typer.BadParameter(
            "DART disclosure event feature file does not exist. "
            "Run build-dart-disclosure-events first: "
            f"{event_path}"
        )
    return read_parquet(event_path)


def _load_investor_flow_feature_frame(
    ticker: str,
    flow_start: str | None,
    flow_end: str,
) -> pd.DataFrame | None:
    if flow_start is None:
        return None

    normalized_ticker = ticker.zfill(6)
    flow_path = investor_flow_feature_file_path(
        settings.project_root,
        normalized_ticker,
        flow_start.replace("-", ""),
        flow_end.replace("-", ""),
    )
    if not flow_path.exists():
        raise typer.BadParameter(
            "Investor flow feature file does not exist. "
            "Run build-investor-flow-features first: "
            f"{flow_path}"
        )
    return read_parquet(flow_path)


def _load_news_sentiment_feature_frame(
    ticker: str,
    news_start: str | None,
    news_end: str,
) -> pd.DataFrame | None:
    if news_start is None:
        return None

    normalized_ticker = ticker.zfill(6)
    news_path = news_sentiment_feature_file_path(
        settings.project_root,
        normalized_ticker,
        news_start.replace("-", ""),
        news_end.replace("-", ""),
    )
    if not news_path.exists():
        raise typer.BadParameter(
            "News sentiment feature file does not exist. "
            "Run build-news-sentiment first: "
            f"{news_path}"
        )
    return read_parquet(news_path)


def _load_macro_feature_frame(
    macro_start: str | None,
    macro_end: str,
    macro_series: str,
) -> pd.DataFrame | None:
    if macro_start is None:
        return None

    request = MacroRequest.from_strings(
        start_date=macro_start,
        end_date=macro_end,
        series_ids=macro_series,
    )
    macro_path = macro_feature_file_path(
        settings.project_root,
        request.compact_start_date,
        request.compact_end_date,
        request.series_slug,
    )
    if not macro_path.exists():
        raise typer.BadParameter(
            f"Macro feature file does not exist. Run build-macro-features first: {macro_path}"
        )
    return read_parquet(macro_path)


def _resolve_cli_date_range(
    start: str | None,
    end: str | None,
    lookback_days: int,
) -> tuple[str, str]:
    if lookback_days <= 0:
        raise typer.BadParameter("lookback_days must be positive.")

    try:
        resolved_end = date.fromisoformat(end) if end is not None else date.today()
        resolved_start = (
            date.fromisoformat(start)
            if start is not None
            else resolved_end - timedelta(days=lookback_days)
        )
    except ValueError as exc:
        raise typer.BadParameter("Date must be in YYYY-MM-DD format.") from exc

    if resolved_start > resolved_end:
        raise typer.BadParameter("start date must be earlier than or equal to end date.")

    return resolved_start.isoformat(), resolved_end.isoformat()


def _select_single_stock_query(
    query: str | None,
    name: str | None,
    ticker: str | None,
) -> str:
    provided_queries = [
        value.strip() for value in (query, name, ticker) if value is not None and value.strip()
    ]
    if not provided_queries:
        raise typer.BadParameter(
            "분석할 종목명이나 종목코드가 필요합니다. 예: python main.py analyze-stock 삼성전자"
        )
    if len(provided_queries) > 1:
        raise typer.BadParameter(
            "종목은 한 번에 하나만 입력하세요. 일반 입력, --name, --ticker 중 하나만 사용합니다."
        )
    return provided_queries[0]


def _print_stock_candidate_table(candidates: list[StockCandidate]) -> None:
    table = Table(title="검색된 종목 후보")
    table.add_column("종목코드")
    table.add_column("종목명")
    table.add_column("출처")
    for candidate in candidates:
        table.add_row(
            candidate.ticker,
            candidate.name,
            label_with_raw(candidate.source, SOURCE_KO),
        )
    console.print(table)


def _print_analysis_items(title: str, items: list[AnalysisItem]) -> None:
    table = Table(title=title)
    table.add_column("항목", style="bold")
    table.add_column("값")
    table.add_column("해석")
    for item in items:
        table.add_row(item.label, item.value, item.interpretation)
    console.print(table)


def _print_beginner_notes(notes: list[str]) -> None:
    table = Table(title="초보자용 해석 메모")
    table.add_column("확인할 내용")
    for note in notes:
        table.add_row(note)
    console.print(table)


def _send_telegram_message_parts(
    message_parts: list[str],
    *,
    dry_run: bool,
    success_message: str,
    dry_run_message: str,
) -> None:
    notifier = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
        timeout_seconds=settings.telegram_timeout_seconds,
        max_retries=settings.telegram_max_retries,
        retry_sleep_seconds=settings.telegram_retry_sleep_seconds,
    )

    try:
        results = [notifier.send_message(part, dry_run=dry_run) for part in message_parts]
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    except RuntimeError as exc:
        console.print("[bold red]Telegram send failed.[/bold red]")
        console.print(str(exc))
        if _contains_certificate_error(exc):
            console.print(
                "[yellow]SSL certificate verification failed. "
                "Check antivirus HTTPS inspection, proxy certificates, or Windows/Python "
                "certificate trust settings. The message was not sent.[/yellow]"
            )
        raise typer.Exit(code=1) from exc

    if dry_run:
        console.print(f"[bold yellow]{dry_run_message}[/bold yellow]")
        for index, result in enumerate(results, start=1):
            console.print(f"[bold]Telegram message part {index}/{len(results)}[/bold]")
            console.print(result.message)
        return

    console.print(f"[bold green]{success_message}[/bold green]")
    console.print(f"Messages sent: {len(results)}")
    if results:
        console.print(f"Last status code: {results[-1].status_code}")


@app.command()
def doctor() -> None:
    """Check whether the local project environment is ready."""
    configure_logger(settings.log_level)
    ensure_project_dirs(settings.project_root)

    console.print("[bold green]KRX Alpha Platform is ready.[/bold green]")
    console.print(f"Environment: {settings.environment}")
    console.print(f"Project root: {settings.project_root}")
    console.print(f"Data root: {settings.data_dir}")


@app.command("check-apis")
def check_apis(
    timeout_seconds: Annotated[
        float,
        typer.Option("--timeout-seconds", help="HTTP timeout for each API check."),
    ] = 10.0,
    include_pykrx: Annotated[
        bool,
        typer.Option("--include-pykrx/--skip-pykrx", help="Also check pykrx price collection."),
    ] = True,
    strict: Annotated[
        bool,
        typer.Option("--strict/--no-strict", help="Exit with code 1 when any check is not OK."),
    ] = False,
    save: Annotated[
        bool,
        typer.Option("--save/--no-save", help="Save API health parquet and Markdown report."),
    ] = False,
    output_name: Annotated[
        str,
        typer.Option("--output-name", help="Output report name without extension when saving."),
    ] = "api_health_latest",
) -> None:
    """Check configured API connectivity without printing secret values."""
    configure_logger(settings.log_level)
    credentials = ApiCredentials.from_settings(settings)
    results = ApiHealthChecker(
        timeout_seconds=timeout_seconds,
        kis_token_cache_path=_kis_token_cache_path(),
    ).run(
        credentials,
        include_pykrx=include_pykrx,
    )

    table = Table(title="API Health Check")
    table.add_column("API")
    table.add_column("Status")
    table.add_column("Detail")
    table.add_column("Action")
    for result in results:
        table.add_row(result.name, _format_api_status(result.status), result.detail, result.action)
    console.print(table)

    ok_count = sum(result.ok for result in results)
    console.print(f"Summary: {ok_count}/{len(results)} OK")

    if save:
        report_name = _safe_report_name(output_name)
        result_path = api_health_file_path(settings.project_root, report_name)
        report_path = monitoring_report_file_path(settings.project_root, report_name)
        write_parquet(api_results_to_frame(results), result_path)
        write_text(format_api_health_report(results), report_path)
        console.print(f"Result: {result_path}")
        console.print(f"Report: {report_path}")

    if strict and ok_count != len(results):
        raise typer.Exit(code=1)


@app.command("check-operations")
def check_operations(
    freshness_hours: Annotated[
        float,
        typer.Option("--freshness-hours", help="Maximum acceptable artifact age in hours."),
    ] = 36.0,
    include_apis: Annotated[
        bool,
        typer.Option(
            "--include-apis/--skip-apis",
            help="Also include API connectivity checks. Skipped by default to avoid network calls.",
        ),
    ] = False,
    include_pykrx: Annotated[
        bool,
        typer.Option(
            "--include-pykrx/--skip-pykrx", help="Also check pykrx when APIs are checked."
        ),
    ] = False,
    timeout_seconds: Annotated[
        float,
        typer.Option("--timeout-seconds", help="HTTP timeout for each API check."),
    ] = 10.0,
    output_name: Annotated[
        str,
        typer.Option("--output-name", help="Output report name without extension."),
    ] = "operations_health_latest",
    strict: Annotated[
        bool,
        typer.Option("--strict/--no-strict", help="Exit with code 1 when any check is not OK."),
    ] = False,
) -> None:
    """Check local pipeline artifacts and write an operations health report."""
    configure_logger(settings.log_level)
    ensure_project_dirs(settings.project_root)

    api_results = None
    if include_apis:
        api_results = ApiHealthChecker(
            timeout_seconds=timeout_seconds,
            kis_token_cache_path=_kis_token_cache_path(),
        ).run(
            ApiCredentials.from_settings(settings),
            include_pykrx=include_pykrx,
        )

    result_frame = OperationsHealthChecker(
        project_root=settings.project_root,
        config=OperationsHealthConfig(freshness_hours=freshness_hours),
    ).run(api_results=api_results)

    report_name = _safe_report_name(output_name)
    result_path = operations_health_file_path(settings.project_root, report_name)
    report_path = monitoring_report_file_path(settings.project_root, report_name)
    write_parquet(result_frame, result_path)
    write_text(format_operations_health_report(result_frame), report_path)

    summary = summarize_operations_health(result_frame)
    table = Table(title="Operations Health")
    table.add_column("Check")
    table.add_column("Category")
    table.add_column("Status")
    table.add_column("Rows", justify="right")
    table.add_column("Age Hours", justify="right")
    table.add_column("Detail")
    table.add_column("Action")
    for _, row in result_frame.sort_values(["severity", "category", "check_name"]).iterrows():
        table.add_row(
            str(row["check_name"]),
            str(row["category"]),
            _format_health_status(str(row["status"])),
            _format_optional_table_value(row["row_count"], decimals=0),
            _format_optional_table_value(row["age_hours"], decimals=2),
            str(row["detail"]),
            str(row.get("action", "")),
        )
    console.print(table)
    console.print(
        "Summary: "
        f"{summary['ok']} OK, {summary['warnings']} warning(s), "
        f"{summary['problems']} problem(s)"
    )
    console.print(f"Result: {result_path}")
    console.print(f"Report: {report_path}")

    if strict and summary["ok"] != summary["total"]:
        raise typer.Exit(code=1)


@app.command("check-price-quality")
def check_price_quality(
    input_path: Annotated[
        Path,
        typer.Option("--input-path", help="Raw or processed price parquet path to check."),
    ],
    dataset_name: Annotated[
        str,
        typer.Option("--dataset-name", help="Dataset label stored in the quality result."),
    ] = "price",
    output_name: Annotated[
        str | None,
        typer.Option("--output-name", help="Output report name without extension."),
    ] = None,
    max_abs_return: Annotated[
        float,
        typer.Option("--max-abs-return", help="Warn when absolute close return exceeds this."),
    ] = 0.30,
    max_calendar_gap_days: Annotated[
        int,
        typer.Option("--max-calendar-gap-days", help="Warn when ticker date gaps exceed this."),
    ] = 7,
    strict: Annotated[
        bool,
        typer.Option("--strict/--no-strict", help="Exit with code 1 when any check fails."),
    ] = False,
) -> None:
    """Run practical data quality checks for daily price datasets."""
    if not input_path.exists():
        raise typer.BadParameter(f"Input file does not exist: {input_path}")

    frame = read_parquet(input_path)
    result_frame = PriceDataQualityChecker(
        PriceQualityConfig(
            max_abs_return=max_abs_return,
            max_calendar_gap_days=max_calendar_gap_days,
        )
    ).check(frame, dataset=dataset_name)

    report_name = _safe_report_name(output_name or f"price_quality_{input_path.stem}")
    result_path = data_quality_file_path(settings.project_root, report_name)
    report_path = monitoring_report_file_path(settings.project_root, report_name)
    write_parquet(result_frame, result_path)
    write_text(format_data_quality_report(result_frame), report_path)

    summary = summarize_quality(result_frame)
    table = Table(title="Price Data Quality")
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Rows", justify="right")
    table.add_column("Affected %", justify="right")
    table.add_column("Detail")
    table.add_column("Action")
    for _, row in result_frame.iterrows():
        table.add_row(
            str(row["check_name"]),
            str(row["status"]),
            str(int(row["affected_rows"])),
            f"{float(row['affected_pct']) * 100:.2f}%",
            str(row["detail"]),
            str(row["action"]),
        )
    console.print(table)
    console.print(
        f"Summary: {summary['pass']} pass, {summary['warn']} warning(s), {summary['fail']} fail(s)"
    )
    console.print(f"Result: {result_path}")
    console.print(f"Report: {report_path}")

    if strict and summary["fail"] > 0:
        raise typer.Exit(code=1)


@app.command("kis-paper-token-check")
def kis_paper_token_check(
    timeout_seconds: Annotated[
        float,
        typer.Option("--timeout-seconds", help="HTTP timeout for the KIS paper token check."),
    ] = 10.0,
) -> None:
    """Issue a KIS mock-investment access token without printing the secret token."""
    configure_logger(settings.log_level)
    try:
        credentials = KISPaperCredentials.from_settings(settings)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    client = KISPaperClient(
        credentials,
        timeout_seconds=timeout_seconds,
        token_cache_path=_kis_token_cache_path(),
    )
    token = client.issue_access_token()

    console.print("[bold green]KIS paper token check succeeded.[/bold green]")
    console.print(f"Account: {credentials.account_id.normalized}")
    console.print(f"Token type: {token.token_type}")
    console.print(f"Token: {token.redacted}")
    console.print(
        "Expires in: "
        + (f"{token.expires_in_seconds} second(s)" if token.expires_in_seconds else "unknown")
    )
    console.print("Mode: paper trading only. No order was sent.")


@app.command("kis-paper-balance")
def kis_paper_balance(
    timeout_seconds: Annotated[
        float,
        typer.Option("--timeout-seconds", help="HTTP timeout for the KIS paper balance check."),
    ] = 10.0,
) -> None:
    """Read KIS mock-investment account balance without sending any order."""
    configure_logger(settings.log_level)
    try:
        credentials = KISPaperCredentials.from_settings(settings)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    client = KISPaperClient(
        credentials,
        timeout_seconds=timeout_seconds,
        token_cache_path=_kis_token_cache_path(),
    )
    token = client.issue_access_token()
    balance = client.inquire_balance(token)

    console.print("[bold green]KIS paper balance loaded.[/bold green]")
    console.print(f"Account: {balance.account}")
    console.print(f"Cash: {balance.cash_amount:,.0f}")
    console.print(f"Stock evaluation: {balance.stock_evaluation_amount:,.0f}")
    console.print(f"Total evaluation: {balance.total_evaluation_amount:,.0f}")
    console.print(f"P/L: {balance.profit_loss_amount:,.0f} ({balance.profit_loss_rate:.2f}%)")

    table = Table(title="KIS Paper Holdings")
    table.add_column("Ticker")
    table.add_column("Name")
    table.add_column("Qty", justify="right")
    table.add_column("Orderable", justify="right")
    table.add_column("Avg", justify="right")
    table.add_column("Price", justify="right")
    table.add_column("Eval", justify="right")
    table.add_column("P/L", justify="right")
    table.add_column("P/L %", justify="right")
    for holding in balance.holdings:
        table.add_row(
            holding.ticker,
            holding.name,
            f"{holding.quantity:,}",
            f"{holding.orderable_quantity:,}",
            f"{holding.average_price:,.0f}",
            f"{holding.current_price:,.0f}",
            f"{holding.evaluation_amount:,.0f}",
            f"{holding.profit_loss_amount:,.0f}",
            f"{holding.profit_loss_rate:.2f}",
        )
    console.print(table)
    console.print("Mode: paper trading only. No order was sent.")


@app.command("build-kis-paper-candidates")
def build_kis_paper_candidates(
    screening_path: Annotated[
        Path | None,
        typer.Option(
            "--screening-path",
            help="Auto-screener result parquet path. Uses the latest result when omitted.",
        ),
    ] = None,
    output_name: Annotated[
        str | None,
        typer.Option("--output-name", help="Output artifact name without extension."),
    ] = None,
    max_candidates: Annotated[
        int,
        typer.Option("--max-candidates", help="Maximum rows to convert into review candidates."),
    ] = 10,
    cash_buffer_pct: Annotated[
        float,
        typer.Option("--cash-buffer-pct", help="Cash percentage reserved from candidate sizing."),
    ] = 5.0,
    timeout_seconds: Annotated[
        float,
        typer.Option("--timeout-seconds", help="HTTP timeout for KIS paper balance inquiry."),
    ] = 10.0,
) -> None:
    """Build KIS mock-account review candidates without sending any order."""
    configure_logger(settings.log_level)
    resolved_screening_path = screening_path or find_latest_screening_result(settings.project_root)
    if resolved_screening_path is None or not resolved_screening_path.exists():
        raise typer.BadParameter(
            "Screening result file does not exist. "
            "Run screen-universe first or pass --screening-path."
        )

    try:
        credentials = KISPaperCredentials.from_settings(settings)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    screening_frame = load_screening_result(resolved_screening_path)
    screening_frame = enrich_screening_reference_prices(screening_frame, settings.project_root)

    client = KISPaperClient(
        credentials,
        timeout_seconds=timeout_seconds,
        token_cache_path=_kis_token_cache_path(),
    )
    token = client.issue_access_token()
    balance = client.inquire_balance(token)

    result_frame = KISPaperCandidateBuilder(
        KISPaperCandidateConfig(
            max_candidates=max_candidates,
            cash_buffer_pct=cash_buffer_pct,
        )
    ).build(screening_frame, balance)

    report_name = _safe_report_name(
        output_name or f"kis_paper_candidates_{resolved_screening_path.stem}"
    )
    result_path = kis_paper_candidate_file_path(settings.project_root, report_name)
    csv_path = kis_paper_candidate_csv_path(settings.project_root, report_name)
    report_path = kis_paper_candidate_report_file_path(settings.project_root, report_name)
    write_parquet(result_frame, result_path)
    write_csv(result_frame, csv_path)
    write_text(format_kis_paper_candidate_report(result_frame), report_path)
    manual_plan_frame = build_manual_order_plan(result_frame)
    manual_plan_name = _safe_report_name(f"manual_order_plan_{report_name}")
    manual_plan_path = manual_order_plan_file_path(settings.project_root, manual_plan_name)
    manual_plan_csv = manual_order_plan_csv_path(settings.project_root, manual_plan_name)
    manual_plan_report = manual_order_plan_report_file_path(settings.project_root, manual_plan_name)
    write_parquet(manual_plan_frame, manual_plan_path)
    write_csv(manual_plan_frame, manual_plan_csv)
    write_text(format_manual_order_plan_report(manual_plan_frame), manual_plan_report)

    review_count = int(result_frame["candidate_action"].isin(["review_buy", "review_add"]).sum())
    manual_count = int((result_frame["candidate_action"] == "manual_price_required").sum())
    manual_plan_review_count = (
        int(
            manual_plan_frame["manual_plan_action"]
            .isin(["manual_buy_review", "manual_add_review"])
            .sum()
        )
        if not manual_plan_frame.empty
        else 0
    )
    console.print("[bold green]KIS paper review candidates generated.[/bold green]")
    console.print(f"Screening: {resolved_screening_path}")
    console.print(f"Account: {balance.account}")
    console.print(f"Cash: {balance.cash_amount:,.0f}")
    console.print(f"Total evaluation: {balance.total_evaluation_amount:,.0f}")
    console.print(f"Review buy/add: {review_count}")
    console.print(f"Manual price checks: {manual_count}")
    console.print(f"Result: {result_path}")
    console.print(f"CSV: {csv_path}")
    console.print(f"Report: {report_path}")
    console.print(f"Manual order plan: {manual_plan_path}")
    console.print(f"Manual order plan CSV: {manual_plan_csv}")
    console.print(f"Manual order plan report: {manual_plan_report}")
    console.print(f"Manual buy/add review rows: {manual_plan_review_count}")
    console.print("Mode: paper trading review only. No order was sent.")
    if not result_frame.empty:
        display_columns = [
            "ticker",
            "candidate_action",
            "estimated_quantity",
            "estimated_amount",
            "target_position_pct",
            "confidence_score",
            "reason",
        ]
        console.print(result_frame[display_columns].to_string(index=False))


@app.command("build-manual-order-plan")
def build_manual_order_plan_command(
    kis_candidate_path: Annotated[
        Path | None,
        typer.Option(
            "--kis-candidate-path",
            help="KIS candidate parquet path. Uses the latest result when omitted.",
        ),
    ] = None,
    output_name: Annotated[
        str | None,
        typer.Option("--output-name", help="Output artifact name without extension."),
    ] = None,
) -> None:
    """Build a human-only manual order checklist without sending any order."""
    configure_logger(settings.log_level)
    resolved_path = kis_candidate_path or find_latest_kis_paper_candidates(settings.project_root)
    if resolved_path is None or not resolved_path.exists():
        raise typer.BadParameter(
            "KIS candidate file does not exist. "
            "Run build-kis-paper-candidates or run-daily-job with --kis-paper-candidates first."
        )

    candidate_frame = load_kis_paper_candidates(resolved_path)
    plan_frame = build_manual_order_plan(candidate_frame)
    report_name = _safe_report_name(output_name or f"manual_order_plan_{resolved_path.stem}")
    plan_path = manual_order_plan_file_path(settings.project_root, report_name)
    csv_path = manual_order_plan_csv_path(settings.project_root, report_name)
    report_path = manual_order_plan_report_file_path(settings.project_root, report_name)
    write_parquet(plan_frame, plan_path)
    write_csv(plan_frame, csv_path)
    write_text(format_manual_order_plan_report(plan_frame), report_path)

    review_count = (
        int(plan_frame["manual_plan_action"].isin(["manual_buy_review", "manual_add_review"]).sum())
        if not plan_frame.empty
        else 0
    )
    console.print("[bold green]Manual order plan generated.[/bold green]")
    console.print(f"Source KIS candidates: {resolved_path}")
    console.print(f"Rows: {len(plan_frame)}")
    console.print(f"Manual buy/add review rows: {review_count}")
    console.print(f"Result: {plan_path}")
    console.print(f"CSV: {csv_path}")
    console.print(f"Report: {report_path}")
    console.print("Mode: manual review only. No broker API or order endpoint was called.")
    if not plan_frame.empty:
        display_columns = [
            "ticker",
            "manual_plan_action",
            "estimated_quantity",
            "estimated_amount",
            "reference_price",
            "manual_status",
            "orders_sent",
        ]
        console.print(plan_frame[display_columns].to_string(index=False))


@app.command("init-dirs")
def init_dirs(project_root: Path | None = None) -> None:
    """Create local data, model, and log directories."""
    root = project_root or settings.project_root
    ensure_project_dirs(root)
    console.print(f"[green]Initialized project directories under {root}[/green]")


@app.command("collect-price")
def collect_price(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    adjusted: Annotated[
        bool,
        typer.Option("--adjusted/--raw-price", help="Use adjusted OHLCV data."),
    ] = True,
) -> None:
    """Collect daily OHLCV data from pykrx and save it as a raw parquet file."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(
        ticker=ticker,
        start_date=start,
        end_date=end,
        adjusted=adjusted,
    )

    try:
        frame = PykrxPriceCollector().collect(request)
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    output_path = raw_price_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(frame, output_path)

    console.print(f"[green]Saved {len(frame)} rows[/green]")
    console.print(f"Ticker: {request.ticker}")
    console.print(f"Output: {output_path}")


@app.command("collect-investor-flow")
def collect_investor_flow(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    demo: Annotated[
        bool,
        typer.Option("--demo/--live", help="Use built-in demo flow data instead of live pykrx."),
    ] = True,
) -> None:
    """Collect daily investor flow data and save it as a raw parquet file."""
    configure_logger(settings.log_level)
    request = InvestorFlowRequest.from_strings(
        ticker=ticker,
        start_date=start,
        end_date=end,
        demo=demo,
    )

    try:
        frame = PykrxInvestorFlowCollector().collect(request)
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    output_path = raw_investor_flow_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(frame, output_path)

    latest = frame.sort_values("date").iloc[-1]
    console.print("[bold green]Collected investor flow data.[/bold green]")
    console.print(f"Ticker: {request.ticker}")
    console.print(f"Rows: {len(frame)}")
    console.print(f"Latest foreign net buy value: {latest['foreign_net_buy_value']:,.0f}")
    console.print(f"Latest institution net buy value: {latest['institution_net_buy_value']:,.0f}")
    console.print(f"Output: {output_path}")


@app.command("collect-news")
def collect_news(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="News window start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="News window end date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    query: Annotated[
        str | None,
        typer.Option("--query", help="News search query. Defaults to a ticker-aware query."),
    ] = None,
    display: Annotated[
        int,
        typer.Option("--display", help="Number of Naver news search items to request."),
    ] = 10,
    demo: Annotated[
        bool,
        typer.Option("--demo/--live", help="Use built-in demo news instead of live Naver API."),
    ] = True,
) -> None:
    """Collect Naver news search results and save raw news data."""
    configure_logger(settings.log_level)
    request = NewsSearchRequest.from_strings(
        ticker=ticker,
        start_date=start,
        end_date=end,
        query=query,
        display=display,
        demo=demo,
    )

    try:
        frame = NaverNewsCollector(
            client_id=settings.naver_client_id,
            client_secret=settings.naver_client_secret,
        ).collect(request)
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    output_path = raw_news_file_path(
        settings.project_root,
        request.ticker,
        request.compact_start_date,
        request.compact_end_date,
    )
    write_parquet(frame, output_path)

    latest = frame.sort_values("published_at").iloc[-1]
    console.print("[bold green]Collected news data.[/bold green]")
    console.print(f"Ticker: {request.ticker}")
    console.print(f"Query: {request.query}")
    console.print(f"Rows: {len(frame)}")
    console.print(f"Latest headline: {latest['title']}")
    console.print(f"Source: {latest['source']}")
    console.print(f"Output: {output_path}")


@app.command("collect-macro")
def collect_macro(
    start: Annotated[
        str,
        typer.Option("--start", help="Macro window start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="Macro window end date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    series: Annotated[
        str,
        typer.Option("--series", help="Comma-separated FRED series ids."),
    ] = "DGS10,DFF,DEXKOUS",
    demo: Annotated[
        bool,
        typer.Option("--demo/--live", help="Use built-in demo macro data instead of live FRED."),
    ] = True,
) -> None:
    """Collect FRED macro observations and save raw macro data."""
    configure_logger(settings.log_level)
    request = MacroRequest.from_strings(
        start_date=start,
        end_date=end,
        series_ids=series,
        demo=demo,
    )
    try:
        frame = FredMacroCollector(api_key=settings.fred_api_key).collect(request)
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    output_path = raw_macro_file_path(
        settings.project_root,
        request.compact_start_date,
        request.compact_end_date,
        request.series_slug,
    )
    write_parquet(frame, output_path)

    latest = frame.sort_values(["date", "series_id"]).iloc[-1]
    console.print("[bold green]Collected FRED macro data.[/bold green]")
    console.print(f"Series: {', '.join(request.series_ids)}")
    console.print(f"Rows: {len(frame)}")
    console.print(f"Latest observation: {latest['series_id']} = {float(latest['value']):.4f}")
    console.print(f"Source: {latest['source']}")
    console.print(f"Output: {output_path}")


@app.command("collect-dart-company")
def collect_dart_company(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    corp_code: Annotated[
        str | None,
        typer.Option("--corp-code", help="OpenDART corporation code. Example: 00126380"),
    ] = None,
    demo: Annotated[
        bool,
        typer.Option("--demo/--live", help="Use built-in demo data instead of live OpenDART."),
    ] = True,
) -> None:
    """Collect OpenDART company overview data."""
    configure_logger(settings.log_level)
    normalized_ticker = ticker.zfill(6)
    resolved_corp_code = resolve_corp_code(normalized_ticker, corp_code)
    request = DartCompanyRequest(
        corp_code=resolved_corp_code,
        ticker=normalized_ticker,
        demo=demo,
    )
    frame = OpenDartCollector(api_key=settings.dart_api_key).collect_company(request)
    output_path = dart_company_file_path(settings.project_root, resolved_corp_code)
    write_parquet(frame, output_path)

    latest = frame.iloc[0]
    console.print("[bold green]Collected DART company overview.[/bold green]")
    console.print(f"Ticker: {latest['stock_code']}")
    console.print(f"Corp code: {latest['corp_code']}")
    console.print(f"Company: {latest['corp_name']}")
    console.print(f"Source: {latest['source']}")
    console.print(f"Output: {output_path}")


@app.command("collect-dart-financials")
def collect_dart_financials(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    corp_code: Annotated[
        str | None,
        typer.Option("--corp-code", help="OpenDART corporation code. Example: 00126380"),
    ] = None,
    year: Annotated[
        str,
        typer.Option("--year", help="Business year. Example: 2023"),
    ] = "2023",
    report_code: Annotated[
        str,
        typer.Option("--report-code", help="OpenDART report code. 11011 is annual report."),
    ] = "11011",
    fs_div: Annotated[
        str,
        typer.Option("--fs-div", help="Financial statement division. CFS or OFS."),
    ] = "CFS",
    demo: Annotated[
        bool,
        typer.Option("--demo/--live", help="Use built-in demo data instead of live OpenDART."),
    ] = True,
) -> None:
    """Collect OpenDART single-company financial statement accounts."""
    configure_logger(settings.log_level)
    normalized_ticker = ticker.zfill(6)
    resolved_corp_code = resolve_corp_code(normalized_ticker, corp_code)
    request = DartFinancialStatementRequest(
        corp_code=resolved_corp_code,
        ticker=normalized_ticker,
        bsns_year=year,
        reprt_code=report_code,
        fs_div=fs_div,
        demo=demo,
    )
    frame = OpenDartCollector(api_key=settings.dart_api_key).collect_financial_statement(request)
    output_path = dart_financial_file_path(
        settings.project_root,
        resolved_corp_code,
        year,
        report_code,
    )
    write_parquet(frame, output_path)

    console.print("[bold green]Collected DART financial statements.[/bold green]")
    console.print(f"Ticker: {normalized_ticker}")
    console.print(f"Corp code: {resolved_corp_code}")
    console.print(f"Rows: {len(frame)}")
    console.print(f"Source: {frame.iloc[0]['source']}")
    console.print(f"Output: {output_path}")


@app.command("collect-dart-disclosures")
def collect_dart_disclosures(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    corp_code: Annotated[
        str | None,
        typer.Option("--corp-code", help="OpenDART corporation code. Example: 00126380"),
    ] = None,
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    page_count: Annotated[
        int,
        typer.Option("--page-count", help="OpenDART disclosure page count."),
    ] = 20,
    demo: Annotated[
        bool,
        typer.Option("--demo/--live", help="Use built-in demo data instead of live OpenDART."),
    ] = True,
) -> None:
    """Collect OpenDART disclosure list data."""
    configure_logger(settings.log_level)
    normalized_ticker = ticker.zfill(6)
    resolved_corp_code = resolve_corp_code(normalized_ticker, corp_code)
    request = DartDisclosureSearchRequest(
        corp_code=resolved_corp_code,
        ticker=normalized_ticker,
        start_date=start,
        end_date=end,
        page_count=page_count,
        demo=demo,
    )
    frame = OpenDartCollector(api_key=settings.dart_api_key).collect_disclosures(request)
    output_path = dart_disclosure_file_path(
        settings.project_root,
        resolved_corp_code,
        request.start_compact,
        request.end_compact,
    )
    write_parquet(frame, output_path)

    console.print("[bold green]Collected DART disclosures.[/bold green]")
    console.print(f"Ticker: {normalized_ticker}")
    console.print(f"Corp code: {resolved_corp_code}")
    console.print(f"Rows: {len(frame)}")
    console.print(f"Source: {frame.iloc[0]['source']}")
    console.print(f"Output: {output_path}")


@app.command("build-dart-disclosure-events")
def build_dart_disclosure_events(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    corp_code: Annotated[
        str | None,
        typer.Option("--corp-code", help="OpenDART corporation code. Example: 00126380"),
    ] = None,
    start: Annotated[
        str,
        typer.Option("--start", help="Disclosure start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="Disclosure end date in YYYY-MM-DD format."),
    ] = "2024-01-31",
) -> None:
    """Build event features from raw OpenDART disclosure search data."""
    configure_logger(settings.log_level)
    normalized_ticker = ticker.zfill(6)
    resolved_corp_code = resolve_corp_code(normalized_ticker, corp_code)
    start_compact = start.replace("-", "")
    end_compact = end.replace("-", "")
    input_path = dart_disclosure_file_path(
        settings.project_root,
        resolved_corp_code,
        start_compact,
        end_compact,
    )
    if not input_path.exists():
        raise typer.BadParameter(f"DART disclosure file does not exist: {input_path}")

    disclosure_frame = read_parquet(input_path)
    event_frame = DartDisclosureEventBuilder().build(disclosure_frame)
    output_path = dart_disclosure_event_file_path(
        settings.project_root,
        resolved_corp_code,
        start_compact,
        end_compact,
    )
    write_parquet(event_frame, output_path)

    risk_count = int(event_frame["event_risk_flag"].sum())
    latest = event_frame.sort_values("date").iloc[-1]
    console.print("[bold green]Built DART disclosure event features.[/bold green]")
    console.print(f"Ticker: {latest['ticker']}")
    console.print(f"Corp code: {latest['corp_code']}")
    console.print(f"Events: {len(event_frame)}")
    console.print(f"Risk events: {risk_count}")
    console.print(f"Latest event score: {float(latest['event_score']):.2f}")
    console.print(f"Latest event reason: {latest['event_reason']}")
    console.print(f"Output: {output_path}")


@app.command("build-dart-financial-features")
def build_dart_financial_features(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    corp_code: Annotated[
        str | None,
        typer.Option("--corp-code", help="OpenDART corporation code. Example: 00126380"),
    ] = None,
    year: Annotated[
        str,
        typer.Option("--year", help="Business year. Example: 2023"),
    ] = "2023",
    report_code: Annotated[
        str,
        typer.Option("--report-code", help="OpenDART report code. 11011 is annual report."),
    ] = "11011",
) -> None:
    """Build financial features from raw OpenDART financial statement data."""
    configure_logger(settings.log_level)
    normalized_ticker = ticker.zfill(6)
    resolved_corp_code = resolve_corp_code(normalized_ticker, corp_code)
    input_path = dart_financial_file_path(
        settings.project_root,
        resolved_corp_code,
        year,
        report_code,
    )
    if not input_path.exists():
        raise typer.BadParameter(f"DART financial file does not exist: {input_path}")

    financial_frame = read_parquet(input_path)
    feature_frame = DartFinancialFeatureBuilder().build(financial_frame)
    output_path = dart_financial_feature_file_path(
        settings.project_root,
        resolved_corp_code,
        year,
        report_code,
    )
    write_parquet(feature_frame, output_path)

    latest = feature_frame.iloc[0]
    console.print("[bold green]Built DART financial features.[/bold green]")
    console.print(f"Ticker: {latest['ticker']}")
    console.print(f"Corp code: {latest['corp_code']}")
    console.print(f"Financial score: {float(latest['financial_score']):.2f}")
    console.print(f"Reason: {latest['financial_reason']}")
    console.print(f"Output: {output_path}")


@app.command("process-price")
def process_price(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
) -> None:
    """Transform a raw daily price file into a processed parquet file."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    input_path = raw_price_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not input_path.exists():
        raise typer.BadParameter(f"Raw price file does not exist: {input_path}")

    raw_frame = read_parquet(input_path)
    processed_frame = PriceProcessor().process(raw_frame)
    output_path = processed_price_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(processed_frame, output_path)

    console.print(f"[green]Processed {len(processed_frame)} rows[/green]")
    console.print(f"Input: {input_path}")
    console.print(f"Output: {output_path}")


@app.command("build-features")
def build_features(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
) -> None:
    """Build reusable price features from a processed daily price file."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    input_path = processed_price_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not input_path.exists():
        raise typer.BadParameter(f"Processed price file does not exist: {input_path}")

    processed_frame = read_parquet(input_path)
    feature_frame = PriceFeatureBuilder().build(processed_frame)
    output_path = price_feature_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(feature_frame, output_path)

    console.print(f"[green]Built {len(feature_frame)} feature rows[/green]")
    console.print(f"Input: {input_path}")
    console.print(f"Output: {output_path}")


@app.command("build-ml-dataset")
def build_ml_dataset(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    holding_days: Annotated[
        int,
        typer.Option("--holding-days", help="Forward return horizon in trading rows."),
    ] = 5,
    minimum_forward_return: Annotated[
        float,
        typer.Option(
            "--minimum-forward-return",
            help="Minimum forward return required for a positive label.",
        ),
    ] = 0.0,
    minimum_excess_return: Annotated[
        float,
        typer.Option(
            "--minimum-excess-return",
            help="Minimum excess return required for an excess-return label.",
        ),
    ] = 0.0,
    benchmark_price_path: Annotated[
        Path | None,
        typer.Option(
            "--benchmark-price-path",
            help="Optional processed benchmark price parquet path for excess-return labels.",
        ),
    ] = None,
    dropna_features: Annotated[
        bool,
        typer.Option(
            "--dropna-features/--keepna-features",
            help="Drop rows with missing technical features before saving.",
        ),
    ] = False,
) -> None:
    """Build a leakage-aware ML training dataset from feature and price files."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    feature_path = price_feature_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    price_path = processed_price_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not feature_path.exists():
        raise typer.BadParameter(f"Price feature file does not exist: {feature_path}")
    if not price_path.exists():
        raise typer.BadParameter(f"Processed price file does not exist: {price_path}")
    benchmark_price_frame = None
    if benchmark_price_path is not None:
        if not benchmark_price_path.exists():
            raise typer.BadParameter(
                f"Benchmark processed price file does not exist: {benchmark_price_path}"
            )
        benchmark_price_frame = read_parquet(benchmark_price_path)

    training_frame = MLTrainingDatasetBuilder(
        MLTrainingDatasetConfig(
            holding_days=holding_days,
            minimum_forward_return=minimum_forward_return,
            minimum_excess_return=minimum_excess_return,
            dropna_features=dropna_features,
        )
    ).build(
        feature_frame=read_parquet(feature_path),
        processed_price_frame=read_parquet(price_path),
        benchmark_price_frame=benchmark_price_frame,
    )
    output_path = ml_training_dataset_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
        holding_days,
    )
    write_parquet(training_frame, output_path)

    positive_rate = float(training_frame["target_positive_forward_return"].mean())
    excess_rate = float(training_frame["target_excess_forward_return"].mean())
    console.print("[bold green]Built ML training dataset.[/bold green]")
    console.print(f"Ticker: {request.ticker}")
    console.print(f"Rows: {len(training_frame)}")
    console.print(f"Holding days: {holding_days}")
    console.print(f"Positive label rate: {positive_rate * 100:.2f}%")
    console.print(f"Excess-return label rate: {excess_rate * 100:.2f}%")
    console.print(f"Feature input: {feature_path}")
    console.print(f"Price input: {price_path}")
    console.print(
        "Benchmark input: "
        f"{benchmark_price_path if benchmark_price_path is not None else 'none, using 0% fallback'}"
    )
    console.print(f"Output: {output_path}")


@app.command("train-ml-baseline")
def train_ml_baseline(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    holding_days: Annotated[
        int,
        typer.Option("--holding-days", help="Training label horizon used by build-ml-dataset."),
    ] = 5,
    train_fraction: Annotated[
        float,
        typer.Option("--train-fraction", help="Time-ordered fraction used for training."),
    ] = 0.7,
    probability_threshold: Annotated[
        float,
        typer.Option("--probability-threshold", help="Probability cutoff for positive labels."),
    ] = 0.55,
    min_train_rows: Annotated[
        int,
        typer.Option("--min-train-rows", help="Minimum rows required in the training window."),
    ] = 20,
) -> None:
    """Train and evaluate the first explainable ML probability baseline."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)
    dataset_path = ml_training_dataset_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
        holding_days,
    )
    if not dataset_path.exists():
        raise typer.BadParameter(
            f"ML training dataset does not exist. Run build-ml-dataset first: {dataset_path}"
        )

    config = MLProbabilityBaselineConfig(
        train_fraction=train_fraction,
        probability_threshold=probability_threshold,
        min_train_rows=min_train_rows,
    )
    result = MLProbabilityBaselineTrainer(config).train_evaluate(read_parquet(dataset_path))

    predictions_path = ml_prediction_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
        holding_days,
    )
    metrics_path = ml_metrics_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
        holding_days,
    )
    artifact_path = ml_model_artifact_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
        holding_days,
    )
    report_path = ml_model_report_file_path(
        settings.project_root,
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
    experiment_log_path = ExperimentTracker(settings.project_root).log(
        build_ml_baseline_experiment_record(
            metrics=result.metrics,
            config=config,
            ticker=request.ticker,
            start_date=start,
            end_date=end,
            artifact_path=report_path,
        )
    )

    test_metric = result.metrics[result.metrics["split"] == "test"].iloc[0]
    console.print("[bold green]ML probability baseline trained.[/bold green]")
    console.print(f"Ticker: {request.ticker}")
    console.print(f"Test rows: {int(test_metric['row_count'])}")
    console.print(f"Test ROC-AUC: {float(test_metric['roc_auc']):.3f}")
    console.print(f"Test F1-score: {float(test_metric['f1_score']):.3f}")
    console.print(f"Predictions: {predictions_path}")
    console.print(f"Metrics: {metrics_path}")
    console.print(f"Model artifact: {artifact_path}")
    console.print(f"Report: {report_path}")
    console.print(f"Experiment log: {experiment_log_path}")


@app.command("build-investor-flow-features")
def build_investor_flow_features(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
) -> None:
    """Build investor flow features from raw investor net-buy data."""
    configure_logger(settings.log_level)
    request = InvestorFlowRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    input_path = raw_investor_flow_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not input_path.exists():
        raise typer.BadParameter(f"Raw investor flow file does not exist: {input_path}")

    flow_frame = read_parquet(input_path)
    feature_frame = InvestorFlowFeatureBuilder().build(flow_frame)
    output_path = investor_flow_feature_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(feature_frame, output_path)

    latest = feature_frame.sort_values("date").iloc[-1]
    console.print("[bold green]Built investor flow features.[/bold green]")
    console.print(f"Ticker: {latest['ticker']}")
    console.print(f"Rows: {len(feature_frame)}")
    console.print(f"Latest flow score: {float(latest['flow_score']):.2f}")
    console.print(f"Latest flow reason: {latest['flow_reason']}")
    console.print(f"Output: {output_path}")


@app.command("build-news-sentiment")
def build_news_sentiment(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="News window start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="News window end date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    use_gemini: Annotated[
        bool,
        typer.Option(
            "--gemini/--rule-based",
            help="Use Gemini for summarization, or deterministic rule-based scoring.",
        ),
    ] = False,
    allow_rule_fallback: Annotated[
        bool,
        typer.Option(
            "--allow-rule-fallback/--no-rule-fallback",
            help="Fall back to rule-based scoring if Gemini fails.",
        ),
    ] = True,
    gemini_model: Annotated[
        str,
        typer.Option("--gemini-model", help="Gemini model for news sentiment analysis."),
    ] = "auto",
) -> None:
    """Build reusable news sentiment features from raw news data."""
    configure_logger(settings.log_level)
    request = NewsSearchRequest.from_strings(ticker=ticker, start_date=start, end_date=end)
    input_path = raw_news_file_path(
        settings.project_root,
        request.ticker,
        request.compact_start_date,
        request.compact_end_date,
    )
    if not input_path.exists():
        raise typer.BadParameter(f"Raw news file does not exist: {input_path}")

    news_frame = read_parquet(input_path)
    try:
        feature_frame = NewsSentimentFeatureBuilder(
            api_key=settings.gemini_api_key,
            config=NewsSentimentConfig(
                use_gemini=use_gemini,
                allow_rule_fallback=allow_rule_fallback,
                gemini_model=gemini_model,
            ),
        ).build(news_frame)
    except (RuntimeError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    output_path = news_sentiment_feature_file_path(
        settings.project_root,
        request.ticker,
        request.compact_start_date,
        request.compact_end_date,
    )
    write_parquet(feature_frame, output_path)

    latest = feature_frame.sort_values("date").iloc[-1]
    console.print("[bold green]Built news sentiment features.[/bold green]")
    console.print(f"Ticker: {latest['ticker']}")
    console.print(f"Rows: {len(feature_frame)}")
    console.print(f"Latest news score: {float(latest['news_score']):.2f}")
    console.print(f"Latest news reason: {latest['news_reason']}")
    console.print(f"Latest summary: {latest['summary']}")
    console.print(f"Output: {output_path}")


@app.command("build-macro-features")
def build_macro_features(
    start: Annotated[
        str,
        typer.Option("--start", help="Macro window start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="Macro window end date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    series: Annotated[
        str,
        typer.Option("--series", help="Comma-separated FRED series ids."),
    ] = "DGS10,DFF,DEXKOUS",
) -> None:
    """Build reusable macro environment features from raw FRED observations."""
    configure_logger(settings.log_level)
    request = MacroRequest.from_strings(start_date=start, end_date=end, series_ids=series)
    input_path = raw_macro_file_path(
        settings.project_root,
        request.compact_start_date,
        request.compact_end_date,
        request.series_slug,
    )
    if not input_path.exists():
        raise typer.BadParameter(f"Raw macro file does not exist: {input_path}")

    macro_frame = read_parquet(input_path)
    feature_frame = MacroFeatureBuilder().build(macro_frame)
    output_path = macro_feature_file_path(
        settings.project_root,
        request.compact_start_date,
        request.compact_end_date,
        request.series_slug,
    )
    write_parquet(feature_frame, output_path)

    latest = feature_frame.sort_values("date").iloc[-1]
    console.print("[bold green]Built macro features.[/bold green]")
    console.print(f"Rows: {len(feature_frame)}")
    console.print(f"Latest macro score: {float(latest['macro_score']):.2f}")
    console.print(f"Latest macro reason: {latest['macro_reason']}")
    console.print(f"Output: {output_path}")


@app.command("score-stock")
def score_stock(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    financial_year: Annotated[
        str | None,
        typer.Option("--financial-year", help="OpenDART business year to blend into scoring."),
    ] = None,
    financial_report_code: Annotated[
        str,
        typer.Option("--financial-report-code", help="OpenDART report code. 11011 is annual."),
    ] = "11011",
    financial_corp_code: Annotated[
        str | None,
        typer.Option("--financial-corp-code", help="OpenDART corporation code when needed."),
    ] = None,
    event_start: Annotated[
        str | None,
        typer.Option("--event-start", help="Disclosure event feature start date."),
    ] = None,
    event_end: Annotated[
        str | None,
        typer.Option("--event-end", help="Disclosure event feature end date."),
    ] = None,
    event_corp_code: Annotated[
        str | None,
        typer.Option("--event-corp-code", help="OpenDART corporation code for event features."),
    ] = None,
    flow_start: Annotated[
        str | None,
        typer.Option("--flow-start", help="Investor flow feature start date."),
    ] = None,
    flow_end: Annotated[
        str | None,
        typer.Option("--flow-end", help="Investor flow feature end date."),
    ] = None,
    news_start: Annotated[
        str | None,
        typer.Option("--news-start", help="News sentiment feature start date."),
    ] = None,
    news_end: Annotated[
        str | None,
        typer.Option("--news-end", help="News sentiment feature end date."),
    ] = None,
    macro_start: Annotated[
        str | None,
        typer.Option("--macro-start", help="Macro feature start date."),
    ] = None,
    macro_end: Annotated[
        str | None,
        typer.Option("--macro-end", help="Macro feature end date."),
    ] = None,
    macro_series: Annotated[
        str,
        typer.Option("--macro-series", help="Comma-separated FRED series ids."),
    ] = "DGS10,DFF,DEXKOUS",
) -> None:
    """Score a stock from feature data and save daily explainable scores."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    input_path = price_feature_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not input_path.exists():
        raise typer.BadParameter(f"Price feature file does not exist: {input_path}")

    feature_frame = read_parquet(input_path)
    financial_frame = _load_financial_feature_frame(
        request.ticker,
        financial_corp_code,
        financial_year,
        financial_report_code,
    )
    event_frame = _load_disclosure_event_frame(
        request.ticker,
        event_corp_code,
        event_start,
        event_end or end,
    )
    flow_frame = _load_investor_flow_feature_frame(
        request.ticker,
        flow_start,
        flow_end or end,
    )
    news_frame = _load_news_sentiment_feature_frame(
        request.ticker,
        news_start,
        news_end or end,
    )
    macro_frame = _load_macro_feature_frame(
        macro_start,
        macro_end or end,
        macro_series,
    )
    score_frame = PriceScorer().score(
        feature_frame,
        financial_frame,
        event_frame,
        flow_frame,
        news_frame,
        macro_frame,
    )
    output_path = daily_score_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(score_frame, output_path)

    latest = score_frame.sort_values("date").iloc[-1]
    console.print(f"[green]Scored {len(score_frame)} rows[/green]")
    console.print(f"Latest signal: {latest['signal_label']}")
    console.print(f"Latest total score: {latest['total_score']:.2f}")
    console.print(f"Latest financial score: {latest['financial_score']:.2f}")
    console.print(f"Latest event score: {latest['event_score']:.2f}")
    console.print(f"Latest flow score: {latest['flow_score']:.2f}")
    console.print(f"Latest news score: {latest['news_score']:.2f}")
    console.print(f"Latest macro score: {latest['macro_score']:.2f}")
    console.print(f"Reason: {latest['score_reason']}")
    console.print(f"Financial reason: {latest['financial_reason']}")
    console.print(f"Event reason: {latest['event_reason']}")
    console.print(f"Flow reason: {latest['flow_reason']}")
    console.print(f"News reason: {latest['news_reason']}")
    console.print(f"Macro reason: {latest['macro_reason']}")
    console.print(f"Output: {output_path}")


@app.command("analyze-regime")
def analyze_regime(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-03-31",
) -> None:
    """Analyze market regime from price features and save a regime report."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    feature_path = price_feature_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not feature_path.exists():
        raise typer.BadParameter(f"Price feature file does not exist: {feature_path}")

    feature_frame = read_parquet(feature_path)
    regime_frame = MarketRegimeAnalyzer().analyze(feature_frame)
    output_path = market_regime_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    report_path = market_regime_report_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(regime_frame, output_path)
    write_text(MarketRegimeReportGenerator().generate(regime_frame), report_path)

    latest = regime_frame.sort_values("date").iloc[-1]
    console.print("[bold green]Market regime analyzed.[/bold green]")
    console.print(f"Latest regime: {latest['regime']}")
    console.print(f"Regime score: {float(latest['regime_score']):.2f}")
    console.print(f"Risk level: {latest['risk_level']}")
    console.print(f"Reason: {latest['regime_reason']}")
    console.print(f"Output: {output_path}")
    console.print(f"Report: {report_path}")


@app.command("generate-report")
def generate_report(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
) -> None:
    """Generate a Markdown daily report from score and feature files."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    score_path = daily_score_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    feature_path = price_feature_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not score_path.exists():
        raise typer.BadParameter(f"Daily score file does not exist: {score_path}")
    if not feature_path.exists():
        raise typer.BadParameter(f"Price feature file does not exist: {feature_path}")

    score_frame = read_parquet(score_path)
    feature_frame = read_parquet(feature_path)
    report = DailyReportGenerator().generate(score_frame, feature_frame)

    latest_date = score_frame.sort_values("date").iloc[-1]["date"]
    output_path = daily_report_file_path(
        settings.project_root,
        request.ticker,
        pd.Timestamp(latest_date).strftime("%Y%m%d"),
    )
    write_text(report, output_path)

    console.print("[green]Generated daily report[/green]")
    console.print(f"Output: {output_path}")


@app.command("generate-signal")
def generate_signal(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
) -> None:
    """Generate final signals with risk filters from score and feature files."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    score_path = daily_score_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    feature_path = price_feature_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not score_path.exists():
        raise typer.BadParameter(f"Daily score file does not exist: {score_path}")
    if not feature_path.exists():
        raise typer.BadParameter(f"Price feature file does not exist: {feature_path}")

    score_frame = read_parquet(score_path)
    feature_frame = read_parquet(feature_path)
    regime_path = market_regime_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    regime_frame = read_parquet(regime_path) if regime_path.exists() else None
    signal_frame = SignalEngine().generate(score_frame, feature_frame, regime_frame)
    output_path = final_signal_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(signal_frame, output_path)

    latest = signal_frame.sort_values("date").iloc[-1]
    console.print(f"[green]Generated {len(signal_frame)} final signal rows[/green]")
    console.print(f"Latest action: {latest['final_action']}")
    console.print(f"Confidence: {latest['confidence_score']:.2f}")
    console.print(f"Risk blocked: {latest['risk_blocked']}")
    console.print(f"Suggested position: {latest['suggested_position_pct']:.2f}%")
    console.print(f"Output: {output_path}")


@app.command("analyze-stock")
def analyze_stock(
    query: Annotated[
        str | None,
        typer.Argument(help="분석할 회사명, 영문명, 또는 종목코드입니다. 예: 삼성전자"),
    ] = None,
    name: Annotated[
        str | None,
        typer.Option("--name", "-n", help="분석할 회사명입니다. 예: 삼성전자"),
    ] = None,
    ticker: Annotated[
        str | None,
        typer.Option("--ticker", "-t", help="6자리 한국 주식 종목코드입니다. 예: 005930"),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="시작일입니다. YYYY-MM-DD 형식이며 없으면 lookback 기준입니다.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="종료일입니다. YYYY-MM-DD 형식이며 없으면 오늘입니다."),
    ] = None,
    lookback_days: Annotated[
        int,
        typer.Option("--lookback-days", help="--start가 없을 때 과거 며칠을 볼지 정합니다."),
    ] = 180,
    live_names: Annotated[
        bool,
        typer.Option(
            "--live-names/--offline-names",
            help="내장 별칭에 없는 종목명도 무료 상장 목록으로 찾아봅니다.",
        ),
    ] = True,
    telegram_dry_run: Annotated[
        bool,
        typer.Option(
            "--telegram-dry-run",
            help="텔레그램으로 보낼 전체 분석 결과를 미리보기만 합니다.",
        ),
    ] = False,
    telegram_send: Annotated[
        bool,
        typer.Option(
            "--telegram-send",
            help="전체 분석 결과를 텔레그램으로 실제 전송합니다.",
        ),
    ] = False,
) -> None:
    """회사명이나 종목코드로 단일 종목 분석을 실행합니다."""
    configure_logger(settings.log_level)
    if telegram_dry_run and telegram_send:
        raise typer.BadParameter("--telegram-dry-run and --telegram-send cannot be used together.")

    selected_query = _select_single_stock_query(query, name, ticker)
    resolver = StockResolver(use_live_sources=live_names)

    try:
        resolution = resolver.resolve(selected_query)
    except AmbiguousStockQuery as exc:
        console.print("[yellow]입력한 이름과 맞는 종목이 여러 개입니다.[/yellow]")
        console.print("회사명을 더 정확히 입력하거나 --ticker로 종목코드를 직접 넣어주세요.")
        _print_stock_candidate_table(exc.candidates)
        raise typer.Exit(code=1) from exc
    except ValueError as exc:
        suggestions = resolver.candidates(selected_query)
        console.print(f"[red]종목을 찾지 못했습니다: {selected_query}[/red]")
        if suggestions:
            console.print("비슷한 후보:")
            _print_stock_candidate_table(suggestions)
        else:
            console.print("6자리 종목코드로 다시 시도해보세요. 예: --ticker 005930")
        raise typer.Exit(code=1) from exc

    start_date, end_date = _resolve_cli_date_range(start, end, lookback_days)
    request = PriceRequest.from_strings(
        ticker=resolution.ticker,
        start_date=start_date,
        end_date=end_date,
    )
    result = DailyPipeline(settings.project_root).run(request)

    stock_label = f"{resolution.ticker} {resolution.name}".strip()
    analysis = build_single_stock_analysis(
        stock_label=stock_label,
        query=selected_query,
        matched_by=resolution.matched_by,
        source=resolution.source,
        start_date=start_date,
        end_date=end_date,
        score_frame=read_parquet(result.score_path),
        feature_frame=read_parquet(result.feature_path),
        signal_frame=read_parquet(result.signal_path),
        data_quality_warning_count=result.data_quality_warning_count,
        data_quality_fail_count=result.data_quality_fail_count,
    )

    console.print("[bold green]단일 종목 분석이 완료되었습니다.[/bold green]")
    _print_analysis_items("1. 한눈에 보는 결론", analysis.summary)
    _print_analysis_items("2. 점수 분해", analysis.score_breakdown)
    _print_analysis_items("3. 가격/기술 지표", analysis.price_metrics)
    _print_analysis_items("4. 판단 근거", analysis.evidence)
    _print_beginner_notes(analysis.beginner_notes)
    console.print(f"시그널 파일: {result.signal_path}")
    console.print(f"리포트 파일: {result.report_path}")
    console.print("모드: 분석 전용입니다. 실제 주문은 보내지 않았습니다.")

    if telegram_dry_run or telegram_send:
        telegram_message = format_single_stock_analysis_message(
            analysis,
            signal_path=result.signal_path,
            report_path=result.report_path,
        )
        _send_telegram_message_parts(
            split_telegram_message(telegram_message),
            dry_run=telegram_dry_run,
            success_message="텔레그램 단일 종목 분석 결과를 전송했습니다.",
            dry_run_message="Telegram dry run. 단일 종목 분석 결과는 전송되지 않았습니다.",
        )


@app.command("run-pipeline")
def run_pipeline(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    financial_year: Annotated[
        str | None,
        typer.Option("--financial-year", help="OpenDART business year to blend into scoring."),
    ] = None,
    financial_report_code: Annotated[
        str,
        typer.Option("--financial-report-code", help="OpenDART report code. 11011 is annual."),
    ] = "11011",
    financial_corp_code: Annotated[
        str | None,
        typer.Option("--financial-corp-code", help="OpenDART corporation code when needed."),
    ] = None,
    event_start: Annotated[
        str | None,
        typer.Option("--event-start", help="Disclosure event feature start date."),
    ] = None,
    event_end: Annotated[
        str | None,
        typer.Option("--event-end", help="Disclosure event feature end date."),
    ] = None,
    event_corp_code: Annotated[
        str | None,
        typer.Option("--event-corp-code", help="OpenDART corporation code for event features."),
    ] = None,
    flow_start: Annotated[
        str | None,
        typer.Option("--flow-start", help="Investor flow feature start date."),
    ] = None,
    flow_end: Annotated[
        str | None,
        typer.Option("--flow-end", help="Investor flow feature end date."),
    ] = None,
    news_start: Annotated[
        str | None,
        typer.Option("--news-start", help="News sentiment feature start date."),
    ] = None,
    news_end: Annotated[
        str | None,
        typer.Option("--news-end", help="News sentiment feature end date."),
    ] = None,
    macro_start: Annotated[
        str | None,
        typer.Option("--macro-start", help="Macro feature start date."),
    ] = None,
    macro_end: Annotated[
        str | None,
        typer.Option("--macro-end", help="Macro feature end date."),
    ] = None,
    macro_series: Annotated[
        str,
        typer.Option("--macro-series", help="Comma-separated FRED series ids."),
    ] = "DGS10,DFF,DEXKOUS",
) -> None:
    """Run collect, process, feature, score, signal, and report steps at once."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)
    financial_frame = _load_financial_feature_frame(
        request.ticker,
        financial_corp_code,
        financial_year,
        financial_report_code,
    )
    event_frame = _load_disclosure_event_frame(
        request.ticker,
        event_corp_code,
        event_start,
        event_end or end,
    )
    flow_frame = _load_investor_flow_feature_frame(
        request.ticker,
        flow_start,
        flow_end or end,
    )
    news_frame = _load_news_sentiment_feature_frame(
        request.ticker,
        news_start,
        news_end or end,
    )
    macro_frame = _load_macro_feature_frame(
        macro_start,
        macro_end or end,
        macro_series,
    )
    result = DailyPipeline(settings.project_root).run(
        request,
        financial_frame,
        event_frame,
        flow_frame,
        news_frame,
        macro_frame,
    )

    console.print("[bold green]Daily pipeline completed.[/bold green]")
    console.print(f"Raw: {result.raw_path}")
    console.print(f"Processed: {result.processed_path}")
    console.print(f"Data quality: {result.data_quality_path}")
    console.print(f"Data quality report: {result.data_quality_report_path}")
    console.print(f"Features: {result.feature_path}")
    console.print(f"Regime: {result.regime_path}")
    console.print(f"Scores: {result.score_path}")
    console.print(f"Signals: {result.signal_path}")
    console.print(f"Report: {result.report_path}")
    console.print(f"Latest action: {result.latest_action}")
    console.print(f"Confidence: {result.latest_confidence_score:.2f}")
    console.print(f"Financial score: {result.latest_financial_score:.2f}")
    console.print(f"Event score: {result.latest_event_score:.2f}")
    console.print(f"Flow score: {result.latest_flow_score:.2f}")
    console.print(f"News score: {result.latest_news_score:.2f}")
    console.print(f"Macro score: {result.latest_macro_score:.2f}")
    console.print(f"Market regime: {result.latest_market_regime}")
    console.print(
        "Data quality: "
        f"{result.data_quality_warning_count} warning(s), {result.data_quality_fail_count} fail(s)"
    )


@app.command("list-universe")
def list_universe(
    universe: Annotated[
        str,
        typer.Option("--universe", "-u", help="Universe name. Use 'all' to list names."),
    ] = "demo",
    save: Annotated[
        bool,
        typer.Option("--save/--no-save", help="Save the selected universe as parquet and CSV."),
    ] = True,
) -> None:
    """List a named stock universe and optionally persist it to data/processed."""
    configure_logger(settings.log_level)
    registry = UniverseRegistry()

    if universe.strip().lower() == "all":
        console.print(registry.list_definitions())
        return

    try:
        definition = registry.get(universe)
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc

    frame = definition.to_frame()
    console.print(f"[bold green]Universe: {definition.name}[/bold green]")
    console.print(definition.description)
    console.print(frame[["ticker", "name", "market", "sector", "reason", "is_active"]])

    if save:
        parquet_path = universe_file_path(settings.project_root, definition.name)
        csv_path = universe_csv_path(settings.project_root, definition.name)
        write_parquet(frame, parquet_path)
        write_csv(frame, csv_path)
        console.print(f"Parquet: {parquet_path}")
        console.print(f"CSV: {csv_path}")


@app.command("run-universe")
def run_universe(
    tickers: Annotated[
        str | None,
        typer.Option(
            "--tickers",
            "-t",
            help="Comma-separated tickers. Overrides --universe when provided.",
        ),
    ] = None,
    universe: Annotated[
        str,
        typer.Option("--universe", "-u", help="Named universe to run when --tickers is omitted."),
    ] = "demo",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
) -> None:
    """Run the daily pipeline for multiple tickers and save a summary table."""
    configure_logger(settings.log_level)
    if tickers:
        ticker_list = _parse_tickers(tickers)
        universe_source = "manual tickers"
    else:
        try:
            definition = UniverseRegistry().get(universe)
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc
        ticker_list = definition.tickers()
        universe_source = f"universe:{definition.name}"

    result = UniversePipeline(settings.project_root).run(
        tickers=ticker_list,
        start_date=start,
        end_date=end,
    )
    summary_frame = read_parquet(
        universe_summary_file_path(
            settings.project_root,
            start.replace("-", ""),
            end.replace("-", ""),
        )
    )

    console.print("[bold green]Universe pipeline completed.[/bold green]")
    console.print(f"Source: {universe_source}")
    console.print(f"Total: {result.total_count}")
    console.print(f"Success: {result.success_count}")
    console.print(f"Failed: {result.failed_count}")
    console.print(f"Summary: {result.summary_path}")
    console.print(f"CSV: {result.summary_csv_path}")
    console.print(
        summary_frame[
            [
                "ticker",
                "status",
                "latest_action",
                "latest_confidence_score",
                "latest_market_regime",
            ]
        ]
    )


def _parse_tickers(tickers: str) -> list[str]:
    return [ticker.strip().zfill(6) for ticker in tickers.split(",") if ticker.strip()]


@app.command("screen-universe")
def screen_universe(
    summary_path: Annotated[
        Path | None,
        typer.Option(
            "--summary-path",
            help="Universe summary parquet path. Uses latest if omitted.",
        ),
    ] = None,
    output_name: Annotated[
        str | None,
        typer.Option("--output-name", help="Output report name without extension."),
    ] = None,
    min_confidence: Annotated[
        float,
        typer.Option("--min-confidence", help="Minimum final signal confidence."),
    ] = 60.0,
    min_screen_score: Annotated[
        float,
        typer.Option("--min-screen-score", help="Minimum composite screening score."),
    ] = 60.0,
    top_n: Annotated[
        int,
        typer.Option("--top-n", help="Number of rows to print."),
    ] = 20,
    passed_only: Annotated[
        bool,
        typer.Option(
            "--passed-only/--all",
            help="Print only passed candidates in the terminal table.",
        ),
    ] = False,
    priority: Annotated[
        str | None,
        typer.Option(
            "--priority",
            help="Comma-separated review priorities to print, e.g. high,medium.",
        ),
    ] = None,
    status_reason: Annotated[
        str | None,
        typer.Option(
            "--status-reason",
            help="Comma-separated screen status reasons to print.",
        ),
    ] = None,
    compact: Annotated[
        bool,
        typer.Option(
            "--compact/--full-table",
            help="Print a compact terminal table.",
        ),
    ] = False,
) -> None:
    """Create a human-review shortlist from the latest universe signal artifacts."""
    configure_logger(settings.log_level)
    resolved_summary_path = summary_path or find_latest_universe_summary(settings.project_root)
    if resolved_summary_path is None or not resolved_summary_path.exists():
        raise typer.BadParameter(
            "Universe summary file does not exist. Run run-universe first or pass --summary-path."
        )

    summary_frame = read_parquet(resolved_summary_path)
    result_frame = AutoScreener(
        project_root=settings.project_root,
        config=AutoScreenerConfig(
            min_confidence=min_confidence,
            min_screen_score=min_screen_score,
        ),
    ).screen(summary_frame)

    report_name = _safe_report_name(output_name or f"screening_{resolved_summary_path.stem}")
    result_path = screening_result_file_path(settings.project_root, report_name)
    csv_path = screening_result_csv_path(settings.project_root, report_name)
    report_path = screening_report_file_path(settings.project_root, report_name)
    write_parquet(result_frame, result_path)
    write_csv(result_frame, csv_path)
    write_text(format_screening_report(result_frame), report_path)

    passed_count = int(result_frame["passed"].sum()) if not result_frame.empty else 0
    console.print("[bold green]Universe screening completed.[/bold green]")
    console.print(f"Checked: {len(result_frame)}")
    console.print(f"Passed: {passed_count}")
    console.print(f"Result: {result_path}")
    console.print(f"CSV: {csv_path}")
    console.print(f"Report: {report_path}")
    if not result_frame.empty:
        console.print(
            f"Priority summary: {_format_screening_counts(result_frame, 'review_priority')}"
        )
        console.print(
            f"Status summary: {_format_screening_counts(result_frame, 'screen_status_reason')}"
        )
    display_columns = _screening_cli_display_columns(compact=compact)
    if not result_frame.empty:
        table_frame = result_frame[result_frame["passed"]] if passed_only else result_frame
        priority_filter = _parse_console_filter_values(priority)
        if priority_filter:
            table_frame = table_frame[
                table_frame["review_priority"].astype(str).isin(priority_filter)
            ]
        status_filter = _parse_console_filter_values(status_reason)
        if status_filter:
            table_frame = table_frame[
                table_frame["screen_status_reason"].astype(str).isin(status_filter)
            ]
        if table_frame.empty:
            console.print("[yellow]No rows matched the terminal display filter.[/yellow]")
            return
        display_frame = table_frame.head(top_n)[display_columns].copy()
        if "reasons" in display_frame.columns:
            display_frame["reasons"] = display_frame["reasons"].map(_short_console_text)
        console.print(display_frame.to_string(index=False))


@app.command("generate-universe-report")
def generate_universe_report(
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
) -> None:
    """Generate a Markdown report from a universe summary file."""
    configure_logger(settings.log_level)
    start_compact = start.replace("-", "")
    end_compact = end.replace("-", "")
    summary_path = universe_summary_file_path(settings.project_root, start_compact, end_compact)
    if not summary_path.exists():
        raise typer.BadParameter(f"Universe summary file does not exist: {summary_path}")

    summary_frame = read_parquet(summary_path)
    report = UniverseReportGenerator().generate(
        summary_frame,
        start_date=start,
        end_date=end,
    )
    output_path = universe_report_file_path(settings.project_root, start_compact, end_compact)
    write_text(report, output_path)

    console.print("[green]Generated universe report[/green]")
    console.print(f"Output: {output_path}")


@app.command("paper-trade")
def paper_trade(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    initial_cash: Annotated[
        float,
        typer.Option("--initial-cash", help="Starting virtual cash for paper trading."),
    ] = 10_000_000.0,
    max_position_pct: Annotated[
        float,
        typer.Option("--max-position-pct", help="Maximum virtual allocation per entry."),
    ] = 10.0,
    transaction_cost_bps: Annotated[
        float,
        typer.Option("--transaction-cost-bps", help="Virtual transaction cost in bps."),
    ] = 15.0,
    slippage_bps: Annotated[
        float,
        typer.Option("--slippage-bps", help="Virtual slippage in bps."),
    ] = 10.0,
) -> None:
    """Run paper-only trading simulation from final signals and processed prices."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    price_path = processed_price_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    signal_path = final_signal_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not price_path.exists():
        raise typer.BadParameter(f"Processed price file does not exist: {price_path}")
    if not signal_path.exists():
        raise typer.BadParameter(f"Final signal file does not exist: {signal_path}")

    price_frame = read_parquet(price_path)
    signal_frame = read_parquet(signal_path)
    config = PaperTradingConfig(
        initial_cash=initial_cash,
        max_position_pct=max_position_pct,
        transaction_cost_bps=transaction_cost_bps,
        slippage_bps=slippage_bps,
    )
    trades, positions, summary = PaperTradingSimulator(config).run(price_frame, signal_frame)

    ledger_path = paper_trade_ledger_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    positions_path = paper_position_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    summary_path = paper_summary_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    report_path = paper_trading_report_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(trades, ledger_path)
    write_parquet(positions, positions_path)
    write_parquet(summary, summary_path)
    write_text(PaperTradingReportGenerator().generate(trades, positions, summary), report_path)

    metric = summary.iloc[0]
    console.print("[bold green]Paper trading simulation completed.[/bold green]")
    console.print("[yellow]Paper mode only. No broker API or real order was called.[/yellow]")
    console.print(f"Filled trades: {int(metric['trade_count'])}")
    console.print(f"Ending equity: {float(metric['ending_equity']):,.0f}")
    console.print(f"Cumulative return: {float(metric['cumulative_return']) * 100:.2f}%")
    console.print(f"Open positions: {len(positions)}")
    console.print(f"Ledger: {ledger_path}")
    console.print(f"Positions: {positions_path}")
    console.print(f"Summary: {summary_path}")
    console.print(f"Report: {report_path}")


@app.command("paper-trade-universe")
def paper_trade_universe(
    tickers: Annotated[
        str | None,
        typer.Option(
            "--tickers",
            "-t",
            help="Comma-separated tickers. Overrides --universe when provided.",
        ),
    ] = None,
    universe: Annotated[
        str,
        typer.Option("--universe", "-u", help="Named universe to run when --tickers is omitted."),
    ] = "demo",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    initial_cash: Annotated[
        float,
        typer.Option("--initial-cash", help="Starting virtual portfolio cash."),
    ] = 10_000_000.0,
    max_position_pct: Annotated[
        float,
        typer.Option("--max-position-pct", help="Maximum virtual allocation per entry."),
    ] = 10.0,
    transaction_cost_bps: Annotated[
        float,
        typer.Option("--transaction-cost-bps", help="Virtual transaction cost in bps."),
    ] = 15.0,
    slippage_bps: Annotated[
        float,
        typer.Option("--slippage-bps", help="Virtual slippage in bps."),
    ] = 10.0,
    strict: Annotated[
        bool,
        typer.Option(
            "--strict/--skip-missing",
            help="Fail on missing ticker inputs or skip unavailable tickers.",
        ),
    ] = False,
) -> None:
    """Run paper-only portfolio simulation for a named universe or ticker list."""
    configure_logger(settings.log_level)
    if tickers:
        ticker_list = _parse_tickers(tickers)
        portfolio_name = "custom"
        portfolio_source = "manual tickers"
    else:
        try:
            definition = UniverseRegistry().get(universe)
        except KeyError as exc:
            raise typer.BadParameter(str(exc)) from exc
        ticker_list = definition.tickers()
        portfolio_name = definition.name
        portfolio_source = f"universe:{definition.name}"

    try:
        result = run_paper_portfolio(
            settings.project_root,
            PaperPortfolioConfig(
                name=portfolio_name,
                tickers=tuple(ticker_list),
                start_date=start,
                end_date=end,
                initial_cash=initial_cash,
                max_position_pct=max_position_pct,
                transaction_cost_bps=transaction_cost_bps,
                slippage_bps=slippage_bps,
                skip_missing=not strict,
            ),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    metric = result.summary.iloc[0]
    console.print("[bold green]Paper portfolio simulation completed.[/bold green]")
    console.print("[yellow]Paper mode only. No broker API or real order was called.[/yellow]")
    console.print(f"Source: {portfolio_source}")
    console.print(f"Requested tickers: {result.requested_ticker_count}")
    console.print(f"Loaded tickers: {result.loaded_ticker_count}")
    console.print(f"Skipped tickers: {len(result.skipped_tickers)}")
    console.print(f"Filled trades: {int(metric['trade_count'])}")
    console.print(f"Ending equity: {float(metric['ending_equity']):,.0f}")
    console.print(f"Cumulative return: {float(metric['cumulative_return']) * 100:.2f}%")
    console.print(f"Gross exposure: {float(metric['gross_exposure_pct']):.2f}%")
    console.print(f"Open positions: {len(result.positions)}")
    console.print(f"Ledger: {result.ledger_path}")
    console.print(f"Positions: {result.positions_path}")
    console.print(f"Summary: {result.summary_path}")
    console.print(f"Report: {result.report_path}")


@app.command("backtest-stock")
def backtest_stock(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-01-31",
    holding_days: Annotated[
        int,
        typer.Option("--holding-days", help="Number of trading days to hold each trade."),
    ] = 5,
) -> None:
    """Backtest buy-candidate final signals against processed prices."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    price_path = processed_price_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    signal_path = final_signal_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not price_path.exists():
        raise typer.BadParameter(f"Processed price file does not exist: {price_path}")
    if not signal_path.exists():
        raise typer.BadParameter(f"Final signal file does not exist: {signal_path}")

    price_frame = read_parquet(price_path)
    signal_frame = read_parquet(signal_path)
    config = BacktestConfig(holding_days=holding_days)
    trades, metrics = SimpleBacktester(config).run(price_frame, signal_frame)

    trades_path = backtest_trades_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    metrics_path = backtest_metrics_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    report_path = backtest_report_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(trades, trades_path)
    write_parquet(metrics, metrics_path)
    write_text(BacktestReportGenerator().generate(trades, metrics), report_path)
    experiment_log_path = ExperimentTracker(settings.project_root).log(
        build_backtest_experiment_record(
            metrics=metrics,
            config=config,
            start_date=start,
            end_date=end,
            artifact_path=report_path,
        )
    )

    metric = metrics.iloc[0]
    console.print("[bold green]Backtest completed.[/bold green]")
    console.print(f"Trades: {int(metric['trade_count'])}")
    console.print(f"Win rate: {float(metric['win_rate']) * 100:.2f}%")
    console.print(f"Cumulative return: {float(metric['cumulative_return']) * 100:.2f}%")
    console.print(f"Max drawdown: {float(metric['max_drawdown']) * 100:.2f}%")
    console.print(f"Sharpe ratio: {float(metric['sharpe_ratio']):.2f}")
    console.print(f"Report: {report_path}")
    console.print(f"Experiment log: {experiment_log_path}")


@app.command("walk-forward-backtest")
def walk_forward_backtest(
    ticker: Annotated[
        str,
        typer.Option("--ticker", "-t", help="Korean stock ticker. Example: 005930"),
    ] = "005930",
    start: Annotated[
        str,
        typer.Option("--start", help="Start date in YYYY-MM-DD format."),
    ] = "2024-01-01",
    end: Annotated[
        str,
        typer.Option("--end", help="End date in YYYY-MM-DD format."),
    ] = "2024-03-31",
    train_size: Annotated[
        int,
        typer.Option("--train-size", help="Number of signal dates in each train window."),
    ] = 20,
    test_size: Annotated[
        int,
        typer.Option("--test-size", help="Number of signal dates in each test window."),
    ] = 5,
    step_size: Annotated[
        int,
        typer.Option("--step-size", help="Number of signal dates to move between folds."),
    ] = 5,
    holding_days: Annotated[
        int,
        typer.Option("--holding-days", help="Number of trading days to hold each trade."),
    ] = 5,
) -> None:
    """Run rolling walk-forward validation for final signals."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)

    price_path = processed_price_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    signal_path = final_signal_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    if not price_path.exists():
        raise typer.BadParameter(f"Processed price file does not exist: {price_path}")
    if not signal_path.exists():
        raise typer.BadParameter(f"Final signal file does not exist: {signal_path}")

    price_frame = read_parquet(price_path)
    signal_frame = read_parquet(signal_path)
    config = WalkForwardConfig(
        train_size=train_size,
        test_size=test_size,
        step_size=step_size,
        holding_days=holding_days,
    )
    folds, summary = WalkForwardBacktester(config).run(price_frame, signal_frame)

    folds_path = walk_forward_folds_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    summary_path = walk_forward_summary_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    report_path = walk_forward_report_file_path(
        settings.project_root,
        request.ticker,
        request.pykrx_start_date,
        request.pykrx_end_date,
    )
    write_parquet(folds, folds_path)
    write_parquet(summary, summary_path)
    write_text(WalkForwardReportGenerator().generate(folds, summary), report_path)
    experiment_log_path = ExperimentTracker(settings.project_root).log(
        build_walk_forward_experiment_record(
            summary=summary,
            config=config,
            start_date=start,
            end_date=end,
            artifact_path=report_path,
        )
    )

    metric = summary.iloc[0]
    console.print("[bold green]Walk-forward backtest completed.[/bold green]")
    console.print(f"Folds: {int(metric['fold_count'])}")
    console.print(f"Trades: {int(metric['total_trade_count'])}")
    console.print(f"Compounded return: {float(metric['compounded_return']) * 100:.2f}%")
    console.print(f"Worst max drawdown: {float(metric['worst_max_drawdown']) * 100:.2f}%")
    console.print(f"Positive fold ratio: {float(metric['positive_fold_ratio']) * 100:.2f}%")
    console.print(f"Report: {report_path}")
    console.print(f"Experiment log: {experiment_log_path}")


@app.command("send-telegram-daily")
def send_telegram_daily(
    summary_path: Annotated[
        Path | None,
        typer.Option(
            "--summary-path", help="Universe summary parquet path. Uses latest if omitted."
        ),
    ] = None,
    include_backtest: Annotated[
        bool,
        typer.Option(
            "--include-backtest/--no-backtest",
            help="Include latest simple backtest metrics when available.",
        ),
    ] = True,
    include_walk_forward: Annotated[
        bool,
        typer.Option(
            "--include-walk-forward/--no-walk-forward",
            help="Include latest walk-forward validation summary when available.",
        ),
    ] = True,
    include_paper_portfolio: Annotated[
        bool,
        typer.Option(
            "--include-paper-portfolio/--no-paper-portfolio",
            help="Include latest paper portfolio summary when available.",
        ),
    ] = True,
    include_operations_health: Annotated[
        bool,
        typer.Option(
            "--include-operations-health/--no-operations-health",
            help="Include latest operations health result when available.",
        ),
    ] = True,
    top_n: Annotated[
        int,
        typer.Option("--top-n", help="Number of ranked candidates to include."),
    ] = 5,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run/--send", help="Preview message locally or send to Telegram."),
    ] = True,
) -> None:
    """Send or preview a compact Telegram daily operations brief."""
    configure_logger(settings.log_level)

    resolved_summary_path = summary_path or find_latest_universe_summary(settings.project_root)
    if resolved_summary_path is None or not resolved_summary_path.exists():
        raise typer.BadParameter(
            "Universe summary file does not exist. Run run-universe first or pass --summary-path."
        )

    universe_summary = read_parquet(resolved_summary_path)
    backtest_metrics = None
    if include_backtest:
        metrics_path = find_latest_backtest_metrics(settings.project_root)
        backtest_metrics = read_parquet(metrics_path) if metrics_path is not None else None

    walk_forward_summary = None
    if include_walk_forward:
        walk_forward_path = find_latest_walk_forward_summary(settings.project_root)
        walk_forward_summary = (
            read_parquet(walk_forward_path) if walk_forward_path is not None else None
        )
    drift_path = find_latest_drift_result(settings.project_root)
    drift_result = read_parquet(drift_path) if drift_path is not None else None
    paper_portfolio_summary = None
    if include_paper_portfolio:
        paper_path = find_latest_paper_portfolio_summary(settings.project_root)
        paper_portfolio_summary = read_parquet(paper_path) if paper_path is not None else None
    screening_result = None
    screening_path = find_latest_screening_result(settings.project_root)
    if screening_path is not None:
        screening_result = load_screening_result(screening_path)
    kis_paper_candidates = None
    kis_path = find_latest_kis_paper_candidates(settings.project_root)
    if kis_path is not None:
        kis_paper_candidates = load_kis_paper_candidates(kis_path)
    operations_health = None
    if include_operations_health:
        operations_health_path = find_latest_operations_health(settings.project_root)
        operations_health = (
            read_parquet(operations_health_path) if operations_health_path is not None else None
        )

    message = build_daily_telegram_message(
        universe_summary=universe_summary,
        screening_result=screening_result,
        paper_portfolio_summary=paper_portfolio_summary,
        kis_paper_candidates=kis_paper_candidates,
        backtest_metrics=backtest_metrics,
        walk_forward_summary=walk_forward_summary,
        drift_result=drift_result,
        operations_health=operations_health,
        top_n=top_n,
    )
    notifier = TelegramNotifier(
        bot_token=settings.telegram_bot_token,
        chat_id=settings.telegram_chat_id,
        timeout_seconds=settings.telegram_timeout_seconds,
        max_retries=settings.telegram_max_retries,
        retry_sleep_seconds=settings.telegram_retry_sleep_seconds,
    )
    try:
        result = notifier.send_message(message, dry_run=dry_run)
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    except RuntimeError as exc:
        console.print("[bold red]Telegram send failed.[/bold red]")
        console.print(str(exc))
        if _contains_certificate_error(exc):
            console.print(
                "[yellow]SSL certificate verification failed. "
                "Check antivirus HTTPS inspection, proxy certificates, or Windows/Python "
                "certificate trust settings. The message was not sent.[/yellow]"
            )
        raise typer.Exit(code=1) from exc

    if result.dry_run:
        console.print("[bold yellow]Telegram dry run. Message was not sent.[/bold yellow]")
        console.print(result.message)
        return

    console.print("[bold green]Telegram daily brief sent.[/bold green]")
    console.print(f"Status code: {result.status_code}")


@app.command("run-daily-job")
def run_daily_job(
    universe: Annotated[
        str,
        typer.Option("--universe", "-u", help="Named universe to run."),
    ] = "demo",
    start: Annotated[
        str | None,
        typer.Option("--start", help="Start date in YYYY-MM-DD format. Defaults by lookback."),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="End date in YYYY-MM-DD format. Defaults to today."),
    ] = None,
    lookback_days: Annotated[
        int,
        typer.Option("--lookback-days", help="Lookback days when --start is omitted."),
    ] = 60,
    notify: Annotated[
        bool,
        typer.Option("--notify/--no-notify", help="Build and send or preview Telegram brief."),
    ] = True,
    telegram_dry_run: Annotated[
        bool,
        typer.Option(
            "--telegram-dry-run/--telegram-send",
            help="Preview Telegram brief locally or send it.",
        ),
    ] = True,
    top_n: Annotated[
        int,
        typer.Option("--top-n", help="Number of ranked candidates in Telegram brief."),
    ] = 5,
    paper_trading: Annotated[
        bool,
        typer.Option(
            "--paper-trading/--no-paper-trading",
            help="Run paper portfolio simulation after universe pipeline.",
        ),
    ] = True,
    paper_initial_cash: Annotated[
        float,
        typer.Option("--paper-initial-cash", help="Starting virtual portfolio cash."),
    ] = 10_000_000.0,
    paper_max_position_pct: Annotated[
        float,
        typer.Option("--paper-max-position-pct", help="Maximum virtual allocation per entry."),
    ] = 10.0,
    screening: Annotated[
        bool,
        typer.Option(
            "--screening/--no-screening",
            help="Run the auto screener after the universe pipeline.",
        ),
    ] = True,
    screening_min_confidence: Annotated[
        float,
        typer.Option("--screen-min-confidence", help="Minimum confidence for screen pass."),
    ] = 60.0,
    screening_min_score: Annotated[
        float,
        typer.Option("--screen-min-score", help="Minimum composite screen score."),
    ] = 60.0,
    kis_paper_candidates: Annotated[
        bool,
        typer.Option(
            "--kis-paper-candidates/--no-kis-paper-candidates",
            help="Build KIS mock-account review candidates after screening. No orders are sent.",
        ),
    ] = False,
    kis_candidate_max_candidates: Annotated[
        int,
        typer.Option(
            "--kis-candidate-max-candidates",
            help="Maximum KIS paper candidate rows to create.",
        ),
    ] = 10,
    kis_candidate_cash_buffer_pct: Annotated[
        float,
        typer.Option(
            "--kis-candidate-cash-buffer-pct",
            help="Cash percentage reserved from KIS paper candidate sizing.",
        ),
    ] = 5.0,
    kis_timeout_seconds: Annotated[
        float,
        typer.Option(
            "--kis-timeout-seconds",
            help="HTTP timeout for KIS paper candidate balance inquiry.",
        ),
    ] = 10.0,
    refresh_dashboard_artifacts: Annotated[
        bool,
        typer.Option(
            "--refresh-dashboard-artifacts/--no-refresh-dashboard-artifacts",
            help=(
                "Refresh same-period macro, backtest, paper, walk-forward, "
                "and ML dashboard artifacts."
            ),
        ),
    ] = True,
    score_external_features: Annotated[
        bool,
        typer.Option(
            "--score-external-features/--no-score-external-features",
            help="Collect news and macro features before scoring the universe.",
        ),
    ] = True,
    dashboard_artifact_ticker: Annotated[
        str | None,
        typer.Option(
            "--dashboard-artifact-ticker",
            help=(
                "Ticker used for single-stock dashboard validation artifacts. "
                "Defaults to top ranked success."
            ),
        ),
    ] = None,
    macro_live: Annotated[
        bool,
        typer.Option(
            "--macro-live/--macro-demo",
            help="Use live FRED macro data when FRED_API_KEY is set, otherwise demo fallback.",
        ),
    ] = True,
    news_live: Annotated[
        bool,
        typer.Option(
            "--news-live/--news-demo",
            help="Use live Naver news when credentials are set, otherwise demo fallback.",
        ),
    ] = True,
    news_gemini: Annotated[
        bool,
        typer.Option(
            "--news-gemini/--news-rule-based",
            help="Use Gemini for news sentiment when GEMINI_API_KEY is set.",
        ),
    ] = False,
    news_display: Annotated[
        int,
        typer.Option("--news-display", help="Number of Naver news search items per ticker."),
    ] = 5,
    evaluate_journal: Annotated[
        bool,
        typer.Option(
            "--evaluate-journal/--no-evaluate-journal",
            help="Refresh the decision journal outcome comparison after writing today's records.",
        ),
    ] = True,
    journal_holding_days: Annotated[
        int,
        typer.Option(
            "--journal-holding-days",
            help="Trading-day horizon for decision journal outcome comparison.",
        ),
    ] = 5,
) -> None:
    """Run the after-market daily job: universe, screener, paper portfolio, and Telegram."""
    configure_logger(settings.log_level)
    kis_candidate_source = None
    if kis_paper_candidates:
        try:
            kis_credentials = KISPaperCredentials.from_settings(settings)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        kis_candidate_source = _CLIKISPaperCandidateSource(
            project_root=settings.project_root,
            credentials=kis_credentials,
            timeout_seconds=kis_timeout_seconds,
            token_cache_path=_kis_token_cache_path(),
        )

    runner = DailyJobRunner(
        project_root=settings.project_root,
        telegram_sender=TelegramNotifier(
            bot_token=settings.telegram_bot_token,
            chat_id=settings.telegram_chat_id,
            timeout_seconds=settings.telegram_timeout_seconds,
            max_retries=settings.telegram_max_retries,
            retry_sleep_seconds=settings.telegram_retry_sleep_seconds,
        ),
        kis_candidate_source=kis_candidate_source,
    )
    try:
        result = runner.run(
            DailyJobConfig(
                universe=universe,
                start_date=start,
                end_date=end,
                lookback_days=lookback_days,
                notify=notify,
                telegram_dry_run=telegram_dry_run,
                telegram_top_n=top_n,
                paper_trade=paper_trading,
                paper_initial_cash=paper_initial_cash,
                paper_max_position_pct=paper_max_position_pct,
                screening=screening,
                screening_min_confidence=screening_min_confidence,
                screening_min_score=screening_min_score,
                kis_paper_candidates=kis_paper_candidates,
                kis_candidate_max_candidates=kis_candidate_max_candidates,
                kis_candidate_cash_buffer_pct=kis_candidate_cash_buffer_pct,
                refresh_dashboard_artifacts=refresh_dashboard_artifacts,
                score_external_features=score_external_features,
                dashboard_artifact_ticker=dashboard_artifact_ticker,
                macro_live=macro_live,
                fred_api_key=settings.fred_api_key,
                news_live=news_live,
                news_display=news_display,
                news_use_gemini=news_gemini,
                naver_client_id=settings.naver_client_id,
                naver_client_secret=settings.naver_client_secret,
                gemini_api_key=settings.gemini_api_key,
                evaluate_decision_journal=evaluate_journal,
                decision_journal_holding_days=journal_holding_days,
            )
        )
    except (KeyError, ValueError) as exc:
        raise typer.BadParameter(str(exc)) from exc

    console.print("[bold green]Daily scheduled job completed.[/bold green]")
    console.print(f"Universe: {result.universe}")
    console.print(f"Period: {result.start_date} to {result.end_date}")
    console.print(f"Total: {result.total_count}")
    console.print(f"Success: {result.success_count}")
    console.print(f"Failed: {result.failed_count}")
    console.print(f"Summary: {result.summary_path}")
    console.print(f"CSV: {result.summary_csv_path}")
    console.print(f"Report: {result.report_path}")
    if result.paper_summary_path:
        console.print(f"Paper summary: {result.paper_summary_path}")
        console.print(f"Paper report: {result.paper_report_path}")
        console.print(f"Paper trades: {result.paper_trade_count}")
        console.print(f"Paper return: {result.paper_cumulative_return * 100:.2f}%")
    if result.screening_result_path:
        console.print(f"Screening result: {result.screening_result_path}")
        console.print(f"Screening CSV: {result.screening_csv_path}")
        console.print(f"Screening report: {result.screening_report_path}")
        console.print(
            f"Screening passed: {result.screening_passed_count}/{result.screening_checked_count}"
        )
    if result.kis_candidate_result_path:
        console.print(f"KIS candidates: {result.kis_candidate_result_path}")
        console.print(f"KIS candidates CSV: {result.kis_candidate_csv_path}")
        console.print(f"KIS candidates report: {result.kis_candidate_report_path}")
        console.print(
            "KIS review candidates: "
            f"{result.kis_candidate_review_count}/{result.kis_candidate_count}"
        )
        console.print(f"KIS manual price checks: {result.kis_candidate_manual_price_count}")
        if result.manual_order_plan_path:
            console.print(f"Manual order plan: {result.manual_order_plan_path}")
            console.print(f"Manual order plan CSV: {result.manual_order_plan_csv_path}")
            console.print(f"Manual order plan report: {result.manual_order_plan_report_path}")
            console.print(f"Manual buy/add review rows: {result.manual_order_plan_review_count}")
        console.print("KIS mode: paper candidate review only. No order was sent.")
    console.print(f"Decision journal: {result.decision_journal_path}")
    console.print(f"Decision journal CSV: {result.decision_journal_csv_path}")
    console.print(
        "Decision journal rows: "
        f"+{result.decision_journal_appended_count} / total {result.decision_journal_total_count}"
    )
    if result.decision_journal_evaluation_path:
        console.print(f"Decision journal evaluation: {result.decision_journal_evaluation_path}")
        console.print(
            f"Decision journal evaluation CSV: {result.decision_journal_evaluation_csv_path}"
        )
        console.print(
            f"Decision journal evaluation report: {result.decision_journal_evaluation_report_path}"
        )
        console.print(
            "Decision journal evaluation rows: "
            f"evaluated {result.decision_journal_evaluated_count} / "
            f"pending {result.decision_journal_pending_count}"
        )
    if result.scoring_macro_feature_path:
        console.print(f"Scoring macro features: {result.scoring_macro_feature_path}")
    if result.scoring_news_feature_paths:
        console.print(f"Scoring news sentiment files: {len(result.scoring_news_feature_paths)}")
    if result.scoring_feature_errors:
        console.print("[yellow]Scoring feature notes:[/yellow]")
        for error in result.scoring_feature_errors:
            console.print(f"- {error}")
    if result.dashboard_artifact_ticker or result.macro_feature_path:
        console.print(f"Dashboard artifact ticker: {result.dashboard_artifact_ticker or 'N/A'}")
    if result.macro_feature_path:
        console.print(f"Macro features: {result.macro_feature_path}")
    if result.backtest_metrics_path:
        console.print(f"Backtest metrics: {result.backtest_metrics_path}")
    if result.paper_single_summary_path:
        console.print(f"Single paper summary: {result.paper_single_summary_path}")
    if result.walk_forward_summary_path:
        console.print(f"Walk-forward summary: {result.walk_forward_summary_path}")
    if result.ml_metrics_path:
        console.print(f"ML metrics: {result.ml_metrics_path}")
    if result.dashboard_artifact_errors:
        console.print("[yellow]Dashboard artifact notes:[/yellow]")
        for error in result.dashboard_artifact_errors:
            console.print(f"- {error}")
    console.print(f"Operations health: {result.operations_health_path}")
    console.print(f"Operations report: {result.operations_health_report_path}")
    console.print(f"Experiment log: {result.experiment_log_path}")
    if notify:
        status = (
            "sent" if result.telegram_sent else "dry-run" if result.telegram_dry_run else "failed"
        )
        console.print(f"Telegram: {status}")
        if result.telegram_response_text and not result.telegram_sent:
            console.print(f"[yellow]{result.telegram_response_text}[/yellow]")
        if result.telegram_dry_run:
            console.print(result.telegram_message)


@app.command("record-decision-journal")
def record_decision_journal(
    universe: Annotated[
        str,
        typer.Option("--universe", "-u", help="Universe name to store in the journal."),
    ] = "large_cap",
    summary_path: Annotated[
        Path | None,
        typer.Option(
            "--summary-path",
            help="Universe summary parquet path. Uses latest if omitted.",
        ),
    ] = None,
    screening_path: Annotated[
        Path | None,
        typer.Option("--screening-path", help="Screening parquet path. Uses latest if omitted."),
    ] = None,
    kis_candidate_path: Annotated[
        Path | None,
        typer.Option(
            "--kis-candidate-path",
            help="KIS candidate parquet path. Uses latest if omitted.",
        ),
    ] = None,
    start: Annotated[
        str | None,
        typer.Option(
            "--start",
            help="Analysis start date. Inferred from summary filename if omitted.",
        ),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", help="Analysis end date. Inferred from summary filename if omitted."),
    ] = None,
) -> None:
    """Append an existing universe run to the durable decision journal."""
    resolved_summary_path = summary_path or find_latest_universe_summary(settings.project_root)
    if resolved_summary_path is None or not resolved_summary_path.exists():
        raise typer.BadParameter("Universe summary path does not exist.")

    if start and end:
        start_date = start
        end_date = end
    else:
        inferred_start, inferred_end = _infer_summary_period(resolved_summary_path)
        start_date = start or inferred_start
        end_date = end or inferred_end
    summary_frame = read_parquet(resolved_summary_path)

    resolved_screening_path = screening_path or find_latest_screening_result(settings.project_root)
    screening_frame = (
        load_screening_result(resolved_screening_path)
        if resolved_screening_path is not None and resolved_screening_path.exists()
        else None
    )
    resolved_kis_path = kis_candidate_path or find_latest_kis_paper_candidates(
        settings.project_root
    )
    kis_candidate_frame = (
        load_kis_paper_candidates(resolved_kis_path)
        if resolved_kis_path is not None and resolved_kis_path.exists()
        else None
    )

    journal_rows = build_decision_journal_frame(
        universe=universe,
        start_date=start_date,
        end_date=end_date,
        summary_frame=summary_frame,
        screening_frame=screening_frame,
        kis_candidate_frame=kis_candidate_frame,
    )
    result = append_decision_journal(settings.project_root, journal_rows)
    console.print("[bold green]Decision journal updated.[/bold green]")
    console.print(f"Summary: {resolved_summary_path}")
    console.print(f"Journal: {result.parquet_path}")
    console.print(f"CSV: {result.csv_path}")
    console.print(f"Rows appended: {result.appended_count}")
    console.print(f"Total rows: {result.total_count}")


@app.command("evaluate-decision-journal")
def evaluate_decision_journal_command(
    holding_days: Annotated[
        int,
        typer.Option("--holding-days", help="Trading-day horizon used for outcome comparison."),
    ] = 5,
    journal_path: Annotated[
        Path | None,
        typer.Option(
            "--journal-path",
            help="Decision journal parquet path. Uses default if omitted.",
        ),
    ] = None,
) -> None:
    """Compare stored decisions with later realized price movement."""
    resolved_journal_path = journal_path or decision_journal_file_path(settings.project_root)
    if not resolved_journal_path.exists():
        raise typer.BadParameter(
            "Decision journal does not exist yet. "
            "Run run-daily-job or record-decision-journal first."
        )

    journal_frame = read_parquet(resolved_journal_path)
    result = evaluate_decision_journal(
        settings.project_root,
        holding_days=holding_days,
        journal_frame=journal_frame,
    )
    console.print("[bold green]Decision journal evaluated.[/bold green]")
    console.print(f"Journal: {resolved_journal_path}")
    console.print(f"Result: {result.parquet_path}")
    console.print(f"CSV: {result.csv_path}")
    console.print(f"Report: {result.report_path}")
    console.print(f"Evaluated: {result.evaluated_count}")
    console.print(f"Pending: {result.pending_count}")
    if result.summary.empty:
        console.print("[yellow]No evaluated summary rows yet.[/yellow]")
        return

    table = Table(title=f"Decision Journal Evaluation h{holding_days}")
    table.add_column("Action")
    table.add_column("Decisions", justify="right")
    table.add_column("Evaluated", justify="right")
    table.add_column("Avg Return", justify="right")
    table.add_column("Favorable", justify="right")
    for _, row in result.summary.iterrows():
        table.add_row(
            str(row["latest_action"]),
            str(int(row["decision_count"])),
            str(int(row["evaluated_count"])),
            f"{float(row['average_forward_return']) * 100:.2f}%",
            f"{float(row['favorable_rate']) * 100:.2f}%",
        )
    console.print(table)


@app.command("show-experiments")
def show_experiments(
    limit: Annotated[
        int,
        typer.Option("--limit", help="Number of recent experiment rows to display."),
    ] = 10,
) -> None:
    """Show recent local experiment tracking rows."""
    tracker = ExperimentTracker(settings.project_root)
    frame = tracker.load()
    if frame.empty:
        console.print("[yellow]No experiment log found.[/yellow]")
        console.print(f"Expected path: {tracker.path}")
        return

    display_columns = [
        "created_at",
        "experiment_name",
        "run_type",
        "ticker",
        "universe",
        "start_date",
        "end_date",
        "metrics_json",
        "artifact_path",
    ]
    console.print(frame.tail(limit)[display_columns])


@app.command("detect-data-drift")
def detect_data_drift(
    reference_path: Annotated[
        Path,
        typer.Option("--reference-path", help="Reference feature parquet path."),
    ],
    current_path: Annotated[
        Path,
        typer.Option("--current-path", help="Current feature parquet path."),
    ],
    columns: Annotated[
        str | None,
        typer.Option(
            "--columns",
            help="Comma-separated numeric columns. Uses common numeric columns if omitted.",
        ),
    ] = None,
    output_name: Annotated[
        str | None,
        typer.Option("--output-name", help="Output report name without extension."),
    ] = None,
    mean_shift_threshold: Annotated[
        float,
        typer.Option("--mean-shift-threshold", help="Mean shift threshold in reference std units."),
    ] = 1.0,
) -> None:
    """Detect feature distribution drift between two parquet datasets."""
    if not reference_path.exists():
        raise typer.BadParameter(f"Reference file does not exist: {reference_path}")
    if not current_path.exists():
        raise typer.BadParameter(f"Current file does not exist: {current_path}")

    reference_frame = read_parquet(reference_path)
    current_frame = read_parquet(current_path)
    result_frame = DataDriftDetector(
        DataDriftConfig(mean_shift_threshold=mean_shift_threshold)
    ).detect(
        reference_frame=reference_frame,
        current_frame=current_frame,
        columns=_parse_optional_columns(columns),
    )
    if result_frame.empty:
        raise typer.BadParameter("No comparable numeric columns were found for drift detection.")

    report_name = _safe_report_name(
        output_name or f"data_drift_{reference_path.stem}_vs_{current_path.stem}"
    )
    result_path = drift_result_file_path(settings.project_root, report_name)
    report_path = monitoring_report_file_path(settings.project_root, report_name)
    write_parquet(result_frame, result_path)
    write_text(format_data_drift_report(result_frame), report_path)

    drift_count = int(result_frame["drift_detected"].sum())
    console.print("[bold green]Data drift detection completed.[/bold green]")
    console.print(f"Checked features: {len(result_frame)}")
    console.print(f"Drifted features: {drift_count}")
    console.print(f"Result: {result_path}")
    console.print(f"Report: {report_path}")


@app.command("detect-performance-drift")
def detect_performance_drift(
    run_type: Annotated[
        str,
        typer.Option("--run-type", help="Experiment run_type to monitor."),
    ] = "backtest",
    metric: Annotated[
        str,
        typer.Option("--metric", help="Metric name stored in metrics_json."),
    ] = "cumulative_return",
    baseline_window: Annotated[
        int,
        typer.Option("--baseline-window", help="Number of older runs for baseline."),
    ] = 5,
    recent_window: Annotated[
        int,
        typer.Option("--recent-window", help="Number of latest runs to compare."),
    ] = 3,
    output_name: Annotated[
        str | None,
        typer.Option("--output-name", help="Output report name without extension."),
    ] = None,
) -> None:
    """Detect performance drift from the local CSV experiment log."""
    tracker = ExperimentTracker(settings.project_root)
    experiment_frame = tracker.load()
    result_frame = PerformanceDriftDetector(
        PerformanceDriftConfig(
            run_type=run_type,
            metric=metric,
            baseline_window=baseline_window,
            recent_window=recent_window,
        )
    ).detect(experiment_frame)

    report_name = _safe_report_name(output_name or f"performance_drift_{run_type}_{metric}")
    result_path = drift_result_file_path(settings.project_root, report_name)
    report_path = monitoring_report_file_path(settings.project_root, report_name)
    write_parquet(result_frame, result_path)
    write_text(format_performance_drift_report(result_frame), report_path)

    row = result_frame.iloc[0]
    console.print("[bold green]Performance drift detection completed.[/bold green]")
    console.print(f"Run type: {row['run_type']}")
    console.print(f"Metric: {row['metric']}")
    console.print(f"Baseline mean: {float(row['baseline_mean']):.4f}")
    console.print(f"Recent mean: {float(row['recent_mean']):.4f}")
    console.print(f"Drift detected: {bool(row['drift_detected'])}")
    console.print(f"Reason: {row['drift_reason']}")
    console.print(f"Result: {result_path}")
    console.print(f"Report: {report_path}")


def _parse_optional_columns(columns: str | None) -> list[str] | None:
    if columns is None:
        return None
    return [column.strip() for column in columns.split(",") if column.strip()]


def _format_api_status(status: str) -> str:
    if status == API_STATUS_OK:
        return "[green]OK[/green]"
    if status == API_STATUS_MISSING:
        return "[yellow]MISSING[/yellow]"
    if status == API_STATUS_FAILED:
        return "[red]FAILED[/red]"
    return status


def _format_health_status(status: str) -> str:
    if status == HEALTH_STATUS_OK:
        return "[green]OK[/green]"
    if status in {HEALTH_STATUS_WARN, HEALTH_STATUS_STALE}:
        return f"[yellow]{status}[/yellow]"
    if status in {HEALTH_STATUS_MISSING, HEALTH_STATUS_EMPTY, HEALTH_STATUS_FAILED}:
        return f"[red]{status}[/red]"
    return status


def _format_optional_table_value(value: object, decimals: int) -> str:
    if pd.isna(value):
        return ""
    if decimals == 0:
        return str(int(cast(Any, value)))
    return f"{float(cast(Any, value)):.{decimals}f}"


def _format_screening_counts(frame: pd.DataFrame, column: str) -> str:
    if column not in frame.columns:
        return "N/A"
    counts = frame[column].fillna("unknown").astype(str).value_counts()
    return ", ".join(f"{name} {count}" for name, count in counts.items())


def _short_console_text(value: object, limit: int = 72) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _parse_console_filter_values(value: str | None) -> set[str]:
    if value is None:
        return set()
    return {item.strip().lower() for item in value.split(",") if item.strip()}


def _screening_cli_display_columns(compact: bool) -> list[str]:
    if compact:
        return [
            "ticker",
            "passed",
            "review_priority",
            "screen_status_reason",
            "screen_score",
            "confidence_score",
            "suggested_position_pct",
        ]
    return [
        "ticker",
        "passed",
        "screen_status_reason",
        "review_priority",
        "screen_score",
        "final_action",
        "confidence_score",
        "suggested_position_pct",
        "reasons",
    ]


def _infer_summary_period(path: Path) -> tuple[str, str]:
    stem = path.stem
    parts = stem.split("_")
    if len(parts) >= 3 and parts[-2].isdigit() and parts[-1].isdigit():
        return _compact_date(parts[-2]), _compact_date(parts[-1])
    raise typer.BadParameter(
        "Could not infer start/end dates from summary filename. Pass --start and --end."
    )


def _compact_date(value: str) -> str:
    if len(value) == 8:
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def _safe_report_name(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "_-" else "_" for character in value
    )


def _contains_certificate_error(exc: BaseException) -> bool:
    current: BaseException | None = exc
    while current is not None:
        if "CERTIFICATE_VERIFY_FAILED" in str(current):
            return True
        current = current.__cause__ or current.__context__
    return False
