from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console

from krx_alpha.backtest.simple_backtester import BacktestConfig, SimpleBacktester
from krx_alpha.collectors.dart_collector import (
    DartCompanyRequest,
    DartDisclosureSearchRequest,
    DartFinancialStatementRequest,
    OpenDartCollector,
    resolve_corp_code,
)
from krx_alpha.collectors.price_collector import PriceRequest, PykrxPriceCollector
from krx_alpha.configs.settings import settings
from krx_alpha.database.storage import (
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
    ensure_project_dirs,
    final_signal_file_path,
    market_regime_file_path,
    market_regime_report_file_path,
    price_feature_file_path,
    processed_price_file_path,
    raw_price_file_path,
    read_parquet,
    universe_csv_path,
    universe_file_path,
    universe_report_file_path,
    universe_summary_file_path,
    write_csv,
    write_parquet,
    write_text,
)
from krx_alpha.features.dart_disclosure_events import DartDisclosureEventBuilder
from krx_alpha.features.dart_financial_features import DartFinancialFeatureBuilder
from krx_alpha.features.price_features import PriceFeatureBuilder
from krx_alpha.pipelines.daily_pipeline import DailyPipeline
from krx_alpha.pipelines.universe_pipeline import UniversePipeline
from krx_alpha.processors.price_processor import PriceProcessor
from krx_alpha.regime.market_regime import MarketRegimeAnalyzer
from krx_alpha.reports.backtest_report import BacktestReportGenerator
from krx_alpha.reports.daily_report import DailyReportGenerator
from krx_alpha.reports.regime_report import MarketRegimeReportGenerator
from krx_alpha.reports.universe_report import UniverseReportGenerator
from krx_alpha.scoring.price_scorer import PriceScorer
from krx_alpha.signals.signal_engine import SignalEngine
from krx_alpha.universe.static_universe import UniverseRegistry
from krx_alpha.utils.logger import configure_logger

app = typer.Typer(help="KRX Alpha Platform command line interface")
console = Console()


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


@app.command()
def doctor() -> None:
    """Check whether the local project environment is ready."""
    configure_logger(settings.log_level)
    ensure_project_dirs(settings.project_root)

    console.print("[bold green]KRX Alpha Platform is ready.[/bold green]")
    console.print(f"Environment: {settings.environment}")
    console.print(f"Project root: {settings.project_root}")
    console.print(f"Data root: {settings.data_dir}")


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

    frame = PykrxPriceCollector().collect(request)
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
    score_frame = PriceScorer().score(feature_frame, financial_frame, event_frame)
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
    console.print(f"Reason: {latest['score_reason']}")
    console.print(f"Financial reason: {latest['financial_reason']}")
    console.print(f"Event reason: {latest['event_reason']}")
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
    result = DailyPipeline(settings.project_root).run(request, financial_frame, event_frame)

    console.print("[bold green]Daily pipeline completed.[/bold green]")
    console.print(f"Raw: {result.raw_path}")
    console.print(f"Processed: {result.processed_path}")
    console.print(f"Features: {result.feature_path}")
    console.print(f"Regime: {result.regime_path}")
    console.print(f"Scores: {result.score_path}")
    console.print(f"Signals: {result.signal_path}")
    console.print(f"Report: {result.report_path}")
    console.print(f"Latest action: {result.latest_action}")
    console.print(f"Confidence: {result.latest_confidence_score:.2f}")
    console.print(f"Financial score: {result.latest_financial_score:.2f}")
    console.print(f"Event score: {result.latest_event_score:.2f}")
    console.print(f"Market regime: {result.latest_market_regime}")


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
    trades, metrics = SimpleBacktester(BacktestConfig(holding_days=holding_days)).run(
        price_frame,
        signal_frame,
    )

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

    metric = metrics.iloc[0]
    console.print("[bold green]Backtest completed.[/bold green]")
    console.print(f"Trades: {int(metric['trade_count'])}")
    console.print(f"Win rate: {float(metric['win_rate']) * 100:.2f}%")
    console.print(f"Cumulative return: {float(metric['cumulative_return']) * 100:.2f}%")
    console.print(f"Max drawdown: {float(metric['max_drawdown']) * 100:.2f}%")
    console.print(f"Sharpe ratio: {float(metric['sharpe_ratio']):.2f}")
    console.print(f"Report: {report_path}")
