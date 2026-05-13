from pathlib import Path
from typing import Annotated

import pandas as pd
import typer
from rich.console import Console

from krx_alpha.backtest.simple_backtester import BacktestConfig, SimpleBacktester
from krx_alpha.collectors.price_collector import PriceRequest, PykrxPriceCollector
from krx_alpha.configs.settings import settings
from krx_alpha.database.storage import (
    backtest_metrics_file_path,
    backtest_report_file_path,
    backtest_trades_file_path,
    daily_report_file_path,
    daily_score_file_path,
    ensure_project_dirs,
    final_signal_file_path,
    price_feature_file_path,
    processed_price_file_path,
    raw_price_file_path,
    read_parquet,
    universe_report_file_path,
    universe_summary_file_path,
    write_parquet,
    write_text,
)
from krx_alpha.features.price_features import PriceFeatureBuilder
from krx_alpha.pipelines.daily_pipeline import DailyPipeline
from krx_alpha.pipelines.universe_pipeline import UniversePipeline
from krx_alpha.processors.price_processor import PriceProcessor
from krx_alpha.reports.backtest_report import BacktestReportGenerator
from krx_alpha.reports.daily_report import DailyReportGenerator
from krx_alpha.reports.universe_report import UniverseReportGenerator
from krx_alpha.scoring.price_scorer import PriceScorer
from krx_alpha.signals.signal_engine import SignalEngine
from krx_alpha.utils.logger import configure_logger

app = typer.Typer(help="KRX Alpha Platform command line interface")
console = Console()


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
    score_frame = PriceScorer().score(feature_frame)
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
    console.print(f"Reason: {latest['score_reason']}")
    console.print(f"Output: {output_path}")


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
    signal_frame = SignalEngine().generate(score_frame, feature_frame)
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
) -> None:
    """Run collect, process, feature, score, signal, and report steps at once."""
    configure_logger(settings.log_level)
    request = PriceRequest.from_strings(ticker=ticker, start_date=start, end_date=end)
    result = DailyPipeline(settings.project_root).run(request)

    console.print("[bold green]Daily pipeline completed.[/bold green]")
    console.print(f"Raw: {result.raw_path}")
    console.print(f"Processed: {result.processed_path}")
    console.print(f"Features: {result.feature_path}")
    console.print(f"Scores: {result.score_path}")
    console.print(f"Signals: {result.signal_path}")
    console.print(f"Report: {result.report_path}")
    console.print(f"Latest action: {result.latest_action}")
    console.print(f"Confidence: {result.latest_confidence_score:.2f}")


@app.command("run-universe")
def run_universe(
    tickers: Annotated[
        str,
        typer.Option(
            "--tickers",
            "-t",
            help="Comma-separated tickers. Example: 005930,000660,005380",
        ),
    ] = "005930,000660,005380",
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
    ticker_list = [ticker.strip() for ticker in tickers.split(",") if ticker.strip()]
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
    console.print(f"Total: {result.total_count}")
    console.print(f"Success: {result.success_count}")
    console.print(f"Failed: {result.failed_count}")
    console.print(f"Summary: {result.summary_path}")
    console.print(f"CSV: {result.summary_csv_path}")
    console.print(summary_frame[["ticker", "status", "latest_action", "latest_confidence_score"]])


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
