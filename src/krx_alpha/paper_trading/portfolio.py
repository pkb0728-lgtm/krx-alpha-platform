from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from krx_alpha.collectors.price_collector import PriceRequest
from krx_alpha.database.storage import (
    final_signal_file_path,
    paper_portfolio_position_file_path,
    paper_portfolio_report_file_path,
    paper_portfolio_summary_file_path,
    paper_portfolio_trade_ledger_file_path,
    processed_price_file_path,
    read_parquet,
    write_parquet,
    write_text,
)
from krx_alpha.paper_trading.simulator import PaperTradingConfig, PaperTradingSimulator
from krx_alpha.reports.paper_trading_report import PaperTradingReportGenerator


@dataclass(frozen=True)
class PaperPortfolioConfig:
    name: str
    tickers: tuple[str, ...]
    start_date: str
    end_date: str
    initial_cash: float = 10_000_000.0
    max_position_pct: float = 10.0
    transaction_cost_bps: float = 15.0
    slippage_bps: float = 10.0
    skip_missing: bool = True


@dataclass(frozen=True)
class PaperPortfolioResult:
    ledger_path: Path
    positions_path: Path
    summary_path: Path
    report_path: Path
    trades: Any
    positions: Any
    summary: Any
    requested_ticker_count: int
    loaded_ticker_count: int
    skipped_tickers: tuple[str, ...]


def run_paper_portfolio(project_root: Path, config: PaperPortfolioConfig) -> PaperPortfolioResult:
    price_frames: list[pd.DataFrame] = []
    signal_frames: list[pd.DataFrame] = []
    skipped_tickers: list[str] = []
    missing_details: list[str] = []

    for ticker in config.tickers:
        request = PriceRequest.from_strings(
            ticker=ticker,
            start_date=config.start_date,
            end_date=config.end_date,
        )
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
        missing_reasons = _missing_input_reasons(price_path, signal_path)
        if missing_reasons:
            skipped_tickers.append(request.ticker)
            missing_details.append(f"{request.ticker} ({'; '.join(missing_reasons)})")
            continue

        price_frames.append(read_parquet(price_path))
        signal_frames.append(read_parquet(signal_path))

    if missing_details and not config.skip_missing:
        raise ValueError("Missing paper portfolio inputs: " + " | ".join(missing_details))
    if not price_frames or not signal_frames:
        raise ValueError(
            "No ticker inputs were available. "
            "Run run-universe first, then retry paper portfolio simulation."
        )

    trading_config = PaperTradingConfig(
        initial_cash=config.initial_cash,
        max_position_pct=config.max_position_pct,
        transaction_cost_bps=config.transaction_cost_bps,
        slippage_bps=config.slippage_bps,
    )
    trades, positions, summary = PaperTradingSimulator(trading_config).run(
        pd.concat(price_frames, ignore_index=True),
        pd.concat(signal_frames, ignore_index=True),
    )
    summary = annotate_paper_portfolio_summary(
        summary=summary,
        positions=positions,
        portfolio_name=config.name,
        requested_ticker_count=len(config.tickers),
        loaded_ticker_count=len(price_frames),
        skipped_tickers=skipped_tickers,
    )

    portfolio_slug = safe_portfolio_name(config.name)
    start_compact = config.start_date.replace("-", "")
    end_compact = config.end_date.replace("-", "")
    ledger_path = paper_portfolio_trade_ledger_file_path(
        project_root,
        portfolio_slug,
        start_compact,
        end_compact,
    )
    positions_path = paper_portfolio_position_file_path(
        project_root,
        portfolio_slug,
        start_compact,
        end_compact,
    )
    summary_path = paper_portfolio_summary_file_path(
        project_root,
        portfolio_slug,
        start_compact,
        end_compact,
    )
    report_path = paper_portfolio_report_file_path(
        project_root,
        portfolio_slug,
        start_compact,
        end_compact,
    )
    write_parquet(trades, ledger_path)
    write_parquet(positions, positions_path)
    write_parquet(summary, summary_path)
    write_text(PaperTradingReportGenerator().generate(trades, positions, summary), report_path)

    return PaperPortfolioResult(
        ledger_path=ledger_path,
        positions_path=positions_path,
        summary_path=summary_path,
        report_path=report_path,
        trades=trades,
        positions=positions,
        summary=summary,
        requested_ticker_count=len(config.tickers),
        loaded_ticker_count=len(price_frames),
        skipped_tickers=tuple(skipped_tickers),
    )


def annotate_paper_portfolio_summary(
    summary: pd.DataFrame,
    positions: pd.DataFrame,
    portfolio_name: str,
    requested_ticker_count: int,
    loaded_ticker_count: int,
    skipped_tickers: list[str],
) -> pd.DataFrame:
    result = summary.copy()
    ending_equity = float(result.loc[0, "ending_equity"])
    ending_cash = float(result.loc[0, "ending_cash"])
    ending_position_value = float(result.loc[0, "ending_position_value"])
    result.insert(0, "universe", portfolio_name)
    result["requested_ticker_count"] = requested_ticker_count
    result["loaded_ticker_count"] = loaded_ticker_count
    result["skipped_tickers"] = ",".join(skipped_tickers)
    result["active_position_count"] = int(len(positions))
    result["gross_exposure_pct"] = (
        ending_position_value / ending_equity * 100 if ending_equity else 0.0
    )
    result["cash_pct"] = ending_cash / ending_equity * 100 if ending_equity else 0.0
    return result


def safe_portfolio_name(value: str) -> str:
    return "".join(
        character if character.isalnum() or character in "_-" else "_" for character in value
    )


def _missing_input_reasons(price_path: Path, signal_path: Path) -> list[str]:
    missing_reasons: list[str] = []
    if not price_path.exists():
        missing_reasons.append(f"processed price missing: {price_path}")
    if not signal_path.exists():
        missing_reasons.append(f"final signal missing: {signal_path}")
    return missing_reasons
