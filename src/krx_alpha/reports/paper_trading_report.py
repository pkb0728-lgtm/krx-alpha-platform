from typing import Any

import pandas as pd

from krx_alpha.contracts.paper_trading_contract import (
    validate_paper_positions,
    validate_paper_summary,
    validate_paper_trades,
)


class PaperTradingReportGenerator:
    """Generate a Markdown report for paper-only trading simulation results."""

    def generate(self, trades: Any, positions: Any, summary: Any) -> str:
        validate_paper_trades(trades)
        validate_paper_positions(positions)
        validate_paper_summary(summary)

        metric = summary.iloc[0]
        return "\n".join(
            [
                "# Paper Trading Report",
                "",
                "> Paper trading only. No broker API or real order was called.",
                "",
                *_optional_portfolio_lines(metric),
                f"- Ticker: {metric['ticker']}",
                f"- Initial cash: {_format_money(metric['initial_cash'])}",
                f"- Ending cash: {_format_money(metric['ending_cash'])}",
                f"- Ending position value: {_format_money(metric['ending_position_value'])}",
                f"- Ending equity: {_format_money(metric['ending_equity'])}",
                f"- Cumulative return: {_format_percent(metric['cumulative_return'])}",
                f"- Realized PnL: {_format_money(metric['realized_pnl'])}",
                f"- Unrealized PnL: {_format_money(metric['unrealized_pnl'])}",
                f"- Filled trades: {int(metric['trade_count'])}",
                f"- Buy/Sell count: {int(metric['buy_count'])}/{int(metric['sell_count'])}",
                f"- Win rate: {_format_percent(metric['win_rate'])}",
                "",
                "## Open Positions",
                "",
                _format_positions(positions),
                "",
                "## Recent Ledger",
                "",
                _format_trades(trades),
                "",
                "## Risk Note",
                "",
                "Paper trading validates workflow behavior. It is not investment advice and "
                "does not guarantee live execution quality.",
                "",
            ]
        )


def _optional_portfolio_lines(metric: Any) -> list[str]:
    lines: list[str] = []
    if "universe" in metric.index:
        lines.append(f"- Universe: {metric['universe']}")
    if "requested_ticker_count" in metric.index:
        lines.append(f"- Requested tickers: {int(metric['requested_ticker_count'])}")
    if "loaded_ticker_count" in metric.index:
        lines.append(f"- Loaded tickers: {int(metric['loaded_ticker_count'])}")
    if "skipped_tickers" in metric.index and str(metric["skipped_tickers"]):
        lines.append(f"- Skipped tickers: {metric['skipped_tickers']}")
    if lines:
        lines.append("")
    return lines


def _format_positions(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No open paper positions."

    rows = [
        "| Ticker | Shares | Avg Price | Last Price | Market Value | Unrealized PnL | Position % |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for _, row in frame.iterrows():
        rows.append(
            "| "
            f"{row['ticker']} | "
            f"{int(row['shares'])} | "
            f"{_format_number(row['average_price'])} | "
            f"{_format_number(row['last_price'])} | "
            f"{_format_money(row['market_value'])} | "
            f"{_format_money(row['unrealized_pnl'])} | "
            f"{_format_number(row['position_pct'])}% |"
        )
    return "\n".join(rows)


def _format_trades(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No paper trades were generated."

    rows = [
        "| Signal Date | Execution Date | Side | Status | Shares | Price | Equity | Reason |",
        "| --- | --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    recent = frame.tail(10)
    for _, row in recent.iterrows():
        rows.append(
            "| "
            f"{_format_date(row['date'])} | "
            f"{_format_date(row['execution_date'])} | "
            f"{row['side']} | "
            f"{row['status']} | "
            f"{int(row['shares'])} | "
            f"{_format_number(row['execution_price'])} | "
            f"{_format_money(row['equity_after'])} | "
            f"{row['reason']} |"
        )
    return "\n".join(rows)


def _format_money(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.0f}"


def _format_number(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.2f}"


def _format_percent(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value) * 100:.2f}%"


def _format_date(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return str(pd.Timestamp(value).strftime("%Y-%m-%d"))
