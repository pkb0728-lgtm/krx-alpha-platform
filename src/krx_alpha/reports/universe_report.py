from typing import Any

import pandas as pd

REQUIRED_UNIVERSE_SUMMARY_COLUMNS = {
    "ticker",
    "status",
    "latest_action",
    "latest_confidence_score",
    "signal_path",
    "report_path",
    "error",
}


class UniverseReportGenerator:
    """Generate a Markdown report for multi-stock universe screening results."""

    def generate(self, summary_frame: Any, start_date: str, end_date: str) -> str:
        _validate_summary_frame(summary_frame)

        frame = summary_frame.copy()
        success_frame = frame[frame["status"] == "success"].sort_values(
            "latest_confidence_score",
            ascending=False,
        )
        failed_frame = frame[frame["status"] == "failed"]

        return "\n".join(
            [
                "# Universe Screening Report",
                "",
                f"- Period: {start_date} to {end_date}",
                f"- Total tickers: {len(frame)}",
                f"- Success: {len(success_frame)}",
                f"- Failed: {len(failed_frame)}",
                "",
                "## Ranked Candidates",
                "",
                _format_success_table(success_frame),
                "",
                "## Failed Tickers",
                "",
                _format_failed_table(failed_frame),
                "",
                "## Reading Guide",
                "",
                "- `buy_candidate`: Candidate passed the current score and risk filters.",
                "- `watch`: Candidate is worth monitoring, but needs more confirmation.",
                "- `blocked`: Risk filter blocked the signal.",
                "- `avoid` or `hold`: No actionable edge from the current rule set.",
                "",
                "## Risk Note",
                "",
                "This report is a screening aid, not investment advice. "
                "Review market regime, disclosures, liquidity, and news before acting.",
                "",
            ]
        )


def _validate_summary_frame(frame: Any) -> None:
    missing_columns = REQUIRED_UNIVERSE_SUMMARY_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required universe summary columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Universe summary frame is empty.")


def _format_success_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No successful ticker results."

    has_regime = "latest_market_regime" in frame.columns
    has_financial = "latest_financial_score" in frame.columns
    has_event = "latest_event_score" in frame.columns
    if has_regime and has_financial and has_event:
        rows = [
            "| Rank | Ticker | Action | Confidence | Financial | Event | Regime | Report |",
            "| --- | --- | --- | ---: | ---: | ---: | --- | --- |",
        ]
    elif has_regime and has_financial:
        rows = [
            "| Rank | Ticker | Action | Confidence | Financial | Regime | Report |",
            "| --- | --- | --- | ---: | ---: | --- | --- |",
        ]
    elif has_regime:
        rows = [
            "| Rank | Ticker | Action | Confidence | Regime | Report |",
            "| --- | --- | --- | ---: | --- | --- |",
        ]
    else:
        rows = [
            "| Rank | Ticker | Action | Confidence | Report |",
            "| --- | --- | --- | ---: | --- |",
        ]
    for rank, (_, row) in enumerate(frame.iterrows(), start=1):
        if has_regime and has_financial and has_event:
            rows.append(
                "| "
                f"{rank} | "
                f"{row['ticker']} | "
                f"{row['latest_action']} | "
                f"{float(row['latest_confidence_score']):.2f} | "
                f"{float(row['latest_financial_score']):.2f} | "
                f"{float(row['latest_event_score']):.2f} | "
                f"{row['latest_market_regime']} | "
                f"{row['report_path']} |"
            )
        elif has_regime and has_financial:
            rows.append(
                "| "
                f"{rank} | "
                f"{row['ticker']} | "
                f"{row['latest_action']} | "
                f"{float(row['latest_confidence_score']):.2f} | "
                f"{float(row['latest_financial_score']):.2f} | "
                f"{row['latest_market_regime']} | "
                f"{row['report_path']} |"
            )
        elif has_regime:
            rows.append(
                "| "
                f"{rank} | "
                f"{row['ticker']} | "
                f"{row['latest_action']} | "
                f"{float(row['latest_confidence_score']):.2f} | "
                f"{row['latest_market_regime']} | "
                f"{row['report_path']} |"
            )
        else:
            rows.append(
                "| "
                f"{rank} | "
                f"{row['ticker']} | "
                f"{row['latest_action']} | "
                f"{float(row['latest_confidence_score']):.2f} | "
                f"{row['report_path']} |"
            )
    return "\n".join(rows)


def _format_failed_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "No failed tickers."

    rows = ["| Ticker | Error |", "| --- | --- |"]
    for _, row in frame.iterrows():
        rows.append(f"| {row['ticker']} | {row['error']} |")
    return "\n".join(rows)
