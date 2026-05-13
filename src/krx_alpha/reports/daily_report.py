from typing import Any

import pandas as pd

from krx_alpha.contracts.feature_contract import validate_price_feature_frame
from krx_alpha.contracts.score_contract import validate_daily_score_frame

REASON_DESCRIPTIONS = {
    "close_above_ma5": "Close is above the 5-day moving average.",
    "close_above_ma20": "Close is above the 20-day moving average.",
    "rsi_recovery_zone": "RSI is in a recovery or neutral-positive zone.",
    "trading_value_increase": "Trading value increased versus the recent baseline.",
    "high_short_term_volatility": "Short-term volatility is elevated.",
    "insufficient_or_neutral_evidence": "Evidence is neutral or insufficient.",
    "no_financial_feature_available": "No financial feature file was attached to this score.",
    "revenue_growth_positive": "Revenue growth is positive versus the previous period.",
    "revenue_growth_negative": "Revenue growth is weak versus the previous period.",
    "operating_margin_healthy": "Operating margin is healthy.",
    "operating_loss": "Operating income is negative.",
    "net_margin_positive": "Net margin is positive.",
    "debt_ratio_conservative": "Debt ratio is conservative.",
    "debt_ratio_high": "Debt ratio is elevated.",
    "roe_positive": "Return on equity is positive.",
    "financial_evidence_neutral": "Financial evidence is neutral.",
}

SIGNAL_DESCRIPTIONS = {
    "watch_buy": "Strong watchlist candidate. Human review is still required.",
    "watch": "Watchlist candidate. Wait for additional confirmation.",
    "neutral": "No clear edge from the current rule set.",
    "avoid": "Avoid or wait until risk and evidence improve.",
}


class DailyReportGenerator:
    """Generate a human-readable Markdown report from score and feature data."""

    def generate(self, score_frame: Any, feature_frame: Any) -> str:
        validate_daily_score_frame(score_frame)
        validate_price_feature_frame(feature_frame)

        scores = score_frame.copy().sort_values(["ticker", "date"])
        features = feature_frame.copy().sort_values(["ticker", "date"])
        latest_score = scores.iloc[-1]
        latest_feature = features[
            (features["ticker"] == latest_score["ticker"])
            & (features["date"] == latest_score["date"])
        ].iloc[-1]

        reasons = _format_reasons(str(latest_score["score_reason"]))
        financial_reasons = _format_reasons(str(latest_score["financial_reason"]))
        signal = str(latest_score["signal_label"])
        signal_description = SIGNAL_DESCRIPTIONS.get(signal, "Unknown signal.")

        return "\n".join(
            [
                f"# Daily Stock Report: {latest_score['ticker']}",
                "",
                f"- Report date: {_format_date(latest_score['date'])}",
                f"- Signal: `{signal}`",
                f"- Signal meaning: {signal_description}",
                f"- Total score: {_format_number(latest_score['total_score'])}",
                f"- Technical score: {_format_number(latest_score['technical_score'])}",
                f"- Risk score: {_format_number(latest_score['risk_score'])}",
                f"- Financial score: {_format_number(latest_score['financial_score'])}",
                "",
                "## Key Metrics",
                "",
                f"- Close: {_format_number(latest_feature['close'])}",
                f"- 5-day moving average: {_format_optional(latest_feature['ma_5'])}",
                f"- 20-day moving average: {_format_optional(latest_feature['ma_20'])}",
                f"- RSI 14: {_format_optional(latest_feature['rsi_14'])}",
                f"- 5-day volatility: {_format_percent(latest_feature['volatility_5d'])}",
                f"- Trading value change 5d: "
                f"{_format_percent(latest_feature['trading_value_change_5d'])}",
                "",
                "## Explanation",
                "",
                reasons,
                "",
                "## Financial Evidence",
                "",
                financial_reasons,
                "",
                "## Risk Note",
                "",
                "This report is decision-support output, not investment advice. "
                "Check liquidity, news, disclosures, and market regime before acting.",
                "",
            ]
        )


def _format_reasons(reason_text: str) -> str:
    reason_codes = [reason.strip() for reason in reason_text.split(",") if reason.strip()]
    if not reason_codes:
        reason_codes = ["insufficient_or_neutral_evidence"]

    lines = []
    for reason in reason_codes:
        description = REASON_DESCRIPTIONS.get(reason, reason)
        lines.append(f"- `{reason}`: {description}")
    return "\n".join(lines)


def _format_date(value: Any) -> str:
    return str(pd.Timestamp(value).strftime("%Y-%m-%d"))


def _format_number(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.2f}"


def _format_optional(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return _format_number(value)


def _format_percent(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value) * 100:.2f}%"
