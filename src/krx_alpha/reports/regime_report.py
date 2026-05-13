from typing import Any

import pandas as pd

from krx_alpha.contracts.regime_contract import validate_market_regime_frame

REGIME_DESCRIPTIONS = {
    "bull": "Positive trend with price above key moving averages.",
    "bear": "Negative trend with price below key moving averages.",
    "sideways": "Range-bound condition with weak directional evidence.",
    "high_volatility": "Volatility is elevated; position risk should be reduced.",
    "rebound": "Price has recovered above the 20-day average, but confirmation is needed.",
    "neutral": "Mixed evidence with no clear market regime.",
    "insufficient_data": "Not enough history to classify the regime reliably.",
}


class MarketRegimeReportGenerator:
    """Generate a Markdown report for market regime analysis."""

    def generate(self, regime_frame: Any) -> str:
        validate_market_regime_frame(regime_frame)
        frame = regime_frame.copy().sort_values(["ticker", "date"])
        latest = frame.iloc[-1]
        regime = str(latest["regime"])

        return "\n".join(
            [
                f"# Market Regime Report: {latest['ticker']}",
                "",
                f"- Report date: {_format_date(latest['date'])}",
                f"- Regime: `{regime}`",
                f"- Meaning: {REGIME_DESCRIPTIONS.get(regime, 'Unknown regime.')}",
                f"- Regime score: {_format_number(latest['regime_score'])}",
                f"- Risk level: `{latest['risk_level']}`",
                f"- Reason: `{latest['regime_reason']}`",
                "",
                "## Key Metrics",
                "",
                f"- Close: {_format_number(latest['close'])}",
                f"- 20-day return: {_format_percent(latest['return_20d'])}",
                f"- 20-day volatility: {_format_percent(latest['volatility_20d'])}",
                f"- Close vs 20-day MA: {_format_percent(latest['close_to_ma_20'])}",
                f"- Close vs 60-day MA: {_format_percent(latest['close_to_ma_60'])}",
                f"- RSI 14: {_format_optional_number(latest['rsi_14'])}",
                "",
                "## Usage Note",
                "",
                "Use this regime as a market-context filter before reviewing stock signals. "
                "It is not an investment instruction.",
                "",
            ]
        )


def _format_date(value: Any) -> str:
    return str(pd.Timestamp(value).strftime("%Y-%m-%d"))


def _format_number(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.2f}"


def _format_optional_number(value: Any) -> str:
    return _format_number(value)


def _format_percent(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value) * 100:.2f}%"
