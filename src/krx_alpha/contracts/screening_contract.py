from typing import Any

REQUIRED_SCREENING_COLUMNS = {
    "screen_date",
    "ticker",
    "passed",
    "screen_score",
    "final_action",
    "confidence_score",
    "market_regime",
    "risk_blocked",
    "suggested_position_pct",
    "trading_value",
    "trading_value_change_5d",
    "rsi_14",
    "volatility_5d",
    "reasons",
    "evidence_summary",
    "caution_summary",
    "review_checklist",
    "signal_path",
    "screened_at",
}


def validate_screening_frame(frame: Any) -> None:
    missing_columns = REQUIRED_SCREENING_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required screening columns: {sorted(missing_columns)}")

    if frame.empty:
        return

    if frame["ticker"].isna().any():
        raise ValueError("Screening frame contains null tickers.")

    if frame["screen_score"].dropna().between(0, 100).all() is False:
        raise ValueError("screen_score values must be between 0 and 100.")

    if frame["confidence_score"].dropna().between(0, 100).all() is False:
        raise ValueError("confidence_score values must be between 0 and 100.")

    if frame["suggested_position_pct"].dropna().between(0, 100).all() is False:
        raise ValueError("suggested_position_pct values must be between 0 and 100.")
