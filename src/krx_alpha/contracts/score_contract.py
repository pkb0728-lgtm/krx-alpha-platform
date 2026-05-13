from typing import Any

REQUIRED_DAILY_SCORE_COLUMNS = {
    "date",
    "as_of_date",
    "ticker",
    "technical_score",
    "risk_score",
    "financial_score",
    "event_score",
    "event_risk_flag",
    "flow_score",
    "total_score",
    "signal_label",
    "score_reason",
    "financial_reason",
    "event_reason",
    "flow_reason",
    "scored_at",
}


def validate_daily_score_frame(frame: Any) -> None:
    missing_columns = REQUIRED_DAILY_SCORE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required daily score columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Daily score frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Daily score frame contains null dates.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Daily score frame contains duplicated date/ticker rows.")

    score_columns = [
        "technical_score",
        "risk_score",
        "financial_score",
        "event_score",
        "flow_score",
        "total_score",
    ]
    for column in score_columns:
        if frame[column].dropna().between(0, 100).all() is False:
            raise ValueError(f"{column} values must be between 0 and 100.")
