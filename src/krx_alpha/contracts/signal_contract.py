from typing import Any

REQUIRED_FINAL_SIGNAL_COLUMNS = {
    "date",
    "as_of_date",
    "ticker",
    "source_signal_label",
    "final_action",
    "confidence_score",
    "risk_blocked",
    "risk_flags",
    "suggested_position_pct",
    "signal_reason",
    "generated_at",
}


def validate_final_signal_frame(frame: Any) -> None:
    missing_columns = REQUIRED_FINAL_SIGNAL_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required final signal columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Final signal frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Final signal frame contains null dates.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Final signal frame contains duplicated date/ticker rows.")

    if frame["confidence_score"].dropna().between(0, 100).all() is False:
        raise ValueError("confidence_score values must be between 0 and 100.")

    if frame["suggested_position_pct"].dropna().between(0, 100).all() is False:
        raise ValueError("suggested_position_pct values must be between 0 and 100.")
