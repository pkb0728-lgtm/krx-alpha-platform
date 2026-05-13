from typing import Any

REQUIRED_DISCLOSURE_EVENT_COLUMNS = {
    "date",
    "as_of_date",
    "corp_code",
    "ticker",
    "report_nm",
    "rcept_no",
    "event_category",
    "event_score",
    "event_risk_flag",
    "event_reason",
    "source",
    "feature_created_at",
}


def validate_disclosure_event_frame(frame: Any) -> None:
    missing_columns = REQUIRED_DISCLOSURE_EVENT_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required disclosure event columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Disclosure event frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Disclosure event frame contains null dates.")

    if not frame["corp_code"].astype(str).str.fullmatch(r"\d{8}").all():
        raise ValueError("Disclosure event corp_code must be an eight-digit code.")

    if not frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all():
        raise ValueError("Disclosure event ticker must be a six-digit code.")

    if frame["rcept_no"].isna().any():
        raise ValueError("Disclosure event frame contains null receipt numbers.")

    if frame.duplicated(subset=["rcept_no"]).any():
        raise ValueError("Disclosure event frame contains duplicated receipt numbers.")

    if frame["event_score"].dropna().between(0, 100).all() is False:
        raise ValueError("event_score values must be between 0 and 100.")
