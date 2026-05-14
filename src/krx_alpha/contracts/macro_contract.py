from typing import Any

REQUIRED_MACRO_COLUMNS = {
    "date",
    "series_id",
    "series_name",
    "value",
    "source",
    "collected_at",
}

REQUIRED_MACRO_FEATURE_COLUMNS = {
    "date",
    "as_of_date",
    "us_10y_yield",
    "fed_funds_rate",
    "usdkrw",
    "us_10y_yield_change_5d",
    "usdkrw_change_5d",
    "usdkrw_change_pct_5d",
    "macro_score",
    "macro_reason",
    "source",
    "feature_created_at",
}


def validate_macro_frame(frame: Any) -> None:
    missing_columns = REQUIRED_MACRO_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required macro columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Macro frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Macro frame contains null dates.")

    if frame["series_id"].astype(str).str.len().eq(0).any():
        raise ValueError("Macro frame contains empty series ids.")

    if frame.duplicated(subset=["date", "series_id"]).any():
        raise ValueError("Macro frame contains duplicated date/series rows.")


def validate_macro_feature_frame(frame: Any) -> None:
    missing_columns = REQUIRED_MACRO_FEATURE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required macro feature columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Macro feature frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Macro feature frame contains null dates.")

    if frame.duplicated(subset=["date"]).any():
        raise ValueError("Macro feature frame contains duplicated dates.")

    if frame["macro_score"].dropna().between(0, 100).all() is False:
        raise ValueError("macro_score values must be between 0 and 100.")
