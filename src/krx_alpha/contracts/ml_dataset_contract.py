from typing import Any

REQUIRED_ML_TRAINING_COLUMNS = {
    "date",
    "as_of_date",
    "ticker",
    "close",
    "future_close",
    "label_end_date",
    "holding_days",
    "forward_return",
    "target_positive_forward_return",
    "label_created_at",
}


def validate_ml_training_frame(frame: Any) -> None:
    missing_columns = REQUIRED_ML_TRAINING_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required ML training columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("ML training frame is empty.")

    if frame[["date", "as_of_date", "label_end_date"]].isna().any().any():
        raise ValueError("ML training frame contains null dates.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("ML training frame contains duplicated date/ticker rows.")

    if (frame[["close", "future_close"]] <= 0).any().any():
        raise ValueError("ML training frame contains non-positive close values.")

    if (frame["holding_days"] <= 0).any():
        raise ValueError("holding_days must be positive.")

    label_values = set(frame["target_positive_forward_return"].dropna().astype(int).unique())
    if not label_values.issubset({0, 1}):
        raise ValueError("target_positive_forward_return must be binary.")

    if (frame["label_end_date"] <= frame["as_of_date"]).any():
        raise ValueError("ML labels must end after each feature as_of_date.")
