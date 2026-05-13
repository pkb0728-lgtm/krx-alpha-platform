from typing import Any

REQUIRED_PRICE_COLUMNS = {
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trading_value",
    "trading_value_is_estimated",
    "source",
    "collected_at",
}

REQUIRED_PROCESSED_PRICE_COLUMNS = REQUIRED_PRICE_COLUMNS | {
    "return_1d",
    "log_return_1d",
    "range_pct",
    "as_of_date",
    "processed_at",
}


def validate_price_frame(frame: Any) -> None:
    missing_columns = REQUIRED_PRICE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required price columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Price frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Price frame contains null dates.")

    if frame[["ticker", "open", "high", "low", "close", "volume"]].isna().any().any():
        raise ValueError("Price frame contains null values in required market columns.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Price frame contains duplicated date/ticker rows.")

    if (frame[["open", "high", "low", "close", "volume", "trading_value"]] < 0).any().any():
        raise ValueError("Price frame contains negative market values.")

    if (frame["high"] < frame["low"]).any():
        raise ValueError("Price frame contains rows where high is lower than low.")


def validate_processed_price_frame(frame: Any) -> None:
    validate_price_frame(frame)

    missing_columns = REQUIRED_PROCESSED_PRICE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required processed price columns: {sorted(missing_columns)}")

    if frame["as_of_date"].isna().any():
        raise ValueError("Processed price frame contains null as-of dates.")

    if frame["processed_at"].isna().any():
        raise ValueError("Processed price frame contains null processed timestamps.")

    if (frame["range_pct"] < 0).any():
        raise ValueError("Processed price frame contains negative daily ranges.")
