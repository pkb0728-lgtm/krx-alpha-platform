from typing import Any

REQUIRED_PRICE_FEATURE_COLUMNS = {
    "date",
    "as_of_date",
    "ticker",
    "close",
    "volume",
    "trading_value",
    "return_1d",
    "ma_5",
    "ma_20",
    "close_to_ma_5",
    "close_to_ma_20",
    "volume_change_5d",
    "trading_value_change_5d",
    "range_pct",
    "volatility_5d",
    "volatility_20d",
    "rsi_14",
    "feature_created_at",
}


def validate_price_feature_frame(frame: Any) -> None:
    missing_columns = REQUIRED_PRICE_FEATURE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required price feature columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Price feature frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Price feature frame contains null dates.")

    if frame["as_of_date"].isna().any():
        raise ValueError("Price feature frame contains null as-of dates.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Price feature frame contains duplicated date/ticker rows.")

    if (frame[["close", "volume", "trading_value"]] < 0).any().any():
        raise ValueError("Price feature frame contains negative base market values.")

    if frame["rsi_14"].dropna().between(0, 100).all() is False:
        raise ValueError("RSI values must be between 0 and 100.")
