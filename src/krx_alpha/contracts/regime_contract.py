from typing import Any

REQUIRED_MARKET_REGIME_COLUMNS = {
    "date",
    "as_of_date",
    "ticker",
    "close",
    "ma_20",
    "ma_60",
    "return_20d",
    "volatility_20d",
    "close_to_ma_20",
    "close_to_ma_60",
    "rsi_14",
    "regime",
    "regime_score",
    "risk_level",
    "regime_reason",
    "generated_at",
}

ALLOWED_REGIMES = {
    "bull",
    "bear",
    "sideways",
    "high_volatility",
    "rebound",
    "neutral",
    "insufficient_data",
}

ALLOWED_RISK_LEVELS = {"low", "medium", "high"}


def validate_market_regime_frame(frame: Any) -> None:
    missing_columns = REQUIRED_MARKET_REGIME_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required market regime columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Market regime frame is empty.")

    if frame[["date", "as_of_date", "ticker", "regime", "risk_level"]].isna().any().any():
        raise ValueError("Market regime frame contains null required values.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Market regime frame contains duplicated date/ticker rows.")

    if frame["regime"].isin(ALLOWED_REGIMES).all() is False:
        raise ValueError("Market regime frame contains an unknown regime.")

    if frame["risk_level"].isin(ALLOWED_RISK_LEVELS).all() is False:
        raise ValueError("Market regime frame contains an unknown risk level.")

    if frame["regime_score"].between(0, 100).all() is False:
        raise ValueError("Market regime scores must be between 0 and 100.")
