from typing import Any

REQUIRED_UNIVERSE_COLUMNS = {
    "ticker",
    "name",
    "market",
    "sector",
    "reason",
    "is_active",
}


def validate_universe_frame(frame: Any) -> None:
    missing_columns = REQUIRED_UNIVERSE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required universe columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Universe frame is empty.")

    tickers = frame["ticker"].astype(str)
    if not tickers.str.fullmatch(r"\d{6}").all():
        raise ValueError("Universe tickers must be six-digit Korean stock codes.")

    if tickers.duplicated().any():
        raise ValueError("Universe contains duplicate tickers.")
