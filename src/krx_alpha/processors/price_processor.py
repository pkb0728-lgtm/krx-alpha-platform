from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.price_contract import validate_price_frame, validate_processed_price_frame

PROCESSED_PRICE_COLUMNS = [
    "date",
    "as_of_date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trading_value",
    "trading_value_is_estimated",
    "return_1d",
    "log_return_1d",
    "range_pct",
    "change_rate",
    "source",
    "collected_at",
    "processed_at",
]


class PriceProcessor:
    """Transform raw daily price data into a clean processed dataset."""

    def process(self, raw_frame: Any) -> Any:
        frame = raw_frame.copy()
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)

        numeric_columns = ["open", "high", "low", "close", "volume", "trading_value"]
        for column in numeric_columns:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

        if "change_rate" in frame.columns:
            frame["change_rate"] = pd.to_numeric(frame["change_rate"], errors="coerce")
        else:
            frame["change_rate"] = pd.NA

        frame["trading_value_is_estimated"] = (
            frame["trading_value_is_estimated"].fillna(False).astype(bool)
        )
        frame = frame.drop_duplicates(subset=["date", "ticker"], keep="last")
        frame = frame.sort_values(["ticker", "date"]).reset_index(drop=True)

        validate_price_frame(frame)

        grouped_close = frame.groupby("ticker")["close"]
        previous_close = grouped_close.shift(1)

        frame["return_1d"] = grouped_close.pct_change()
        frame["log_return_1d"] = np.log(frame["close"] / previous_close)
        frame["range_pct"] = (frame["high"] - frame["low"]) / frame["close"]
        frame["as_of_date"] = frame["date"]
        frame["processed_at"] = pd.Timestamp.now(tz="UTC")

        processed_frame = frame[PROCESSED_PRICE_COLUMNS]
        validate_processed_price_frame(processed_frame)
        return processed_frame
