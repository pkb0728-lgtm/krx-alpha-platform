from typing import Any

import pandas as pd

from krx_alpha.contracts.feature_contract import validate_price_feature_frame

PRICE_FEATURE_COLUMNS = [
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
]


class PriceFeatureBuilder:
    """Build reusable price-based features from processed daily prices."""

    def build(self, processed_frame: Any) -> Any:
        frame = processed_frame.copy()
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.date
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)
        frame = frame.sort_values(["ticker", "date"]).reset_index(drop=True)

        groups = frame.groupby("ticker", group_keys=False)

        frame["ma_5"] = groups["close"].transform(lambda series: series.rolling(5).mean())
        frame["ma_20"] = groups["close"].transform(lambda series: series.rolling(20).mean())
        frame["close_to_ma_5"] = frame["close"] / frame["ma_5"] - 1
        frame["close_to_ma_20"] = frame["close"] / frame["ma_20"] - 1

        frame["volume_change_5d"] = groups["volume"].pct_change(5)
        frame["trading_value_change_5d"] = groups["trading_value"].pct_change(5)
        frame["volatility_5d"] = groups["return_1d"].transform(
            lambda series: series.rolling(5).std()
        )
        frame["volatility_20d"] = groups["return_1d"].transform(
            lambda series: series.rolling(20).std()
        )
        frame["rsi_14"] = groups["close"].transform(_rsi_14)
        frame["feature_created_at"] = pd.Timestamp.now(tz="UTC")

        feature_frame = frame[PRICE_FEATURE_COLUMNS]
        validate_price_feature_frame(feature_frame)
        return feature_frame


def _rsi_14(close: pd.Series) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    average_gain = gain.rolling(14).mean()
    average_loss = loss.rolling(14).mean()
    relative_strength = average_gain / average_loss
    rsi = 100 - (100 / (1 + relative_strength))

    return rsi.clip(lower=0, upper=100)
