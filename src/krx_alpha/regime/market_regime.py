from dataclasses import dataclass
from typing import Any

import pandas as pd

from krx_alpha.contracts.feature_contract import validate_price_feature_frame
from krx_alpha.contracts.regime_contract import validate_market_regime_frame

MARKET_REGIME_COLUMNS = [
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
]


@dataclass(frozen=True)
class RegimeConfig:
    high_volatility_threshold: float = 0.035
    trend_threshold: float = 0.03
    moving_average_threshold: float = 0.02


@dataclass(frozen=True)
class RegimeClassification:
    regime: str
    regime_score: float
    risk_level: str
    reason: str


class MarketRegimeAnalyzer:
    """Classify market condition from point-in-time technical features."""

    def __init__(self, config: RegimeConfig | None = None) -> None:
        self.config = config or RegimeConfig()

    def analyze(self, feature_frame: Any) -> Any:
        validate_price_feature_frame(feature_frame)

        frame = feature_frame.copy().sort_values(["ticker", "date"]).reset_index(drop=True)
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.date
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)

        groups = frame.groupby("ticker", group_keys=False)
        frame["ma_60"] = groups["close"].transform(lambda series: series.rolling(60).mean())
        frame["return_20d"] = groups["close"].pct_change(20)
        frame["close_to_ma_60"] = frame["close"] / frame["ma_60"] - 1

        classifications = [_classify(row, self.config) for _, row in frame.iterrows()]
        frame["regime"] = [classification.regime for classification in classifications]
        frame["regime_score"] = [classification.regime_score for classification in classifications]
        frame["risk_level"] = [classification.risk_level for classification in classifications]
        frame["regime_reason"] = [classification.reason for classification in classifications]
        frame["generated_at"] = pd.Timestamp.now(tz="UTC")

        regime_frame = frame[MARKET_REGIME_COLUMNS]
        validate_market_regime_frame(regime_frame)
        return regime_frame


def _classify(row: Any, config: RegimeConfig) -> RegimeClassification:
    if _has_insufficient_data(row):
        return RegimeClassification(
            regime="insufficient_data",
            regime_score=50.0,
            risk_level="medium",
            reason="not_enough_history",
        )

    return_20d = float(row["return_20d"])
    volatility_20d = float(row["volatility_20d"])
    close_to_ma_20 = float(row["close_to_ma_20"])
    close_to_ma_60 = float(row["close_to_ma_60"])
    rsi_14 = float(row["rsi_14"]) if pd.notna(row["rsi_14"]) else 50.0

    if volatility_20d >= config.high_volatility_threshold:
        return RegimeClassification(
            regime="high_volatility",
            regime_score=_clip_score(55 - volatility_20d * 600),
            risk_level="high",
            reason="volatility_20d_above_threshold",
        )

    if (
        return_20d > config.trend_threshold
        and close_to_ma_20 > config.moving_average_threshold
        and close_to_ma_60 > config.trend_threshold
    ):
        return RegimeClassification(
            regime="bull",
            regime_score=_clip_score(65 + return_20d * 100 + close_to_ma_60 * 80),
            risk_level="low",
            reason="positive_20d_return_and_price_above_moving_averages",
        )

    if (
        return_20d < -config.trend_threshold
        and close_to_ma_20 < -config.moving_average_threshold
        and close_to_ma_60 < -config.trend_threshold
    ):
        return RegimeClassification(
            regime="bear",
            regime_score=_clip_score(35 + return_20d * 100),
            risk_level="high",
            reason="negative_20d_return_and_price_below_moving_averages",
        )

    if abs(return_20d) <= config.trend_threshold and abs(close_to_ma_20) <= 0.03:
        return RegimeClassification(
            regime="sideways",
            regime_score=55.0,
            risk_level="medium",
            reason="flat_20d_return_and_near_20d_average",
        )

    if close_to_ma_20 > 0 and rsi_14 >= 50:
        return RegimeClassification(
            regime="rebound",
            regime_score=_clip_score(58 + close_to_ma_20 * 100),
            risk_level="medium",
            reason="price_recovered_above_20d_average_with_neutral_rsi",
        )

    return RegimeClassification(
        regime="neutral",
        regime_score=50.0,
        risk_level="medium",
        reason="mixed_or_weak_evidence",
    )


def _has_insufficient_data(row: Any) -> bool:
    required_values = [
        row["ma_20"],
        row["ma_60"],
        row["return_20d"],
        row["volatility_20d"],
        row["close_to_ma_20"],
        row["close_to_ma_60"],
    ]
    return any(pd.isna(value) for value in required_values)


def _clip_score(value: float) -> float:
    return float(min(100, max(0, value)))
