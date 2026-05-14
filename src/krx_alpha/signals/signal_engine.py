from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.feature_contract import validate_price_feature_frame
from krx_alpha.contracts.regime_contract import validate_market_regime_frame
from krx_alpha.contracts.score_contract import validate_daily_score_frame
from krx_alpha.contracts.signal_contract import validate_final_signal_frame
from krx_alpha.risk.risk_filters import RiskFilter

FINAL_SIGNAL_COLUMNS = [
    "date",
    "as_of_date",
    "ticker",
    "source_signal_label",
    "financial_score",
    "financial_reason",
    "event_score",
    "event_risk_flag",
    "event_reason",
    "flow_score",
    "flow_reason",
    "news_score",
    "news_reason",
    "macro_score",
    "macro_reason",
    "market_regime",
    "market_regime_score",
    "market_regime_risk_level",
    "final_action",
    "confidence_score",
    "risk_blocked",
    "risk_flags",
    "suggested_position_pct",
    "signal_reason",
    "generated_at",
]


class SignalEngine:
    """Convert scores into final human-in-the-loop decision-support signals."""

    def __init__(self, risk_filter: RiskFilter | None = None) -> None:
        self.risk_filter = risk_filter or RiskFilter()

    def generate(
        self,
        score_frame: Any,
        feature_frame: Any,
        regime_frame: Any | None = None,
    ) -> Any:
        validate_daily_score_frame(score_frame)
        validate_price_feature_frame(feature_frame)

        scores = score_frame.copy()
        scores = _ensure_optional_score_columns(scores)
        features = feature_frame.copy()
        scores["date"] = pd.to_datetime(scores["date"]).dt.date
        features["date"] = pd.to_datetime(features["date"]).dt.date

        feature_columns = [
            "date",
            "ticker",
            "close",
            "trading_value",
            "ma_20",
            "rsi_14",
            "range_pct",
            "volatility_5d",
        ]
        frame = scores.merge(features[feature_columns], on=["date", "ticker"], how="left")
        frame = _merge_regime(frame, regime_frame)

        frame["risk_flags"] = frame.apply(lambda row: self.risk_filter.evaluate(row), axis=1)
        frame["risk_blocked"] = frame["risk_flags"].apply(bool)
        frame["source_signal_label"] = frame["signal_label"]
        frame["final_action"] = frame.apply(_final_action, axis=1)
        frame["confidence_score"] = frame.apply(_confidence_score, axis=1)
        frame["suggested_position_pct"] = frame.apply(_suggested_position_pct, axis=1)
        frame["signal_reason"] = frame.apply(_signal_reason, axis=1)
        frame["risk_flags"] = frame["risk_flags"].apply(lambda flags: ", ".join(flags))
        frame["generated_at"] = pd.Timestamp.now(tz="UTC")

        signal_frame = frame[FINAL_SIGNAL_COLUMNS]
        validate_final_signal_frame(signal_frame)
        return signal_frame


def _merge_regime(frame: Any, regime_frame: Any | None) -> Any:
    if regime_frame is None:
        frame["market_regime"] = "unknown"
        frame["market_regime_score"] = 50.0
        frame["market_regime_risk_level"] = "medium"
        return frame

    validate_market_regime_frame(regime_frame)
    regimes = regime_frame.copy()
    regimes["date"] = pd.to_datetime(regimes["date"]).dt.date
    regimes["ticker"] = regimes["ticker"].astype(str).str.zfill(6)
    regime_columns = [
        "date",
        "ticker",
        "regime",
        "regime_score",
        "risk_level",
    ]
    merged = frame.merge(regimes[regime_columns], on=["date", "ticker"], how="left")
    merged["market_regime"] = merged["regime"].fillna("unknown")
    merged["market_regime_score"] = merged["regime_score"].fillna(50.0)
    merged["market_regime_risk_level"] = merged["risk_level"].fillna("medium")
    return merged


def _ensure_optional_score_columns(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    if "news_score" not in frame.columns:
        frame["news_score"] = 50.0
    if "news_reason" not in frame.columns:
        frame["news_reason"] = "no_news_sentiment_available"
    if "macro_score" not in frame.columns:
        frame["macro_score"] = 50.0
    if "macro_reason" not in frame.columns:
        frame["macro_reason"] = "no_macro_feature_available"
    return frame


def _final_action(row: pd.Series) -> str:
    if bool(row["risk_blocked"]):
        return "blocked"

    source_label = str(row["source_signal_label"])
    if source_label == "watch_buy":
        return "buy_candidate"
    if source_label == "watch":
        return "watch"
    if source_label == "avoid":
        return "avoid"
    return "hold"


def _confidence_score(row: pd.Series) -> float:
    penalty = 15.0 if bool(row["risk_blocked"]) else 0.0
    confidence = float(row["total_score"]) * 0.8 + float(row["risk_score"]) * 0.2 - penalty
    return float(np.clip(confidence, 0, 100))


def _suggested_position_pct(row: pd.Series) -> float:
    if bool(row["risk_blocked"]):
        return 0.0

    action = str(row["final_action"])
    risk_multiplier = float(row["risk_score"]) / 100
    if action == "buy_candidate":
        return round(5.0 * risk_multiplier, 2)
    if action == "watch":
        return round(2.0 * risk_multiplier, 2)
    return 0.0


def _signal_reason(row: pd.Series) -> str:
    if bool(row["risk_blocked"]):
        return f"Blocked by risk filter: {', '.join(row['risk_flags'])}"

    return str(row["score_reason"])
