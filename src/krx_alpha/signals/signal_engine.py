from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.feature_contract import validate_price_feature_frame
from krx_alpha.contracts.score_contract import validate_daily_score_frame
from krx_alpha.contracts.signal_contract import validate_final_signal_frame
from krx_alpha.risk.risk_filters import RiskFilter

FINAL_SIGNAL_COLUMNS = [
    "date",
    "as_of_date",
    "ticker",
    "source_signal_label",
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

    def generate(self, score_frame: Any, feature_frame: Any) -> Any:
        validate_daily_score_frame(score_frame)
        validate_price_feature_frame(feature_frame)

        scores = score_frame.copy()
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
