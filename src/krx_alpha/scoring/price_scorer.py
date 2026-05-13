from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.feature_contract import validate_price_feature_frame
from krx_alpha.contracts.financial_feature_contract import validate_financial_feature_frame
from krx_alpha.contracts.score_contract import validate_daily_score_frame

SCORE_COLUMNS = [
    "date",
    "as_of_date",
    "ticker",
    "technical_score",
    "risk_score",
    "financial_score",
    "total_score",
    "signal_label",
    "score_reason",
    "financial_reason",
    "scored_at",
]


class PriceScorer:
    """Score stocks using simple explainable rules over price features."""

    def score(self, feature_frame: Any, financial_feature_frame: Any | None = None) -> Any:
        frame = feature_frame.copy()
        validate_price_feature_frame(frame)
        frame = _attach_financial_scores(frame, financial_feature_frame)

        frame["technical_score"] = frame.apply(_technical_score, axis=1)
        frame["risk_score"] = frame.apply(_risk_score, axis=1)
        frame["total_score"] = (
            frame["technical_score"] * 0.55
            + frame["risk_score"] * 0.25
            + frame["financial_score"] * 0.20
        ).clip(0, 100)
        frame["signal_label"] = frame["total_score"].apply(_signal_label)
        frame["score_reason"] = frame.apply(_score_reason, axis=1)
        frame["scored_at"] = pd.Timestamp.now(tz="UTC")

        score_frame = frame[SCORE_COLUMNS]
        validate_daily_score_frame(score_frame)
        return score_frame


def _attach_financial_scores(frame: Any, financial_feature_frame: Any | None) -> Any:
    frame = frame.copy()
    if financial_feature_frame is None:
        frame["financial_score"] = 50.0
        frame["financial_reason"] = "no_financial_feature_available"
        return frame

    validate_financial_feature_frame(financial_feature_frame)
    financials = financial_feature_frame.copy()
    financials["ticker"] = financials["ticker"].astype(str).str.zfill(6)
    financials["bsns_year"] = financials["bsns_year"].astype(str)
    financials["reprt_code"] = financials["reprt_code"].astype(str)

    latest_financials = (
        financials.sort_values(["ticker", "bsns_year", "reprt_code"])
        .groupby("ticker", as_index=False)
        .tail(1)
    )
    financial_columns = ["ticker", "financial_score", "financial_reason"]
    merged = frame.merge(latest_financials[financial_columns], on="ticker", how="left")
    merged["financial_score"] = merged["financial_score"].fillna(50.0)
    merged["financial_reason"] = merged["financial_reason"].fillna("no_financial_feature_available")
    return merged


def _technical_score(row: pd.Series) -> float:
    score = 50.0

    score += _score_close_to_ma(row.get("close_to_ma_5"))
    score += _score_close_to_ma(row.get("close_to_ma_20"))
    score += _score_rsi(row.get("rsi_14"))
    score += _score_change(row.get("trading_value_change_5d"), positive_weight=10.0)

    return float(np.clip(score, 0, 100))


def _risk_score(row: pd.Series) -> float:
    score = 80.0

    volatility_5d = row.get("volatility_5d")
    volatility_20d = row.get("volatility_20d")
    range_pct = row.get("range_pct")

    if pd.notna(volatility_5d):
        score -= min(float(volatility_5d) * 400, 25)
    if pd.notna(volatility_20d):
        score -= min(float(volatility_20d) * 300, 20)
    if pd.notna(range_pct):
        score -= min(float(range_pct) * 200, 20)

    return float(np.clip(score, 0, 100))


def _score_close_to_ma(value: Any) -> float:
    if pd.isna(value):
        return 0.0

    value = float(value)
    if value > 0.03:
        return 10.0
    if value > 0:
        return 6.0
    if value > -0.03:
        return -2.0
    return -8.0


def _score_rsi(value: Any) -> float:
    if pd.isna(value):
        return 0.0

    value = float(value)
    if 45 <= value <= 60:
        return 10.0
    if 35 <= value < 45:
        return 6.0
    if 60 < value <= 70:
        return 3.0
    if value > 75:
        return -10.0
    if value < 30:
        return -8.0
    return 0.0


def _score_change(value: Any, positive_weight: float) -> float:
    if pd.isna(value):
        return 0.0

    value = float(value)
    if value > 0.3:
        return positive_weight
    if value > 0.1:
        return positive_weight * 0.6
    if value < -0.3:
        return -positive_weight * 0.5
    return 0.0


def _signal_label(total_score: float) -> str:
    if total_score >= 70:
        return "watch_buy"
    if total_score >= 55:
        return "watch"
    if total_score >= 40:
        return "neutral"
    return "avoid"


def _score_reason(row: pd.Series) -> str:
    reasons: list[str] = []

    if pd.notna(row.get("close_to_ma_5")) and float(row["close_to_ma_5"]) > 0:
        reasons.append("close_above_ma5")
    if pd.notna(row.get("close_to_ma_20")) and float(row["close_to_ma_20"]) > 0:
        reasons.append("close_above_ma20")
    if pd.notna(row.get("rsi_14")) and 35 <= float(row["rsi_14"]) <= 60:
        reasons.append("rsi_recovery_zone")
    if pd.notna(row.get("trading_value_change_5d")) and float(row["trading_value_change_5d"]) > 0.1:
        reasons.append("trading_value_increase")
    if pd.notna(row.get("volatility_5d")) and float(row["volatility_5d"]) > 0.04:
        reasons.append("high_short_term_volatility")

    return ", ".join(reasons) if reasons else "insufficient_or_neutral_evidence"
