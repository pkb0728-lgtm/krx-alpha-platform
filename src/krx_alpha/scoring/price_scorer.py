from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.disclosure_event_contract import validate_disclosure_event_frame
from krx_alpha.contracts.feature_contract import validate_price_feature_frame
from krx_alpha.contracts.financial_feature_contract import validate_financial_feature_frame
from krx_alpha.contracts.investor_flow_contract import validate_investor_flow_feature_frame
from krx_alpha.contracts.macro_contract import validate_macro_feature_frame
from krx_alpha.contracts.news_contract import validate_news_sentiment_frame
from krx_alpha.contracts.score_contract import validate_daily_score_frame

SCORE_COLUMNS = [
    "date",
    "as_of_date",
    "ticker",
    "technical_score",
    "risk_score",
    "financial_score",
    "event_score",
    "event_risk_flag",
    "flow_score",
    "news_score",
    "macro_score",
    "total_score",
    "signal_label",
    "score_reason",
    "financial_reason",
    "event_reason",
    "flow_reason",
    "news_reason",
    "macro_reason",
    "scored_at",
]


class PriceScorer:
    """Score stocks using simple explainable rules over price features."""

    def score(
        self,
        feature_frame: Any,
        financial_feature_frame: Any | None = None,
        event_feature_frame: Any | None = None,
        flow_feature_frame: Any | None = None,
        news_feature_frame: Any | None = None,
        macro_feature_frame: Any | None = None,
    ) -> Any:
        frame = feature_frame.copy()
        validate_price_feature_frame(frame)
        frame = _attach_financial_scores(frame, financial_feature_frame)
        frame = _attach_event_scores(frame, event_feature_frame)
        frame = _attach_flow_scores(frame, flow_feature_frame)
        frame = _attach_news_scores(frame, news_feature_frame)
        frame = _attach_macro_scores(frame, macro_feature_frame)

        frame["technical_score"] = frame.apply(_technical_score, axis=1)
        frame["risk_score"] = frame.apply(_risk_score, axis=1)
        frame["total_score"] = (
            frame["technical_score"] * 0.30
            + frame["risk_score"] * 0.20
            + frame["financial_score"] * 0.15
            + frame["event_score"] * 0.10
            + frame["flow_score"] * 0.10
            + frame["news_score"] * 0.10
            + frame["macro_score"] * 0.05
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
    financials["fs_priority"] = financials["fs_div"].astype(str).map({"OFS": 0, "CFS": 1}).fillna(0)

    latest_financials = (
        financials.sort_values(["ticker", "bsns_year", "reprt_code", "fs_priority"])
        .groupby("ticker", as_index=False)
        .tail(1)
    )
    financial_columns = ["ticker", "financial_score", "financial_reason"]
    merged = frame.merge(latest_financials[financial_columns], on="ticker", how="left")
    merged["financial_score"] = merged["financial_score"].fillna(50.0)
    merged["financial_reason"] = merged["financial_reason"].fillna("no_financial_feature_available")
    return merged


def _attach_event_scores(frame: Any, event_feature_frame: Any | None) -> Any:
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    if event_feature_frame is None:
        frame["event_score"] = 50.0
        frame["event_risk_flag"] = False
        frame["event_reason"] = "no_disclosure_event_available"
        return frame

    validate_disclosure_event_frame(event_feature_frame)
    events = event_feature_frame.copy()
    events["ticker"] = events["ticker"].astype(str).str.zfill(6)
    events["date"] = pd.to_datetime(events["date"]).dt.date

    event_summary = (
        events.groupby(["ticker", "date"], as_index=False)
        .agg(
            event_score=("event_score", "min"),
            event_risk_flag=("event_risk_flag", "max"),
            event_reason=("event_reason", _join_reasons),
        )
        .reset_index(drop=True)
    )
    merged = frame.merge(event_summary, on=["ticker", "date"], how="left")
    merged["event_score"] = merged["event_score"].fillna(50.0)
    merged["event_risk_flag"] = (
        merged["event_risk_flag"].where(merged["event_risk_flag"].notna(), False).astype(bool)
    )
    merged["event_reason"] = merged["event_reason"].fillna("no_disclosure_event_available")
    return merged


def _join_reasons(values: pd.Series) -> str:
    reasons = sorted({str(value) for value in values if str(value)})
    return ", ".join(reasons) if reasons else "disclosure_event_neutral"


def _attach_flow_scores(frame: Any, flow_feature_frame: Any | None) -> Any:
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    if flow_feature_frame is None:
        frame["flow_score"] = 50.0
        frame["flow_reason"] = "no_investor_flow_available"
        return frame

    validate_investor_flow_feature_frame(flow_feature_frame)
    flows = flow_feature_frame.copy()
    flows["ticker"] = flows["ticker"].astype(str).str.zfill(6)
    flows["date"] = pd.to_datetime(flows["date"]).dt.date
    flow_columns = ["date", "ticker", "flow_score", "flow_reason"]
    merged = frame.merge(flows[flow_columns], on=["date", "ticker"], how="left")
    merged["flow_score"] = merged["flow_score"].fillna(50.0)
    merged["flow_reason"] = merged["flow_reason"].fillna("no_investor_flow_available")
    return merged


def _attach_news_scores(frame: Any, news_feature_frame: Any | None) -> Any:
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    if news_feature_frame is None:
        frame["news_score"] = 50.0
        frame["news_reason"] = "no_news_sentiment_available"
        return frame

    validate_news_sentiment_frame(news_feature_frame)
    news = news_feature_frame.copy()
    news["ticker"] = news["ticker"].astype(str).str.zfill(6)
    news["date"] = pd.to_datetime(news["date"]).dt.date
    news_columns = ["date", "ticker", "news_score", "news_reason"]
    merged = frame.merge(news[news_columns], on=["date", "ticker"], how="left")
    merged["news_score"] = merged["news_score"].fillna(50.0)
    merged["news_reason"] = merged["news_reason"].fillna("no_news_sentiment_available")
    return merged


def _attach_macro_scores(frame: Any, macro_feature_frame: Any | None) -> Any:
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    if macro_feature_frame is None:
        frame["macro_score"] = 50.0
        frame["macro_reason"] = "no_macro_feature_available"
        return frame

    validate_macro_feature_frame(macro_feature_frame)
    macro = macro_feature_frame.copy()
    macro["date"] = pd.to_datetime(macro["date"]).dt.date
    macro_columns = ["date", "macro_score", "macro_reason"]
    merged = frame.merge(macro[macro_columns], on="date", how="left")
    merged["macro_score"] = merged["macro_score"].fillna(50.0)
    merged["macro_reason"] = merged["macro_reason"].fillna("no_macro_feature_available")
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
