from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.investor_flow_contract import (
    validate_investor_flow_feature_frame,
    validate_investor_flow_frame,
)

INVESTOR_FLOW_FEATURE_COLUMNS = [
    "date",
    "as_of_date",
    "ticker",
    "individual_net_buy_value",
    "foreign_net_buy_value",
    "institution_net_buy_value",
    "smart_money_net_buy_value",
    "foreign_net_buy_value_5d",
    "institution_net_buy_value_5d",
    "smart_money_net_buy_value_5d",
    "flow_score",
    "flow_reason",
    "feature_created_at",
]


class InvestorFlowFeatureBuilder:
    """Build investor flow features from investor net-buy data."""

    def build(self, flow_frame: Any) -> Any:
        validate_investor_flow_frame(flow_frame)
        frame = flow_frame.copy().sort_values(["ticker", "date"])
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["as_of_date"] = frame["date"]
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)
        frame["smart_money_net_buy_value"] = (
            frame["foreign_net_buy_value"] + frame["institution_net_buy_value"]
        )

        group = frame.groupby("ticker", group_keys=False)
        frame["foreign_net_buy_value_5d"] = (
            group["foreign_net_buy_value"]
            .rolling(5, min_periods=1)
            .sum()
            .reset_index(level=0, drop=True)
        )
        frame["institution_net_buy_value_5d"] = (
            group["institution_net_buy_value"]
            .rolling(5, min_periods=1)
            .sum()
            .reset_index(level=0, drop=True)
        )
        frame["smart_money_net_buy_value_5d"] = (
            group["smart_money_net_buy_value"]
            .rolling(5, min_periods=1)
            .sum()
            .reset_index(level=0, drop=True)
        )
        frame["flow_score"] = frame.apply(_flow_score, axis=1)
        frame["flow_reason"] = frame.apply(_flow_reason, axis=1)
        frame["feature_created_at"] = pd.Timestamp.now(tz="UTC")

        feature_frame = frame[INVESTOR_FLOW_FEATURE_COLUMNS]
        validate_investor_flow_feature_frame(feature_frame)
        return feature_frame


def _flow_score(row: pd.Series) -> float:
    score = 50.0
    foreign_5d = float(row["foreign_net_buy_value_5d"])
    institution_5d = float(row["institution_net_buy_value_5d"])
    smart_money_5d = float(row["smart_money_net_buy_value_5d"])

    if foreign_5d > 0:
        score += 10
    else:
        score -= 10

    if institution_5d > 0:
        score += 10
    else:
        score -= 10

    if smart_money_5d > 20_000_000_000:
        score += 15
    elif smart_money_5d > 0:
        score += 8
    elif smart_money_5d < -20_000_000_000:
        score -= 20
    elif smart_money_5d < 0:
        score -= 8

    return float(np.clip(score, 0, 100))


def _flow_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    foreign_5d = float(row["foreign_net_buy_value_5d"])
    institution_5d = float(row["institution_net_buy_value_5d"])
    smart_money_5d = float(row["smart_money_net_buy_value_5d"])

    if foreign_5d > 0:
        reasons.append("foreign_net_buy_positive_5d")
    else:
        reasons.append("foreign_net_sell_5d")

    if institution_5d > 0:
        reasons.append("institution_net_buy_positive_5d")
    else:
        reasons.append("institution_net_sell_5d")

    if smart_money_5d > 20_000_000_000:
        reasons.append("strong_smart_money_inflow")
    elif smart_money_5d > 0:
        reasons.append("smart_money_inflow")
    elif smart_money_5d < -20_000_000_000:
        reasons.append("strong_smart_money_outflow")
    elif smart_money_5d < 0:
        reasons.append("smart_money_outflow")

    return ", ".join(reasons) if reasons else "investor_flow_neutral"
