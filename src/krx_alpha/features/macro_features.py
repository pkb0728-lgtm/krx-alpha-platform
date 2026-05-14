from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.macro_contract import validate_macro_feature_frame, validate_macro_frame

MACRO_FEATURE_COLUMNS = [
    "date",
    "as_of_date",
    "us_10y_yield",
    "fed_funds_rate",
    "usdkrw",
    "us_10y_yield_change_5d",
    "usdkrw_change_5d",
    "usdkrw_change_pct_5d",
    "macro_score",
    "macro_reason",
    "source",
    "feature_created_at",
]


class MacroFeatureBuilder:
    """Build explainable macro risk features from FRED observations."""

    source_name = "macro_features"

    def build(self, macro_frame: Any) -> pd.DataFrame:
        validate_macro_frame(macro_frame)
        frame = macro_frame.copy()
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["series_id"] = frame["series_id"].astype(str).str.upper()
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")

        pivot = (
            frame.pivot_table(index="date", columns="series_id", values="value", aggfunc="last")
            .sort_index()
            .astype(float)
        )
        all_dates = pd.date_range(min(pivot.index), max(pivot.index), freq="B").date
        pivot = pivot.reindex(all_dates).ffill()

        feature_frame = pd.DataFrame(
            {
                "date": pivot.index,
                "as_of_date": pivot.index,
                "us_10y_yield": _series_or_nan(pivot, "DGS10"),
                "fed_funds_rate": _series_or_nan(pivot, "DFF"),
                "usdkrw": _series_or_nan(pivot, "DEXKOUS"),
            }
        ).reset_index(drop=True)
        feature_frame["us_10y_yield_change_5d"] = feature_frame["us_10y_yield"].diff(5)
        feature_frame["usdkrw_change_5d"] = feature_frame["usdkrw"].diff(5)
        feature_frame["usdkrw_change_pct_5d"] = feature_frame["usdkrw"].pct_change(
            periods=5,
            fill_method=None,
        )
        feature_frame["macro_score"] = feature_frame.apply(_macro_score, axis=1)
        feature_frame["macro_reason"] = feature_frame.apply(_macro_reason, axis=1)
        feature_frame["source"] = self.source_name
        feature_frame["feature_created_at"] = pd.Timestamp.now(tz="UTC")

        feature_frame = feature_frame[MACRO_FEATURE_COLUMNS]
        validate_macro_feature_frame(feature_frame)
        return feature_frame


def _series_or_nan(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce")
    return pd.Series([np.nan] * len(frame), index=frame.index)


def _macro_score(row: pd.Series) -> float:
    score = 50.0
    us_10y_yield = row.get("us_10y_yield")
    fed_funds_rate = row.get("fed_funds_rate")
    yield_change = row.get("us_10y_yield_change_5d")
    usdkrw_change_pct = row.get("usdkrw_change_pct_5d")

    if pd.notna(us_10y_yield):
        if float(us_10y_yield) >= 5.0:
            score -= 10.0
        elif float(us_10y_yield) >= 4.5:
            score -= 6.0
        elif float(us_10y_yield) <= 3.5:
            score += 5.0

    if pd.notna(fed_funds_rate) and float(fed_funds_rate) >= 5.0:
        score -= 5.0

    if pd.notna(yield_change):
        if float(yield_change) >= 0.15:
            score -= 12.0
        elif float(yield_change) <= -0.15:
            score += 8.0

    if pd.notna(usdkrw_change_pct):
        if float(usdkrw_change_pct) >= 0.03:
            score -= 10.0
        elif float(usdkrw_change_pct) <= -0.02:
            score += 6.0

    return float(np.clip(score, 0, 100))


def _macro_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    us_10y_yield = row.get("us_10y_yield")
    fed_funds_rate = row.get("fed_funds_rate")
    yield_change = row.get("us_10y_yield_change_5d")
    usdkrw_change_pct = row.get("usdkrw_change_pct_5d")

    if pd.notna(us_10y_yield):
        if float(us_10y_yield) >= 5.0:
            reasons.append("us_10y_yield_high")
        elif float(us_10y_yield) >= 4.5:
            reasons.append("us_10y_yield_elevated")
        elif float(us_10y_yield) <= 3.5:
            reasons.append("us_10y_yield_supportive")

    if pd.notna(fed_funds_rate) and float(fed_funds_rate) >= 5.0:
        reasons.append("fed_funds_rate_restrictive")

    if pd.notna(yield_change):
        if float(yield_change) >= 0.15:
            reasons.append("us_10y_yield_rising")
        elif float(yield_change) <= -0.15:
            reasons.append("us_10y_yield_falling")

    if pd.notna(usdkrw_change_pct):
        if float(usdkrw_change_pct) >= 0.03:
            reasons.append("usdkrw_rising_fx_pressure")
        elif float(usdkrw_change_pct) <= -0.02:
            reasons.append("usdkrw_falling_fx_tailwind")

    return ", ".join(reasons) if reasons else "macro_environment_neutral"
