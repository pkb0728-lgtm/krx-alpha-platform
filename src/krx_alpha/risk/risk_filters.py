from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class RiskFilterConfig:
    min_trading_value: float = 5_000_000_000
    max_range_pct: float = 0.07
    max_volatility_5d: float = 0.04
    min_risk_score: float = 40.0
    min_macro_score: float = 25.0
    blocked_market_regimes: tuple[str, ...] = ("bear", "high_volatility")


class RiskFilter:
    """Apply conservative risk blocks before a score becomes an actionable signal."""

    def __init__(self, config: RiskFilterConfig | None = None) -> None:
        self.config = config or RiskFilterConfig()

    def evaluate(self, row: Any) -> list[str]:
        flags: list[str] = []

        if pd.isna(row.get("ma_20")) or pd.isna(row.get("rsi_14")):
            flags.append("insufficient_history")

        trading_value = row.get("trading_value")
        if pd.notna(trading_value) and float(trading_value) < self.config.min_trading_value:
            flags.append("low_liquidity")

        range_pct = row.get("range_pct")
        if pd.notna(range_pct) and float(range_pct) > self.config.max_range_pct:
            flags.append("wide_daily_range")

        volatility_5d = row.get("volatility_5d")
        if pd.notna(volatility_5d) and float(volatility_5d) > self.config.max_volatility_5d:
            flags.append("high_short_term_volatility")

        risk_score = row.get("risk_score")
        if pd.notna(risk_score) and float(risk_score) < self.config.min_risk_score:
            flags.append("weak_risk_score")

        if bool(row.get("event_risk_flag", False)):
            flags.append("disclosure_event_risk")

        flow_score = row.get("flow_score")
        if pd.notna(flow_score) and float(flow_score) < 25:
            flags.append("weak_investor_flow")

        macro_score = row.get("macro_score")
        if pd.notna(macro_score) and float(macro_score) < self.config.min_macro_score:
            flags.append("weak_macro_environment")

        market_regime = str(row.get("market_regime", "")).lower()
        if market_regime in self.config.blocked_market_regimes:
            flags.append(f"market_regime_{market_regime}")

        return flags
