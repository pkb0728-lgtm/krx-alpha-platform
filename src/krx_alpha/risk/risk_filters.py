from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class RiskFilterConfig:
    min_trading_value: float = 5_000_000_000
    max_range_pct: float = 0.07
    max_volatility_5d: float = 0.04
    min_risk_score: float = 40.0


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

        return flags
