from dataclasses import dataclass
from typing import Any

import pandas as pd

from krx_alpha.contracts.universe_contract import validate_universe_frame


@dataclass(frozen=True)
class UniverseMember:
    ticker: str
    name: str
    market: str
    sector: str
    reason: str
    is_active: bool = True


@dataclass(frozen=True)
class UniverseDefinition:
    name: str
    description: str
    members: tuple[UniverseMember, ...]

    def tickers(self) -> list[str]:
        return [member.ticker for member in self.members if member.is_active]

    def to_frame(self) -> Any:
        frame = pd.DataFrame(
            [
                {
                    "ticker": member.ticker,
                    "name": member.name,
                    "market": member.market,
                    "sector": member.sector,
                    "reason": member.reason,
                    "is_active": member.is_active,
                }
                for member in self.members
            ]
        )
        validate_universe_frame(frame)
        return frame


class UniverseRegistry:
    def __init__(self, definitions: tuple[UniverseDefinition, ...] | None = None) -> None:
        selected_definitions = definitions or DEFAULT_UNIVERSES
        self._definitions = {definition.name: definition for definition in selected_definitions}

    def names(self) -> list[str]:
        return sorted(self._definitions)

    def get(self, name: str) -> UniverseDefinition:
        normalized_name = name.strip().lower()
        if normalized_name not in self._definitions:
            available = ", ".join(self.names())
            raise KeyError(f"Unknown universe '{name}'. Available universes: {available}")
        return self._definitions[normalized_name]

    def tickers(self, name: str) -> list[str]:
        return self.get(name).tickers()

    def list_definitions(self) -> Any:
        return pd.DataFrame(
            [
                {
                    "universe": definition.name,
                    "count": len(definition.tickers()),
                    "description": definition.description,
                }
                for definition in self._definitions.values()
            ]
        ).sort_values("universe")


DEMO_UNIVERSE = UniverseDefinition(
    name="demo",
    description="Small beginner-friendly universe for local smoke tests and demos.",
    members=(
        UniverseMember("005930", "Samsung Electronics", "KOSPI", "semiconductor", "large cap"),
        UniverseMember("000660", "SK hynix", "KOSPI", "semiconductor", "large cap"),
        UniverseMember("005380", "Hyundai Motor", "KOSPI", "auto", "large cap"),
    ),
)

SEMICONDUCTOR_AUTO_UNIVERSE = UniverseDefinition(
    name="semiconductor_auto",
    description="Semiconductor and auto names often used for Korean market monitoring.",
    members=(
        UniverseMember("005930", "Samsung Electronics", "KOSPI", "semiconductor", "mega cap"),
        UniverseMember("000660", "SK hynix", "KOSPI", "semiconductor", "mega cap"),
        UniverseMember("042700", "Hanmi Semiconductor", "KOSPI", "semiconductor", "equipment"),
        UniverseMember("005380", "Hyundai Motor", "KOSPI", "auto", "large cap"),
        UniverseMember("000270", "Kia", "KOSPI", "auto", "large cap"),
    ),
)

LARGE_CAP_UNIVERSE = UniverseDefinition(
    name="large_cap",
    description="Static large-cap watchlist for portfolio demonstrations.",
    members=(
        UniverseMember("005930", "Samsung Electronics", "KOSPI", "semiconductor", "mega cap"),
        UniverseMember("000660", "SK hynix", "KOSPI", "semiconductor", "mega cap"),
        UniverseMember("005380", "Hyundai Motor", "KOSPI", "auto", "large cap"),
        UniverseMember("000270", "Kia", "KOSPI", "auto", "large cap"),
        UniverseMember("035420", "NAVER", "KOSPI", "internet", "large cap"),
        UniverseMember("035720", "Kakao", "KOSPI", "internet", "large cap"),
        UniverseMember("051910", "LG Chem", "KOSPI", "chemical", "large cap"),
        UniverseMember("068270", "Celltrion", "KOSPI", "bio", "large cap"),
        UniverseMember("055550", "Shinhan Financial Group", "KOSPI", "finance", "large cap"),
        UniverseMember("105560", "KB Financial Group", "KOSPI", "finance", "large cap"),
    ),
)

DEFAULT_UNIVERSES = (
    DEMO_UNIVERSE,
    LARGE_CAP_UNIVERSE,
    SEMICONDUCTOR_AUTO_UNIVERSE,
)
