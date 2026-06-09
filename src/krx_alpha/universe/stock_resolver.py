import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast

import pandas as pd

from krx_alpha.universe.static_universe import DEFAULT_UNIVERSES
from krx_alpha.utils.external_output import suppress_external_output

_TICKER_PATTERN = re.compile(r"^\d{1,6}$")


@dataclass(frozen=True)
class StockCandidate:
    ticker: str
    name: str
    source: str


@dataclass(frozen=True)
class StockResolution:
    ticker: str
    name: str
    matched_by: str
    source: str


@dataclass(frozen=True)
class StockRecord:
    ticker: str
    name: str
    aliases: tuple[str, ...]
    source: str

    def search_terms(self) -> tuple[str, ...]:
        return (self.ticker, self.name, *self.aliases)

    def to_candidate(self) -> StockCandidate:
        return StockCandidate(ticker=self.ticker, name=self.name, source=self.source)


class AmbiguousStockQuery(ValueError):
    def __init__(self, query: str, candidates: list[StockCandidate]) -> None:
        self.query = query
        self.candidates = candidates
        super().__init__(f"Stock query '{query}' matched multiple stocks.")


class StockResolver:
    """Resolve Korean stock names or ticker codes into a six-digit ticker."""

    def __init__(
        self,
        records: Iterable[StockRecord] | None = None,
        *,
        use_live_sources: bool = True,
    ) -> None:
        self._static_records = list(records or _default_static_records())
        self._use_live_sources = use_live_sources
        self._live_records: list[StockRecord] | None = None

    def resolve(self, query: str) -> StockResolution:
        cleaned_query = query.strip()
        if not cleaned_query:
            raise ValueError("Stock query is empty.")

        ticker = normalize_ticker(cleaned_query)
        if ticker is not None:
            record = self._record_by_ticker(ticker)
            return StockResolution(
                ticker=ticker,
                name=record.name if record is not None else "",
                matched_by="ticker",
                source=record.source if record is not None else "user_input",
            )

        static_result = self._resolve_from_records(cleaned_query, self._static_records)
        if static_result is not None:
            return static_result

        live_records = self._get_live_records()
        if live_records:
            live_result = self._resolve_from_records(cleaned_query, live_records)
            if live_result is not None:
                return live_result

        raise ValueError(f"Unknown stock query: {query}")

    def candidates(self, query: str, limit: int = 10) -> list[StockCandidate]:
        records = [*self._static_records, *self._get_live_records()]
        matches = _deduplicate_candidates(_partial_matches(query, records))
        return matches[:limit]

    def _resolve_from_records(
        self,
        query: str,
        records: list[StockRecord],
    ) -> StockResolution | None:
        exact_matches = _deduplicate_records(_exact_matches(query, records))
        if len(exact_matches) == 1:
            record = exact_matches[0]
            return StockResolution(
                ticker=record.ticker,
                name=record.name,
                matched_by="name",
                source=record.source,
            )
        if len(exact_matches) > 1:
            raise AmbiguousStockQuery(query, _deduplicate_candidates(exact_matches))

        partial_matches = _deduplicate_records(_partial_matches(query, records))
        if len(partial_matches) == 1:
            record = partial_matches[0]
            return StockResolution(
                ticker=record.ticker,
                name=record.name,
                matched_by="partial_name",
                source=record.source,
            )
        if len(partial_matches) > 1:
            raise AmbiguousStockQuery(query, _deduplicate_candidates(partial_matches))

        return None

    def _record_by_ticker(self, ticker: str) -> StockRecord | None:
        for record in [*self._static_records, *self._get_live_records()]:
            if record.ticker == ticker:
                return record
        return None

    def _get_live_records(self) -> list[StockRecord]:
        if not self._use_live_sources:
            return []
        if self._live_records is None:
            self._live_records = _load_finance_data_reader_records()
        return self._live_records


def normalize_ticker(value: str) -> str | None:
    stripped = value.strip()
    if not _TICKER_PATTERN.fullmatch(stripped):
        return None
    return stripped.zfill(6)


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", "", value).casefold()


def _exact_matches(query: str, records: list[StockRecord]) -> list[StockRecord]:
    normalized_query = _normalize_name(query)
    return [
        record
        for record in records
        if any(_normalize_name(term) == normalized_query for term in record.search_terms())
    ]


def _partial_matches(query: str, records: list[StockRecord]) -> list[StockRecord]:
    normalized_query = _normalize_name(query)
    return [
        record
        for record in records
        if any(
            normalized_query in _normalize_name(term)
            for term in record.search_terms()
            if not normalize_ticker(term)
        )
    ]


def _deduplicate_candidates(records: list[StockRecord]) -> list[StockCandidate]:
    return [record.to_candidate() for record in _deduplicate_records(records)]


def _deduplicate_records(records: list[StockRecord]) -> list[StockRecord]:
    selected_records: list[StockRecord] = []
    seen_tickers: set[str] = set()
    for record in records:
        if record.ticker in seen_tickers:
            continue
        seen_tickers.add(record.ticker)
        selected_records.append(record)
    return selected_records


def _default_static_records() -> list[StockRecord]:
    records = [
        StockRecord(
            ticker="005930",
            name="삼성전자",
            aliases=("Samsung Electronics", "삼전"),
            source="static_alias",
        ),
        StockRecord(
            ticker="000660",
            name="SK하이닉스",
            aliases=("SK hynix", "하이닉스"),
            source="static_alias",
        ),
        StockRecord(
            ticker="005380",
            name="현대차",
            aliases=("Hyundai Motor", "현대자동차"),
            source="static_alias",
        ),
        StockRecord(
            ticker="000270",
            name="기아",
            aliases=("Kia", "기아차"),
            source="static_alias",
        ),
        StockRecord(
            ticker="035420",
            name="NAVER",
            aliases=("네이버",),
            source="static_alias",
        ),
        StockRecord(
            ticker="035720",
            name="카카오",
            aliases=("Kakao",),
            source="static_alias",
        ),
        StockRecord(
            ticker="051910",
            name="LG화학",
            aliases=("LG Chem",),
            source="static_alias",
        ),
        StockRecord(
            ticker="068270",
            name="셀트리온",
            aliases=("Celltrion",),
            source="static_alias",
        ),
        StockRecord(
            ticker="055550",
            name="신한지주",
            aliases=("Shinhan Financial Group", "신한금융지주"),
            source="static_alias",
        ),
        StockRecord(
            ticker="105560",
            name="KB금융",
            aliases=("KB Financial Group", "KB금융지주"),
            source="static_alias",
        ),
        StockRecord(
            ticker="042700",
            name="한미반도체",
            aliases=("Hanmi Semiconductor",),
            source="static_alias",
        ),
    ]

    universe_records = [
        StockRecord(
            ticker=member.ticker,
            name=member.name,
            aliases=(),
            source=f"universe:{definition.name}",
        )
        for definition in DEFAULT_UNIVERSES
        for member in definition.members
    ]
    return [*records, *universe_records]


def _load_finance_data_reader_records() -> list[StockRecord]:
    try:
        with suppress_external_output():
            import FinanceDataReader as fdr
    except Exception:
        return []

    try:
        with suppress_external_output():
            listing = cast(pd.DataFrame, fdr.StockListing("KRX"))
    except Exception:
        return []

    if listing.empty:
        return []

    code_column = _first_existing_column(listing, ("Code", "Symbol", "code", "symbol"))
    name_column = _first_existing_column(listing, ("Name", "name"))
    if code_column is None or name_column is None:
        return []

    records: list[StockRecord] = []
    selected_columns = listing[[code_column, name_column]].dropna()
    for row in selected_columns.itertuples(index=False, name=None):
        ticker = normalize_ticker(str(row[0]))
        name = str(row[1]).strip()
        if ticker is None or not name:
            continue
        records.append(
            StockRecord(
                ticker=ticker,
                name=name,
                aliases=(),
                source="finance_data_reader",
            )
        )
    return records


def _first_existing_column(frame: pd.DataFrame, names: tuple[str, ...]) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    return None
