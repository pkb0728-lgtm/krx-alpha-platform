import pytest

from krx_alpha.universe.stock_resolver import (
    AmbiguousStockQuery,
    StockRecord,
    StockResolver,
    normalize_ticker,
)


def test_normalize_ticker_zero_pads_short_codes() -> None:
    assert normalize_ticker("5930") == "005930"
    assert normalize_ticker("005930") == "005930"
    assert normalize_ticker("삼성전자") is None


def test_resolver_resolves_korean_company_name_without_live_sources() -> None:
    result = StockResolver(use_live_sources=False).resolve("삼성전자")

    assert result.ticker == "005930"
    assert result.name == "삼성전자"
    assert result.matched_by == "name"


def test_resolver_resolves_english_alias_without_duplicate_universe_match() -> None:
    result = StockResolver(use_live_sources=False).resolve("Samsung Electronics")

    assert result.ticker == "005930"
    assert result.name == "삼성전자"


def test_resolver_accepts_ticker_even_when_name_is_unknown() -> None:
    result = StockResolver(use_live_sources=False).resolve("123456")

    assert result.ticker == "123456"
    assert result.name == ""
    assert result.source == "user_input"


def test_resolver_reports_ambiguous_partial_names() -> None:
    records = [
        StockRecord("005930", "삼성전자", (), "test"),
        StockRecord("006400", "삼성SDI", (), "test"),
    ]

    with pytest.raises(AmbiguousStockQuery) as exc_info:
        StockResolver(records, use_live_sources=False).resolve("삼성")

    assert [candidate.ticker for candidate in exc_info.value.candidates] == [
        "005930",
        "006400",
    ]
