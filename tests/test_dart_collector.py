import pytest

from krx_alpha.collectors.dart_collector import (
    DartCompanyRequest,
    DartDisclosureSearchRequest,
    DartFinancialStatementRequest,
    OpenDartCollector,
    resolve_corp_code,
)


def test_open_dart_collector_collects_demo_company() -> None:
    request = DartCompanyRequest(corp_code="00126380", ticker="005930", demo=True)

    frame = OpenDartCollector().collect_company(request)

    assert frame.loc[0, "corp_code"] == "00126380"
    assert frame.loc[0, "stock_code"] == "005930"
    assert frame.loc[0, "source"] == "opendart_demo"


def test_open_dart_collector_normalizes_financial_statement_from_provider() -> None:
    def fake_provider(endpoint: str, params: dict[str, str]) -> dict[str, object]:
        assert endpoint.endswith("/fnlttSinglAcnt.json")
        assert params["corp_code"] == "00126380"
        return {
            "status": "000",
            "message": "normal",
            "list": [
                {
                    "corp_code": "00126380",
                    "bsns_year": "2023",
                    "reprt_code": "11011",
                    "fs_div": "CFS",
                    "sj_div": "IS",
                    "account_nm": "매출액",
                    "thstrm_amount": "1,234,000",
                    "frmtrm_amount": "1,000,000",
                    "currency": "KRW",
                }
            ],
        }

    request = DartFinancialStatementRequest(
        corp_code="00126380",
        ticker="005930",
        bsns_year="2023",
        demo=False,
    )

    frame = OpenDartCollector(api_key="x" * 40, provider=fake_provider).collect_financial_statement(
        request
    )

    assert frame.loc[0, "account_nm"] == "매출액"
    assert frame.loc[0, "thstrm_amount_value"] == 1234000.0
    assert frame.loc[0, "source"] == "opendart"


def test_open_dart_collector_collects_demo_disclosures() -> None:
    request = DartDisclosureSearchRequest(
        corp_code="00126380",
        ticker="005930",
        start_date="2024-01-01",
        end_date="2024-01-31",
        demo=True,
    )

    frame = OpenDartCollector().collect_disclosures(request)

    assert len(frame) == 2
    assert frame.loc[0, "rcept_dt"] == "20240131"
    assert frame.loc[0, "source"] == "opendart_demo"


def test_open_dart_collector_raises_for_api_error() -> None:
    def fake_provider(endpoint: str, params: dict[str, str]) -> dict[str, object]:
        return {"status": "010", "message": "invalid key"}

    request = DartCompanyRequest(corp_code="00126380", ticker="005930", demo=False)

    with pytest.raises(ValueError, match="OpenDART API error 010"):
        OpenDartCollector(api_key="x" * 40, provider=fake_provider).collect_company(request)


def test_resolve_corp_code_supports_builtin_and_override() -> None:
    assert resolve_corp_code("005930") == "00126380"
    assert resolve_corp_code("123456", corp_code="777") == "00000777"
