from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd

from krx_alpha.contracts.dart_contract import (
    validate_dart_company_frame,
    validate_dart_disclosure_frame,
    validate_dart_financial_frame,
)

OPENDART_BASE_URL = "https://opendart.fss.or.kr/api"
REPORT_CODE_ANNUAL = "11011"

TICKER_TO_DART_CORP_CODE = {
    "005930": "00126380",
    "000660": "00164779",
    "035420": "00266961",
    "035720": "00401731",
}


class DartJsonProvider(Protocol):
    def __call__(self, endpoint: str, params: dict[str, str]) -> dict[str, Any]:
        """Return OpenDART JSON response."""


@dataclass(frozen=True)
class DartCompanyRequest:
    corp_code: str
    ticker: str
    demo: bool = True


@dataclass(frozen=True)
class DartFinancialStatementRequest:
    corp_code: str
    ticker: str
    bsns_year: str
    reprt_code: str = REPORT_CODE_ANNUAL
    fs_div: str = "CFS"
    demo: bool = True


@dataclass(frozen=True)
class DartDisclosureSearchRequest:
    corp_code: str
    ticker: str
    start_date: str
    end_date: str
    page_count: int = 20
    demo: bool = True

    @property
    def start_compact(self) -> str:
        return self.start_date.replace("-", "")

    @property
    def end_compact(self) -> str:
        return self.end_date.replace("-", "")


class OpenDartCollector:
    source_name = "opendart"
    demo_source_name = "opendart_demo"

    def __init__(
        self,
        api_key: str | None = None,
        provider: DartJsonProvider | None = None,
    ) -> None:
        self.api_key = api_key
        self.provider = provider or _requests_provider

    def collect_company(self, request: DartCompanyRequest) -> Any:
        payload = (
            _demo_company_payload(request)
            if request.demo or not self.api_key
            else self.provider(
                f"{OPENDART_BASE_URL}/company.json",
                {
                    "crtfc_key": self.api_key,
                    "corp_code": request.corp_code,
                },
            )
        )
        _raise_for_dart_error(payload)
        frame = _normalize_company(payload, request)
        validate_dart_company_frame(frame)
        return frame

    def collect_financial_statement(self, request: DartFinancialStatementRequest) -> Any:
        payload = (
            _demo_financial_payload(request)
            if request.demo or not self.api_key
            else self.provider(
                f"{OPENDART_BASE_URL}/fnlttSinglAcnt.json",
                {
                    "crtfc_key": self.api_key,
                    "corp_code": request.corp_code,
                    "bsns_year": request.bsns_year,
                    "reprt_code": request.reprt_code,
                    "fs_div": request.fs_div,
                },
            )
        )
        _raise_for_dart_error(payload)
        frame = _normalize_financial_statement(payload, request)
        validate_dart_financial_frame(frame)
        return frame

    def collect_disclosures(self, request: DartDisclosureSearchRequest) -> Any:
        payload = (
            _demo_disclosure_payload(request)
            if request.demo or not self.api_key
            else self.provider(
                f"{OPENDART_BASE_URL}/list.json",
                {
                    "crtfc_key": self.api_key,
                    "corp_code": request.corp_code,
                    "bgn_de": request.start_compact,
                    "end_de": request.end_compact,
                    "page_count": str(request.page_count),
                },
            )
        )
        _raise_for_dart_error(payload)
        frame = _normalize_disclosures(payload, request)
        validate_dart_disclosure_frame(frame)
        return frame


def resolve_corp_code(ticker: str, corp_code: str | None = None) -> str:
    if corp_code:
        return corp_code.zfill(8)

    normalized_ticker = ticker.zfill(6)
    if normalized_ticker not in TICKER_TO_DART_CORP_CODE:
        raise ValueError(
            f"No built-in DART corp_code mapping for {normalized_ticker}. "
            "Provide --corp-code or add it to TICKER_TO_DART_CORP_CODE."
        )
    return TICKER_TO_DART_CORP_CODE[normalized_ticker]


def _requests_provider(endpoint: str, params: dict[str, str]) -> dict[str, Any]:
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError(
            "requests is not installed. Run: python -m pip install -e .[data]"
        ) from exc

    response = requests.get(endpoint, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise ValueError("OpenDART response is not a JSON object.")
    return data


def _raise_for_dart_error(payload: dict[str, Any]) -> None:
    status = str(payload.get("status", "000"))
    if status != "000":
        message = str(payload.get("message", "Unknown OpenDART error."))
        raise ValueError(f"OpenDART API error {status}: {message}")


def _normalize_company(payload: dict[str, Any], request: DartCompanyRequest) -> Any:
    frame = pd.DataFrame(
        [
            {
                "corp_code": str(payload.get("corp_code", request.corp_code)).zfill(8),
                "stock_code": str(payload.get("stock_code", request.ticker)).zfill(6),
                "stock_name": str(payload.get("stock_name", "")),
                "corp_name": str(payload.get("corp_name", "")),
                "corp_name_eng": str(payload.get("corp_name_eng", "")),
                "corp_cls": str(payload.get("corp_cls", "")),
                "ceo_nm": str(payload.get("ceo_nm", "")),
                "induty_code": str(payload.get("induty_code", "")),
                "est_dt": str(payload.get("est_dt", "")),
                "acc_mt": str(payload.get("acc_mt", "")),
                "source": _source_name(request.demo),
                "collected_at": pd.Timestamp.now(tz="UTC"),
            }
        ]
    )
    return frame


def _normalize_financial_statement(
    payload: dict[str, Any],
    request: DartFinancialStatementRequest,
) -> Any:
    rows = []
    for item in payload.get("list", []):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "corp_code": str(item.get("corp_code", request.corp_code)).zfill(8),
                "ticker": request.ticker.zfill(6),
                "bsns_year": str(item.get("bsns_year", request.bsns_year)),
                "reprt_code": str(item.get("reprt_code", request.reprt_code)),
                "fs_div": str(item.get("fs_div", request.fs_div)),
                "sj_div": str(item.get("sj_div", "")),
                "account_id": str(item.get("account_id", "")),
                "account_nm": str(item.get("account_nm", "")),
                "thstrm_amount": str(item.get("thstrm_amount", "")),
                "thstrm_amount_value": _parse_amount(item.get("thstrm_amount")),
                "frmtrm_amount": str(item.get("frmtrm_amount", "")),
                "frmtrm_amount_value": _parse_amount(item.get("frmtrm_amount")),
                "currency": str(item.get("currency", "KRW") or "KRW"),
                "source": _source_name(request.demo),
                "collected_at": pd.Timestamp.now(tz="UTC"),
            }
        )
    return pd.DataFrame(rows)


def _normalize_disclosures(payload: dict[str, Any], request: DartDisclosureSearchRequest) -> Any:
    rows = []
    for item in payload.get("list", []):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "corp_code": str(item.get("corp_code", request.corp_code)).zfill(8),
                "corp_name": str(item.get("corp_name", "")),
                "stock_code": str(item.get("stock_code", request.ticker)).zfill(6),
                "report_nm": str(item.get("report_nm", "")),
                "rcept_no": str(item.get("rcept_no", "")),
                "rcept_dt": str(item.get("rcept_dt", "")),
                "flr_nm": str(item.get("flr_nm", "")),
                "rm": str(item.get("rm", "")),
                "source": _source_name(request.demo),
                "collected_at": pd.Timestamp.now(tz="UTC"),
            }
        )
    return pd.DataFrame(rows)


def _parse_amount(value: Any) -> float | None:
    if value is None or value == "":
        return None
    cleaned = str(value).replace(",", "").replace(" ", "")
    if cleaned in {"-", "nan", "None"}:
        return None
    return float(cleaned)


def _source_name(demo: bool) -> str:
    return OpenDartCollector.demo_source_name if demo else OpenDartCollector.source_name


def _demo_company_payload(request: DartCompanyRequest) -> dict[str, Any]:
    stock_name = {"005930": "Samsung Electronics", "000660": "SK hynix"}.get(
        request.ticker.zfill(6),
        "Demo Company",
    )
    return {
        "status": "000",
        "message": "normal",
        "corp_code": request.corp_code,
        "corp_name": stock_name,
        "corp_name_eng": stock_name,
        "stock_name": stock_name,
        "stock_code": request.ticker.zfill(6),
        "corp_cls": "Y",
        "ceo_nm": "Demo CEO",
        "induty_code": "264",
        "est_dt": "19690113",
        "acc_mt": "12",
    }


def _demo_financial_payload(request: DartFinancialStatementRequest) -> dict[str, Any]:
    base_rows = [
        ("ifrs-full_Revenue", "매출액", "2,589,355,000,000", "2,302,314,000,000"),
        ("dart_OperatingIncomeLoss", "영업이익", "656,697,000,000", "432,000,000,000"),
        ("ifrs-full_ProfitLoss", "당기순이익", "531,012,000,000", "398,400,000,000"),
        ("ifrs-full_Assets", "자산총계", "9,876,543,000,000", "9,100,000,000,000"),
        ("ifrs-full_Liabilities", "부채총계", "3,210,000,000,000", "3,000,000,000,000"),
        ("ifrs-full_Equity", "자본총계", "6,666,543,000,000", "6,100,000,000,000"),
    ]
    return {
        "status": "000",
        "message": "normal",
        "list": [
            {
                "corp_code": request.corp_code,
                "bsns_year": request.bsns_year,
                "reprt_code": request.reprt_code,
                "fs_div": request.fs_div,
                "sj_div": "IS" if index < 3 else "BS",
                "account_id": account_id,
                "account_nm": account_nm,
                "thstrm_amount": thstrm_amount,
                "frmtrm_amount": frmtrm_amount,
                "currency": "KRW",
            }
            for index, (account_id, account_nm, thstrm_amount, frmtrm_amount) in enumerate(
                base_rows
            )
        ],
    }


def _demo_disclosure_payload(request: DartDisclosureSearchRequest) -> dict[str, Any]:
    return {
        "status": "000",
        "message": "normal",
        "list": [
            {
                "corp_code": request.corp_code,
                "corp_name": "Demo Company",
                "stock_code": request.ticker.zfill(6),
                "report_nm": "사업보고서",
                "rcept_no": f"{request.end_compact}000001",
                "rcept_dt": request.end_compact,
                "flr_nm": "Demo Company",
                "rm": "demo",
            },
            {
                "corp_code": request.corp_code,
                "corp_name": "Demo Company",
                "stock_code": request.ticker.zfill(6),
                "report_nm": "분기보고서",
                "rcept_no": f"{request.end_compact}000002",
                "rcept_dt": request.end_compact,
                "flr_nm": "Demo Company",
                "rm": "demo",
            },
        ],
    }
