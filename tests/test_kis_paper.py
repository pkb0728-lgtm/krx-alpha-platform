from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from krx_alpha.broker.kis_paper import (
    KIS_DOMESTIC_BALANCE_PATH,
    KIS_PAPER_BALANCE_TR_ID,
    KIS_PAPER_BASE_URL,
    KISPaperAccountId,
    KISPaperClient,
    KISPaperCredentials,
    KISPaperToken,
)


class FakeKISHttpClient:
    def __init__(self, status_code: int = 200, payload: dict[str, Any] | None = None) -> None:
        self.status_code = status_code
        self.payload = payload or {
            "access_token": "abcd1234efgh5678",
            "token_type": "Bearer",
            "expires_in": "86400",
        }
        self.calls: list[dict[str, Any]] = []

    def request_json(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        json_payload: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "headers": headers,
                "json_payload": json_payload,
                "timeout_seconds": timeout_seconds,
            }
        )
        return self.status_code, self.payload


def test_kis_paper_account_id_normalizes_common_formats() -> None:
    assert KISPaperAccountId.parse("12345678-01").normalized == "12345678-01"
    assert KISPaperAccountId.parse("1234567801").normalized == "12345678-01"

    with pytest.raises(ValueError, match="KIS_ACCOUNT_NO"):
        KISPaperAccountId.parse("1234")


def test_kis_paper_credentials_from_settings_requires_all_values() -> None:
    settings = SimpleNamespace(kis_app_key="key", kis_app_secret=None, kis_account_no="12345678-01")

    with pytest.raises(ValueError, match="KIS_APP_SECRET"):
        KISPaperCredentials.from_settings(settings)


def test_kis_paper_client_issues_token_against_mock_server_only() -> None:
    credentials = KISPaperCredentials(
        app_key="app-key",
        app_secret="app-secret",
        account_id=KISPaperAccountId.parse("12345678-01"),
    )
    fake_client = FakeKISHttpClient()

    token = KISPaperClient(credentials, http_client=fake_client).issue_access_token()

    assert token.token_type == "Bearer"
    assert token.expires_in_seconds == 86400
    assert token.redacted == "abcd...[REDACTED]...5678"
    assert fake_client.calls[0]["method"] == "POST"
    assert fake_client.calls[0]["url"] == f"{KIS_PAPER_BASE_URL}/oauth2/tokenP"
    assert fake_client.calls[0]["json_payload"] == {
        "grant_type": "client_credentials",
        "appkey": "app-key",
        "appsecret": "app-secret",
    }


def test_kis_paper_client_reuses_cached_token(tmp_path: Path) -> None:
    credentials = KISPaperCredentials(
        app_key="app-key",
        app_secret="app-secret",
        account_id=KISPaperAccountId.parse("12345678-01"),
    )
    cache_path = tmp_path / "kis_paper_token.json"
    first_client = FakeKISHttpClient()

    issued_token = KISPaperClient(
        credentials,
        http_client=first_client,
        token_cache_path=cache_path,
    ).issue_access_token()

    assert cache_path.exists()
    assert issued_token.access_token == "abcd1234efgh5678"
    assert len(first_client.calls) == 1

    rate_limited_client = FakeKISHttpClient(
        status_code=403,
        payload={"msg1": "접근토큰 발급 잠시 후 다시 시도하세요(1분당 1회)"},
    )
    cached_token = KISPaperClient(
        credentials,
        http_client=rate_limited_client,
        token_cache_path=cache_path,
    ).issue_access_token()

    assert cached_token.access_token == issued_token.access_token
    assert rate_limited_client.calls == []


def test_kis_paper_client_redacts_secret_from_failure_message() -> None:
    credentials = KISPaperCredentials(
        app_key="app-key",
        app_secret="very-secret",
        account_id=KISPaperAccountId.parse("12345678-01"),
    )
    fake_client = FakeKISHttpClient(
        status_code=401,
        payload={"msg1": "invalid very-secret"},
    )

    with pytest.raises(RuntimeError) as exc_info:
        KISPaperClient(credentials, http_client=fake_client).issue_access_token()

    message = str(exc_info.value)
    assert "very-secret" not in message
    assert "[REDACTED]" in message


def test_kis_paper_client_inquires_balance_with_paper_transaction_id() -> None:
    credentials = KISPaperCredentials(
        app_key="app-key",
        app_secret="app-secret",
        account_id=KISPaperAccountId.parse("12345678-01"),
    )
    fake_client = FakeKISHttpClient(
        payload={
            "rt_cd": "0",
            "output1": [
                {
                    "pdno": "005930",
                    "prdt_name": "Samsung Electronics",
                    "hldg_qty": "3",
                    "ord_psbl_qty": "2",
                    "pchs_avg_pric": "70000",
                    "prpr": "72000",
                    "evlu_amt": "216000",
                    "evlu_pfls_amt": "6000",
                    "evlu_pfls_rt": "2.86",
                },
                {
                    "pdno": "000660",
                    "prdt_name": "SK Hynix",
                    "hldg_qty": "0",
                },
            ],
            "output2": [
                {
                    "dnca_tot_amt": "1000000",
                    "tot_evlu_amt": "1216000",
                    "scts_evlu_amt": "216000",
                    "pchs_amt_smtl_amt": "210000",
                    "evlu_pfls_smtl_amt": "6000",
                    "asst_icdc_erng_rt": "0.50",
                }
            ],
        }
    )
    client = KISPaperClient(credentials, http_client=fake_client)

    balance = client.inquire_balance(KISPaperToken("paper-token", "Bearer", 86400))

    balance_call = fake_client.calls[0]
    assert balance_call["method"] == "GET"
    assert balance_call["url"] == f"{KIS_PAPER_BASE_URL}{KIS_DOMESTIC_BALANCE_PATH}"
    assert balance_call["params"]["CANO"] == "12345678"
    assert balance_call["params"]["ACNT_PRDT_CD"] == "01"
    assert balance_call["headers"]["tr_id"] == KIS_PAPER_BALANCE_TR_ID
    assert balance.account == "12345678-01"
    assert balance.cash_amount == 1_000_000.0
    assert balance.total_evaluation_amount == 1_216_000.0
    assert len(balance.holdings) == 1
    assert balance.holdings[0].ticker == "005930"
    assert balance.holdings[0].quantity == 3


def test_kis_paper_client_redacts_balance_failure_message() -> None:
    credentials = KISPaperCredentials(
        app_key="app-key",
        app_secret="very-secret",
        account_id=KISPaperAccountId.parse("12345678-01"),
    )
    fake_client = FakeKISHttpClient(
        status_code=401,
        payload={"msg1": "invalid very-secret"},
    )

    with pytest.raises(RuntimeError) as exc_info:
        KISPaperClient(credentials, http_client=fake_client).inquire_balance(
            token=KISPaperToken("paper-token", "Bearer", 86400)
        )

    message = str(exc_info.value)
    assert "very-secret" not in message
    assert "[REDACTED]" in message
