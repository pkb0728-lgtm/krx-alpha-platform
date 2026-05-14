from types import SimpleNamespace
from typing import Any

import pytest

from krx_alpha.broker.kis_paper import (
    KIS_PAPER_BASE_URL,
    KISPaperAccountId,
    KISPaperClient,
    KISPaperCredentials,
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
        headers: dict[str, str] | None = None,
        json_payload: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> tuple[int, dict[str, Any]]:
        self.calls.append(
            {
                "method": method,
                "url": url,
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
