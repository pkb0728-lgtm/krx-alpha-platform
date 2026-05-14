from typing import Any

from krx_alpha.monitoring.api_health import (
    API_STATUS_FAILED,
    API_STATUS_MISSING,
    API_STATUS_OK,
    ApiCredentials,
    ApiHealthChecker,
    api_results_to_frame,
    format_api_health_report,
    summarize_api_results,
)


class FakeJsonHttpClient:
    def __init__(self, fail_message: str | None = None) -> None:
        self.fail_message = fail_message
        self.calls: list[tuple[str, str]] = []

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
        self.calls.append((method, url))
        if self.fail_message:
            raise RuntimeError(self.fail_message)

        if "opendart" in url:
            return 200, {"status": "000"}
        if "naver" in url:
            return 200, {"items": [{"title": "news"}]}
        if "generativelanguage" in url:
            return 200, {"models": [{"name": "gemini"}]}
        if "getMe" in url:
            return 200, {"ok": True}
        if "getChat" in url:
            return 200, {"ok": True}
        if "koreainvestment" in url:
            return 200, {"access_token": "token"}
        if "stlouisfed" in url:
            return 200, {"observations": [{"value": "4.00"}]}
        return 404, {}


def test_api_health_checker_reports_ok_for_configured_services() -> None:
    credentials = ApiCredentials(
        dart_api_key="dart",
        naver_client_id="naver-id",
        naver_client_secret="naver-secret",
        gemini_api_key="gemini",
        telegram_bot_token="telegram-token",
        telegram_chat_id="telegram-chat",
        kis_app_key="kis-key",
        kis_app_secret="kis-secret",
        kis_account_no="12345678-01",
        fred_api_key="fred",
    )

    results = ApiHealthChecker(FakeJsonHttpClient()).run(credentials, include_pykrx=False)

    assert {result.name: result.status for result in results} == {
        "OpenDART": API_STATUS_OK,
        "Naver Search": API_STATUS_OK,
        "Gemini": API_STATUS_OK,
        "Telegram": API_STATUS_OK,
        "KIS Paper": API_STATUS_OK,
        "FRED": API_STATUS_OK,
    }
    assert all(result.action == "ready" for result in results)
    assert summarize_api_results(results) == {
        "total": 6,
        "ok": 6,
        "missing": 0,
        "failed": 0,
    }

    frame = api_results_to_frame(results)
    assert frame.loc[0, "api"] == "OpenDART"
    assert frame.loc[0, "action"] == "ready"

    report = format_api_health_report(results)
    assert "API Health Report" in report
    assert "| OpenDART | OK | company endpoint returned status 000 | ready |" in report


def test_api_health_checker_reports_missing_without_network_call() -> None:
    fake_client = FakeJsonHttpClient()

    results = ApiHealthChecker(fake_client).run(ApiCredentials(), include_pykrx=False)

    assert all(result.status == API_STATUS_MISSING for result in results)
    assert results[0].action == "set DART_API_KEY in .env if this feature is needed"
    assert fake_client.calls == []


def test_api_health_checker_redacts_secret_values_from_errors() -> None:
    secret = "super-secret-dart-key"

    results = ApiHealthChecker(FakeJsonHttpClient(fail_message=f"failed with {secret}")).run(
        ApiCredentials(dart_api_key=secret),
        include_pykrx=False,
    )

    open_dart = results[0]
    assert open_dart.status == API_STATUS_FAILED
    assert secret not in open_dart.detail
    assert "[REDACTED]" in open_dart.detail
    assert (
        open_dart.action
        == "verify credentials, network access, and API quota, then rerun check-apis"
    )
