from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from krx_alpha.collectors.price_collector import PriceRequest, PykrxPriceCollector

API_STATUS_OK = "OK"
API_STATUS_MISSING = "MISSING"
API_STATUS_FAILED = "FAILED"


class JsonHttpClient(Protocol):
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
        """Return an HTTP status code and JSON response body."""


@dataclass(frozen=True)
class ApiCredentials:
    dart_api_key: str | None = None
    naver_client_id: str | None = None
    naver_client_secret: str | None = None
    gemini_api_key: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    kis_app_key: str | None = None
    kis_app_secret: str | None = None
    kis_account_no: str | None = None
    fred_api_key: str | None = None

    @classmethod
    def from_settings(cls, settings: Any) -> "ApiCredentials":
        return cls(
            dart_api_key=settings.dart_api_key,
            naver_client_id=settings.naver_client_id,
            naver_client_secret=settings.naver_client_secret,
            gemini_api_key=settings.gemini_api_key,
            telegram_bot_token=settings.telegram_bot_token,
            telegram_chat_id=settings.telegram_chat_id,
            kis_app_key=settings.kis_app_key,
            kis_app_secret=settings.kis_app_secret,
            kis_account_no=settings.kis_account_no,
            fred_api_key=settings.fred_api_key,
        )

    def secret_values(self) -> list[str]:
        return [
            value
            for value in [
                self.dart_api_key,
                self.naver_client_id,
                self.naver_client_secret,
                self.gemini_api_key,
                self.telegram_bot_token,
                self.telegram_chat_id,
                self.kis_app_key,
                self.kis_app_secret,
                self.kis_account_no,
                self.fred_api_key,
            ]
            if value
        ]


@dataclass(frozen=True)
class ApiCheckResult:
    name: str
    status: str
    detail: str

    @property
    def ok(self) -> bool:
        return self.status == API_STATUS_OK

    @property
    def action(self) -> str:
        return _recommended_action(self)


class RequestsJsonHttpClient:
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
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError(
                "requests is not installed. Run: python -m pip install -e .[data]"
            ) from exc

        response = requests.request(
            method=method,
            url=url,
            params=params,
            headers=cast(Any, headers),
            json=cast(Any, json_payload),
            timeout=timeout_seconds,
        )
        try:
            payload = response.json()
        except ValueError:
            payload = {"raw_text": response.text[:200]}

        if not isinstance(payload, dict):
            payload = {"payload": payload}
        return response.status_code, payload


class ApiHealthChecker:
    def __init__(
        self,
        http_client: JsonHttpClient | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.http_client = http_client or RequestsJsonHttpClient()
        self.timeout_seconds = timeout_seconds

    def run(self, credentials: ApiCredentials, include_pykrx: bool = True) -> list[ApiCheckResult]:
        checks = [
            self._check_opendart(credentials),
            self._check_naver(credentials),
            self._check_gemini(credentials),
            self._check_telegram(credentials),
            self._check_kis_paper(credentials),
            self._check_fred(credentials),
        ]
        if include_pykrx:
            checks.append(self._check_pykrx(credentials))
        return checks

    def _check_opendart(self, credentials: ApiCredentials) -> ApiCheckResult:
        missing = _missing_fields({"DART_API_KEY": credentials.dart_api_key})
        if missing:
            return _missing_result("OpenDART", missing)

        return self._guarded_check(
            "OpenDART",
            credentials,
            lambda: self._request_opendart(credentials.dart_api_key or ""),
        )

    def _check_naver(self, credentials: ApiCredentials) -> ApiCheckResult:
        missing = _missing_fields(
            {
                "NAVER_CLIENT_ID": credentials.naver_client_id,
                "NAVER_CLIENT_SECRET": credentials.naver_client_secret,
            }
        )
        if missing:
            return _missing_result("Naver Search", missing)

        return self._guarded_check(
            "Naver Search",
            credentials,
            lambda: self._request_naver(
                credentials.naver_client_id or "",
                credentials.naver_client_secret or "",
            ),
        )

    def _check_gemini(self, credentials: ApiCredentials) -> ApiCheckResult:
        missing = _missing_fields({"GEMINI_API_KEY": credentials.gemini_api_key})
        if missing:
            return _missing_result("Gemini", missing)

        return self._guarded_check(
            "Gemini",
            credentials,
            lambda: self._request_gemini(credentials.gemini_api_key or ""),
        )

    def _check_telegram(self, credentials: ApiCredentials) -> ApiCheckResult:
        missing = _missing_fields(
            {
                "TELEGRAM_BOT_TOKEN": credentials.telegram_bot_token,
                "TELEGRAM_CHAT_ID": credentials.telegram_chat_id,
            }
        )
        if missing:
            return _missing_result("Telegram", missing)

        return self._guarded_check(
            "Telegram",
            credentials,
            lambda: self._request_telegram(
                credentials.telegram_bot_token or "",
                credentials.telegram_chat_id or "",
            ),
        )

    def _check_kis_paper(self, credentials: ApiCredentials) -> ApiCheckResult:
        missing = _missing_fields(
            {
                "KIS_APP_KEY": credentials.kis_app_key,
                "KIS_APP_SECRET": credentials.kis_app_secret,
                "KIS_ACCOUNT_NO": credentials.kis_account_no,
            }
        )
        if missing:
            return _missing_result("KIS Paper", missing)

        return self._guarded_check(
            "KIS Paper",
            credentials,
            lambda: self._request_kis_paper(
                credentials.kis_app_key or "",
                credentials.kis_app_secret or "",
            ),
        )

    def _check_fred(self, credentials: ApiCredentials) -> ApiCheckResult:
        missing = _missing_fields({"FRED_API_KEY": credentials.fred_api_key})
        if missing:
            return _missing_result("FRED", missing)

        return self._guarded_check(
            "FRED",
            credentials,
            lambda: self._request_fred(credentials.fred_api_key or ""),
        )

    def _check_pykrx(self, credentials: ApiCredentials) -> ApiCheckResult:
        return self._guarded_check("pykrx", credentials, self._request_pykrx)

    def _guarded_check(
        self,
        name: str,
        credentials: ApiCredentials,
        check: Callable[[], ApiCheckResult],
    ) -> ApiCheckResult:
        try:
            return check()
        except Exception as exc:
            return ApiCheckResult(
                name=name,
                status=API_STATUS_FAILED,
                detail=_redact(str(exc), credentials.secret_values()),
            )

    def _request_opendart(self, api_key: str) -> ApiCheckResult:
        status_code, payload = self.http_client.request_json(
            "GET",
            "https://opendart.fss.or.kr/api/company.json",
            params={"crtfc_key": api_key, "corp_code": "00126380"},
            timeout_seconds=self.timeout_seconds,
        )
        dart_status = str(payload.get("status", ""))
        if status_code == 200 and dart_status == "000":
            return ApiCheckResult("OpenDART", API_STATUS_OK, "company endpoint returned status 000")
        return ApiCheckResult(
            "OpenDART",
            API_STATUS_FAILED,
            f"HTTP {status_code}, DART status {dart_status or 'unknown'}",
        )

    def _request_naver(self, client_id: str, client_secret: str) -> ApiCheckResult:
        status_code, payload = self.http_client.request_json(
            "GET",
            "https://openapi.naver.com/v1/search/news.json",
            params={"query": "Samsung Electronics", "display": "1", "start": "1", "sort": "date"},
            headers={
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
            },
            timeout_seconds=self.timeout_seconds,
        )
        items = payload.get("items")
        if status_code == 200 and isinstance(items, list):
            return ApiCheckResult(
                "Naver Search",
                API_STATUS_OK,
                f"news search returned {len(items)} item(s)",
            )
        return ApiCheckResult("Naver Search", API_STATUS_FAILED, f"HTTP {status_code}")

    def _request_gemini(self, api_key: str) -> ApiCheckResult:
        status_code, payload = self.http_client.request_json(
            "GET",
            "https://generativelanguage.googleapis.com/v1beta/models",
            params={"key": api_key},
            timeout_seconds=self.timeout_seconds,
        )
        models = payload.get("models")
        if status_code == 200 and isinstance(models, list):
            return ApiCheckResult(
                "Gemini",
                API_STATUS_OK,
                f"models endpoint returned {len(models)}",
            )
        return ApiCheckResult("Gemini", API_STATUS_FAILED, f"HTTP {status_code}")

    def _request_telegram(self, bot_token: str, chat_id: str) -> ApiCheckResult:
        bot_status, bot_payload = self.http_client.request_json(
            "GET",
            f"https://api.telegram.org/bot{bot_token}/getMe",
            timeout_seconds=self.timeout_seconds,
        )
        chat_status, chat_payload = self.http_client.request_json(
            "GET",
            f"https://api.telegram.org/bot{bot_token}/getChat",
            params={"chat_id": chat_id},
            timeout_seconds=self.timeout_seconds,
        )
        if bot_status == 200 and bot_payload.get("ok") is True and chat_payload.get("ok") is True:
            return ApiCheckResult("Telegram", API_STATUS_OK, "bot and chat are reachable")
        return ApiCheckResult(
            "Telegram",
            API_STATUS_FAILED,
            f"getMe HTTP {bot_status}, getChat HTTP {chat_status}",
        )

    def _request_kis_paper(self, app_key: str, app_secret: str) -> ApiCheckResult:
        status_code, payload = self.http_client.request_json(
            "POST",
            "https://openapivts.koreainvestment.com:29443/oauth2/tokenP",
            headers={"content-type": "application/json; charset=utf-8"},
            json_payload={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret,
            },
            timeout_seconds=self.timeout_seconds,
        )
        if status_code == 200 and payload.get("access_token"):
            return ApiCheckResult("KIS Paper", API_STATUS_OK, "paper token endpoint returned token")
        message = str(payload.get("msg1") or payload.get("error_description") or "")
        detail = f"HTTP {status_code}" + (f", {message}" if message else "")
        return ApiCheckResult("KIS Paper", API_STATUS_FAILED, detail)

    def _request_fred(self, api_key: str) -> ApiCheckResult:
        status_code, payload = self.http_client.request_json(
            "GET",
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": "DGS10",
                "api_key": api_key,
                "file_type": "json",
                "observation_start": "2024-01-01",
                "observation_end": "2024-01-10",
            },
            timeout_seconds=self.timeout_seconds,
        )
        observations = payload.get("observations")
        if status_code == 200 and isinstance(observations, list):
            return ApiCheckResult(
                "FRED",
                API_STATUS_OK,
                f"returned {len(observations)} observation(s)",
            )
        return ApiCheckResult("FRED", API_STATUS_FAILED, f"HTTP {status_code}")

    def _request_pykrx(self) -> ApiCheckResult:
        frame = PykrxPriceCollector().collect(
            PriceRequest.from_strings(
                ticker="005930",
                start_date="2024-01-02",
                end_date="2024-01-05",
            )
        )
        if frame.empty:
            return ApiCheckResult("pykrx", API_STATUS_FAILED, "price collector returned no rows")
        return ApiCheckResult(
            "pykrx",
            API_STATUS_OK,
            f"price collector returned {len(frame)} row(s)",
        )


def _missing_fields(fields: dict[str, str | None]) -> list[str]:
    return [name for name, value in fields.items() if not value]


def _missing_result(name: str, missing: list[str]) -> ApiCheckResult:
    return ApiCheckResult(name=name, status=API_STATUS_MISSING, detail=", ".join(missing))


def _recommended_action(result: ApiCheckResult) -> str:
    if result.status == API_STATUS_OK:
        return "ready"
    if result.status == API_STATUS_MISSING:
        return f"set {result.detail} in .env if this feature is needed"
    if result.name == "Telegram":
        return "confirm bot token, chat id, and that the bot has received your first message"
    if result.name == "KIS Paper":
        return "confirm KIS mock-investment app key/secret and paper endpoint access"
    if result.name == "pykrx":
        return "check internet access and retry later if the public data source is unavailable"
    if "CERTIFICATE_VERIFY_FAILED" in result.detail or "certificate" in result.detail.lower():
        return "check antivirus/proxy SSL inspection, then rerun with normal certificate validation"
    return "verify credentials, network access, and API quota, then rerun check-apis"


def _redact(message: str, secrets: list[str]) -> str:
    cleaned = message
    for secret in secrets:
        if secret:
            cleaned = cleaned.replace(secret, "[REDACTED]")
    return cleaned
