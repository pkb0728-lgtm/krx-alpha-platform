from dataclasses import dataclass
from typing import Any, Protocol, cast

KIS_PAPER_BASE_URL = "https://openapivts.koreainvestment.com:29443"
KIS_TOKEN_PATH = "/oauth2/tokenP"


class KISHttpClient(Protocol):
    def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        json_payload: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> tuple[int, dict[str, Any]]:
        """Return an HTTP status code and JSON response body."""


@dataclass(frozen=True)
class KISPaperAccountId:
    cano: str
    product_code: str

    @classmethod
    def parse(cls, account_no: str) -> "KISPaperAccountId":
        cleaned = account_no.strip().replace("-", "")
        if len(cleaned) != 10 or not cleaned.isdigit():
            raise ValueError("KIS_ACCOUNT_NO must be 10 digits or formatted as 8 digits-2 digits.")
        return cls(cano=cleaned[:8], product_code=cleaned[8:])

    @property
    def normalized(self) -> str:
        return f"{self.cano}-{self.product_code}"


@dataclass(frozen=True)
class KISPaperCredentials:
    app_key: str
    app_secret: str
    account_id: KISPaperAccountId

    @classmethod
    def from_settings(cls, settings: Any) -> "KISPaperCredentials":
        missing = [
            name
            for name, value in {
                "KIS_APP_KEY": settings.kis_app_key,
                "KIS_APP_SECRET": settings.kis_app_secret,
                "KIS_ACCOUNT_NO": settings.kis_account_no,
            }.items()
            if not value
        ]
        if missing:
            raise ValueError(f"Missing KIS paper credentials: {', '.join(missing)}")

        return cls(
            app_key=str(settings.kis_app_key),
            app_secret=str(settings.kis_app_secret),
            account_id=KISPaperAccountId.parse(str(settings.kis_account_no)),
        )

    def secret_values(self) -> list[str]:
        return [self.app_key, self.app_secret, self.account_id.normalized]


@dataclass(frozen=True)
class KISPaperToken:
    access_token: str
    token_type: str
    expires_in_seconds: int | None

    @property
    def redacted(self) -> str:
        if len(self.access_token) <= 8:
            return "[REDACTED]"
        return f"{self.access_token[:4]}...[REDACTED]...{self.access_token[-4:]}"


class RequestsKISHttpClient:
    def request_json(
        self,
        method: str,
        url: str,
        *,
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


class KISPaperClient:
    """Minimal Korea Investment mock-trading client.

    The base URL is intentionally fixed to the mock-investment host. This class
    does not expose live-order behavior.
    """

    def __init__(
        self,
        credentials: KISPaperCredentials,
        http_client: KISHttpClient | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.credentials = credentials
        self.http_client = http_client or RequestsKISHttpClient()
        self.timeout_seconds = timeout_seconds
        self.base_url = KIS_PAPER_BASE_URL

    def issue_access_token(self) -> KISPaperToken:
        status_code, payload = self.http_client.request_json(
            "POST",
            f"{self.base_url}{KIS_TOKEN_PATH}",
            headers={"content-type": "application/json; charset=utf-8"},
            json_payload={
                "grant_type": "client_credentials",
                "appkey": self.credentials.app_key,
                "appsecret": self.credentials.app_secret,
            },
            timeout_seconds=self.timeout_seconds,
        )

        token = payload.get("access_token")
        if status_code != 200 or not isinstance(token, str) or not token:
            message = str(payload.get("msg1") or payload.get("error_description") or "")
            detail = f"KIS paper token request failed: HTTP {status_code}"
            if message:
                detail = f"{detail}, {_redact(message, self.credentials.secret_values())}"
            raise RuntimeError(detail)

        return KISPaperToken(
            access_token=token,
            token_type=str(payload.get("token_type") or "Bearer"),
            expires_in_seconds=_optional_int(payload.get("expires_in")),
        )


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError):
        return None


def _redact(message: str, secrets: list[str]) -> str:
    cleaned = message
    for secret in secrets:
        if secret:
            cleaned = cleaned.replace(secret, "[REDACTED]")
    return cleaned
