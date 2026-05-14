from dataclasses import dataclass
from typing import Any, Protocol, cast

KIS_PAPER_BASE_URL = "https://openapivts.koreainvestment.com:29443"
KIS_TOKEN_PATH = "/oauth2/tokenP"
KIS_DOMESTIC_BALANCE_PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
KIS_PAPER_BALANCE_TR_ID = "VTTC8434R"


class KISHttpClient(Protocol):
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


@dataclass(frozen=True)
class KISPaperHolding:
    ticker: str
    name: str
    quantity: int
    orderable_quantity: int
    average_price: float
    current_price: float
    evaluation_amount: float
    profit_loss_amount: float
    profit_loss_rate: float


@dataclass(frozen=True)
class KISPaperBalance:
    account: str
    cash_amount: float
    total_evaluation_amount: float
    stock_evaluation_amount: float
    purchase_amount: float
    profit_loss_amount: float
    profit_loss_rate: float
    holdings: list[KISPaperHolding]


class RequestsKISHttpClient:
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

    def inquire_balance(self, token: KISPaperToken) -> KISPaperBalance:
        status_code, payload = self.http_client.request_json(
            "GET",
            f"{self.base_url}{KIS_DOMESTIC_BALANCE_PATH}",
            params={
                "CANO": self.credentials.account_id.cano,
                "ACNT_PRDT_CD": self.credentials.account_id.product_code,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "01",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "01",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
            headers={
                "content-type": "application/json; charset=utf-8",
                "authorization": f"{token.token_type} {token.access_token}",
                "appkey": self.credentials.app_key,
                "appsecret": self.credentials.app_secret,
                "tr_id": KIS_PAPER_BALANCE_TR_ID,
                "custtype": "P",
            },
            timeout_seconds=self.timeout_seconds,
        )

        if status_code != 200 or str(payload.get("rt_cd", "0")) not in {"0", ""}:
            message = str(payload.get("msg1") or payload.get("error_description") or "")
            detail = f"KIS paper balance request failed: HTTP {status_code}"
            if message:
                detail = f"{detail}, {_redact(message, self.credentials.secret_values())}"
            raise RuntimeError(detail)

        return _parse_balance_payload(payload, self.credentials.account_id.normalized)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError):
        return None


def _parse_balance_payload(payload: dict[str, Any], account: str) -> KISPaperBalance:
    holdings_payload = payload.get("output1")
    summary_payload = payload.get("output2")
    holdings_rows = holdings_payload if isinstance(holdings_payload, list) else []
    summary_rows = summary_payload if isinstance(summary_payload, list) else []
    summary = summary_rows[0] if summary_rows and isinstance(summary_rows[0], dict) else {}

    holdings = [
        KISPaperHolding(
            ticker=str(row.get("pdno") or "").zfill(6),
            name=str(row.get("prdt_name") or ""),
            quantity=_safe_int(row.get("hldg_qty")),
            orderable_quantity=_safe_int(row.get("ord_psbl_qty")),
            average_price=_safe_float(row.get("pchs_avg_pric")),
            current_price=_safe_float(row.get("prpr")),
            evaluation_amount=_safe_float(row.get("evlu_amt")),
            profit_loss_amount=_safe_float(row.get("evlu_pfls_amt")),
            profit_loss_rate=_safe_float(row.get("evlu_pfls_rt")),
        )
        for row in holdings_rows
        if isinstance(row, dict) and _safe_int(row.get("hldg_qty")) > 0
    ]

    return KISPaperBalance(
        account=account,
        cash_amount=_first_float(summary, ["dnca_tot_amt", "nass_amt"]),
        total_evaluation_amount=_first_float(summary, ["tot_evlu_amt", "asst_icdc_amt"]),
        stock_evaluation_amount=_first_float(summary, ["scts_evlu_amt"]),
        purchase_amount=_first_float(summary, ["pchs_amt_smtl_amt"]),
        profit_loss_amount=_first_float(summary, ["evlu_pfls_smtl_amt"]),
        profit_loss_rate=_first_float(summary, ["asst_icdc_erng_rt", "evlu_pfls_rt"]),
        holdings=holdings,
    )


def _first_float(row: dict[str, Any], keys: list[str]) -> float:
    for key in keys:
        if key in row:
            return _safe_float(row.get(key))
    return 0.0


def _safe_int(value: object) -> int:
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return 0


def _safe_float(value: object) -> float:
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _redact(message: str, secrets: list[str]) -> str:
    cleaned = message
    for secret in secrets:
        if secret:
            cleaned = cleaned.replace(secret, "[REDACTED]")
    return cleaned
