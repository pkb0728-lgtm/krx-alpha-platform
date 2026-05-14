import re
from dataclasses import dataclass
from datetime import date
from html import unescape
from typing import Any, Protocol

import pandas as pd

from krx_alpha.contracts.news_contract import validate_news_frame

NAVER_NEWS_SEARCH_URL = "https://openapi.naver.com/v1/search/news.json"

TICKER_TO_DEFAULT_QUERY = {
    "005930": "Samsung Electronics",
    "000660": "SK hynix",
    "005380": "Hyundai Motor",
    "035420": "NAVER",
    "035720": "Kakao",
}

STANDARD_NEWS_COLUMNS = [
    "date",
    "ticker",
    "query",
    "title",
    "description",
    "link",
    "originallink",
    "published_at",
    "source",
    "collected_at",
]


class NewsJsonProvider(Protocol):
    def __call__(self, query: str, display: int) -> dict[str, Any]:
        """Return a Naver news search JSON response."""


@dataclass(frozen=True)
class NewsSearchRequest:
    ticker: str
    query: str
    start_date: date
    end_date: date
    display: int = 10
    demo: bool = True

    @classmethod
    def from_strings(
        cls,
        ticker: str,
        start_date: str,
        end_date: str,
        query: str | None = None,
        display: int = 10,
        demo: bool = True,
    ) -> "NewsSearchRequest":
        normalized_ticker = ticker.zfill(6)
        return cls(
            ticker=normalized_ticker,
            query=query or default_news_query(normalized_ticker),
            start_date=date.fromisoformat(start_date),
            end_date=date.fromisoformat(end_date),
            display=display,
            demo=demo,
        )

    @property
    def compact_start_date(self) -> str:
        return self.start_date.strftime("%Y%m%d")

    @property
    def compact_end_date(self) -> str:
        return self.end_date.strftime("%Y%m%d")


class NaverNewsCollector:
    source_name = "naver_news"
    demo_source_name = "naver_news_demo"

    def __init__(
        self,
        client_id: str | None = None,
        client_secret: str | None = None,
        provider: NewsJsonProvider | None = None,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.provider = provider

    def collect(self, request: NewsSearchRequest) -> pd.DataFrame:
        if request.demo:
            payload = _demo_news_payload(request)
            source = self.demo_source_name
        else:
            if not self.client_id or not self.client_secret:
                raise ValueError("NAVER_CLIENT_ID and NAVER_CLIENT_SECRET must be set.")
            provider = self.provider or _naver_news_provider(self.client_id, self.client_secret)
            payload = provider(request.query, request.display)
            source = self.source_name

        frame = _normalize_news_payload(payload, request, source)
        validate_news_frame(frame)
        return frame


def default_news_query(ticker: str) -> str:
    return TICKER_TO_DEFAULT_QUERY.get(ticker.zfill(6), ticker.zfill(6))


def _naver_news_provider(client_id: str, client_secret: str) -> NewsJsonProvider:
    def provider(query: str, display: int) -> dict[str, Any]:
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError(
                "requests is not installed. Run: python -m pip install -e .[data]"
            ) from exc

        response = requests.get(
            NAVER_NEWS_SEARCH_URL,
            params={"query": query, "display": str(display), "start": "1", "sort": "date"},
            headers={
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("Naver news response is not a JSON object.")
        return data

    return provider


def _normalize_news_payload(
    payload: dict[str, Any],
    request: NewsSearchRequest,
    source: str,
) -> pd.DataFrame:
    items = payload.get("items", [])
    if not isinstance(items, list) or not items:
        raise ValueError("Naver news provider returned no news items.")

    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue

        published_at = pd.to_datetime(item.get("pubDate"), errors="coerce", utc=True)
        if pd.isna(published_at):
            published_at = pd.Timestamp.now(tz="UTC")
        news_date = published_at.date()
        if news_date < request.start_date or news_date > request.end_date:
            if request.demo:
                continue

        rows.append(
            {
                "date": news_date,
                "ticker": request.ticker,
                "query": request.query,
                "title": _clean_html(str(item.get("title", ""))),
                "description": _clean_html(str(item.get("description", ""))),
                "link": str(item.get("link", "")),
                "originallink": str(item.get("originallink", "")),
                "published_at": published_at,
                "source": source,
                "collected_at": pd.Timestamp.now(tz="UTC"),
            }
        )

    if not rows:
        raise ValueError("No news rows remained after normalization.")
    return pd.DataFrame(rows, columns=STANDARD_NEWS_COLUMNS).sort_values(
        ["date", "published_at"],
        ascending=[True, False],
    )


def _clean_html(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", "", value)
    return unescape(without_tags).strip()


def _demo_news_payload(request: NewsSearchRequest) -> dict[str, Any]:
    first_date = pd.Timestamp(request.start_date).strftime("%a, %d %b %Y 09:00:00 +0900")
    second_date = pd.Timestamp(request.end_date).strftime("%a, %d %b %Y 15:30:00 +0900")
    return {
        "items": [
            {
                "title": f"{request.query} earnings outlook improves",
                "description": "Analysts cite stronger memory demand and operating momentum.",
                "link": "https://example.com/news/positive",
                "originallink": "https://example.com/original/positive",
                "pubDate": first_date,
            },
            {
                "title": f"{request.query} shares face short term volatility",
                "description": "Market participants watch currency and demand uncertainty.",
                "link": "https://example.com/news/risk",
                "originallink": "https://example.com/original/risk",
                "pubDate": second_date,
            },
        ]
    }
