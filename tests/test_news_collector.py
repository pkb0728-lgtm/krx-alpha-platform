from typing import Any

from krx_alpha.collectors.news_collector import NaverNewsCollector, NewsSearchRequest


def test_news_collector_normalizes_naver_payload() -> None:
    def provider(query: str, display: int) -> dict[str, Any]:
        assert query == "Samsung Electronics"
        assert display == 1
        return {
            "items": [
                {
                    "title": "<b>Samsung</b> earnings improve",
                    "description": "Strong demand supports profit growth.",
                    "link": "https://example.com/news",
                    "originallink": "https://example.com/original",
                    "pubDate": "Wed, 31 Jan 2024 09:00:00 +0900",
                }
            ]
        }

    request = NewsSearchRequest.from_strings(
        ticker="005930",
        start_date="2024-01-01",
        end_date="2024-01-31",
        query="Samsung Electronics",
        display=1,
        demo=False,
    )
    frame = NaverNewsCollector(
        client_id="client-id",
        client_secret="client-secret",
        provider=provider,
    ).collect(request)

    assert len(frame) == 1
    assert frame.loc[0, "ticker"] == "005930"
    assert frame.loc[0, "title"] == "Samsung earnings improve"
    assert frame.loc[0, "source"] == "naver_news"


def test_news_collector_demo_mode_creates_rows() -> None:
    request = NewsSearchRequest.from_strings(
        ticker="005930",
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    frame = NaverNewsCollector().collect(request)

    assert not frame.empty
    assert frame["ticker"].eq("005930").all()
    assert frame["source"].eq("naver_news_demo").all()
