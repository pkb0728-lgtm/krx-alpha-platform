from typing import Any

import pandas as pd

from krx_alpha.features.news_sentiment import (
    NewsSentimentConfig,
    NewsSentimentFeatureBuilder,
    _resolve_gemini_model,
)


def test_news_sentiment_builder_creates_rule_based_scores() -> None:
    news_frame = pd.DataFrame(
        {
            "date": ["2024-01-31", "2024-01-31"],
            "ticker": ["005930", "005930"],
            "query": ["Samsung"] * 2,
            "title": ["Strong profit growth", "Demand momentum improves"],
            "description": ["Analysts upgrade outlook.", "Memory demand is strong."],
            "link": ["https://example.com/a", "https://example.com/b"],
            "originallink": ["https://example.com/oa", "https://example.com/ob"],
            "published_at": [
                pd.Timestamp("2024-01-31T00:00:00Z"),
                pd.Timestamp("2024-01-31T01:00:00Z"),
            ],
            "source": ["naver_news_demo"] * 2,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 2,
        }
    )

    feature_frame = NewsSentimentFeatureBuilder().build(news_frame)

    assert len(feature_frame) == 1
    assert feature_frame.loc[0, "news_score"] > 50.0
    assert "news_sentiment_positive" in feature_frame.loc[0, "news_reason"]


def test_news_sentiment_builder_uses_ai_provider() -> None:
    news_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "ticker": ["005930"],
            "query": ["Samsung"],
            "title": ["Volatility risk rises"],
            "description": ["Uncertainty and weak demand are discussed."],
            "link": ["https://example.com/a"],
            "originallink": ["https://example.com/oa"],
            "published_at": [pd.Timestamp("2024-01-31T00:00:00Z")],
            "source": ["naver_news"],
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    def provider(articles: list[dict[str, str]]) -> dict[str, Any]:
        assert articles[0]["title"] == "Volatility risk rises"
        return {
            "summary": "AI summary",
            "sentiment_score": -0.6,
            "positive_news_count": 0,
            "negative_news_count": 1,
            "top_headline": "Volatility risk rises",
        }

    feature_frame = NewsSentimentFeatureBuilder(
        provider=provider,
        config=NewsSentimentConfig(use_gemini=True),
    ).build(news_frame)

    assert feature_frame.loc[0, "news_score"] < 50.0
    assert feature_frame.loc[0, "summary"] == "AI summary"
    assert feature_frame.loc[0, "source"] == "gemini_news_sentiment"


def test_resolve_gemini_model_prefers_flash_generate_content_model() -> None:
    class FakeResponse:
        status_code = 200

        def json(self) -> dict[str, object]:
            return {
                "models": [
                    {
                        "name": "models/text-embedding-004",
                        "supportedGenerationMethods": ["embedContent"],
                    },
                    {
                        "name": "models/gemini-2.0-flash",
                        "supportedGenerationMethods": ["generateContent"],
                    },
                ]
            }

    class FakeRequests:
        def get(
            self,
            url: str,
            *,
            params: dict[str, str],
            timeout: int,
        ) -> FakeResponse:
            assert "models" in url
            assert params["key"] == "key"
            assert timeout == 10
            return FakeResponse()

    assert _resolve_gemini_model(FakeRequests(), "key", "auto") == "gemini-2.0-flash"
