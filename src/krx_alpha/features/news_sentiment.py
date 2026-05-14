import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

import numpy as np
import pandas as pd

from krx_alpha.contracts.news_contract import validate_news_frame, validate_news_sentiment_frame

POSITIVE_TERMS = (
    "beat",
    "growth",
    "improves",
    "improved",
    "strong",
    "rebound",
    "upgrade",
    "record",
    "profit",
    "momentum",
    "demand",
)

NEGATIVE_TERMS = (
    "risk",
    "weak",
    "loss",
    "lawsuit",
    "downgrade",
    "uncertainty",
    "volatility",
    "recall",
    "decline",
    "miss",
)

NEWS_SENTIMENT_COLUMNS = [
    "date",
    "as_of_date",
    "ticker",
    "news_count",
    "positive_news_count",
    "negative_news_count",
    "sentiment_score",
    "news_score",
    "news_reason",
    "top_headline",
    "summary",
    "source",
    "feature_created_at",
]


class NewsSentimentProvider(Protocol):
    def __call__(self, articles: list[dict[str, str]]) -> dict[str, Any]:
        """Return summary and sentiment for a group of news articles."""


@dataclass(frozen=True)
class NewsSentimentConfig:
    use_gemini: bool = False
    allow_rule_fallback: bool = True
    gemini_model: str = "auto"


class NewsSentimentFeatureBuilder:
    source_name = "news_sentiment"
    gemini_source_name = "gemini_news_sentiment"

    def __init__(
        self,
        api_key: str | None = None,
        provider: NewsSentimentProvider | None = None,
        config: NewsSentimentConfig | None = None,
    ) -> None:
        self.api_key = api_key
        self.config = config or NewsSentimentConfig()
        self.provider = provider

    def build(self, news_frame: Any) -> pd.DataFrame:
        frame = news_frame.copy()
        validate_news_frame(frame)
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)
        frame["published_at"] = pd.to_datetime(frame["published_at"], utc=True)

        rows = []
        for (ticker, current_date), group in frame.groupby(["ticker", "date"], sort=True):
            analysis, source = self._analyze_group(group)
            sentiment_score = float(np.clip(analysis["sentiment_score"], -1, 1))
            positive_count = int(analysis["positive_news_count"])
            negative_count = int(analysis["negative_news_count"])
            news_score = _news_score(sentiment_score, int(len(group)))
            rows.append(
                {
                    "date": current_date,
                    "as_of_date": current_date,
                    "ticker": ticker,
                    "news_count": int(len(group)),
                    "positive_news_count": positive_count,
                    "negative_news_count": negative_count,
                    "sentiment_score": sentiment_score,
                    "news_score": news_score,
                    "news_reason": _news_reason(sentiment_score, int(len(group))),
                    "top_headline": str(analysis["top_headline"]),
                    "summary": str(analysis["summary"]),
                    "source": source,
                    "feature_created_at": pd.Timestamp.now(tz="UTC"),
                }
            )

        sentiment_frame = pd.DataFrame(rows, columns=NEWS_SENTIMENT_COLUMNS)
        validate_news_sentiment_frame(sentiment_frame)
        return sentiment_frame

    def _analyze_group(self, group: pd.DataFrame) -> tuple[dict[str, Any], str]:
        articles = _articles_from_group(group)
        if self.config.use_gemini:
            provider = self.provider or self._default_gemini_provider()
            try:
                analysis = provider(articles)
                return _normalize_ai_analysis(analysis, articles), self.gemini_source_name
            except Exception:
                if not self.config.allow_rule_fallback:
                    raise

        return _rule_based_analysis(articles), self.source_name

    def _default_gemini_provider(self) -> NewsSentimentProvider:
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY must be set when use_gemini=True.")
        return GeminiNewsSentimentProvider(
            api_key=self.api_key,
            model=self.config.gemini_model,
        )


class GeminiNewsSentimentProvider:
    def __init__(self, api_key: str, model: str) -> None:
        self.api_key = api_key
        self.model = model

    def __call__(self, articles: list[dict[str, str]]) -> dict[str, Any]:
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError(
                "requests is not installed. Run: python -m pip install -e .[data]"
            ) from exc

        model = _resolve_gemini_model(requests, self.api_key, self.model)
        endpoint = (
            f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
        )
        response = requests.post(
            endpoint,
            params={"key": self.api_key},
            json={"contents": [{"parts": [{"text": _gemini_prompt(articles)}]}]},
            timeout=20,
        )
        payload = response.json()
        if response.status_code >= 400:
            message = str(payload.get("error", {}).get("message", "Gemini request failed."))
            raise RuntimeError(message)

        text = _gemini_text(payload)
        return _parse_json_object(text)


def _resolve_gemini_model(requests_module: Any, api_key: str, requested_model: str) -> str:
    if requested_model != "auto":
        return requested_model.removeprefix("models/")

    response = requests_module.get(
        "https://generativelanguage.googleapis.com/v1beta/models",
        params={"key": api_key},
        timeout=10,
    )
    payload = response.json()
    if response.status_code >= 400:
        message = str(payload.get("error", {}).get("message", "Gemini model lookup failed."))
        raise RuntimeError(message)

    models = payload.get("models", [])
    if not isinstance(models, list):
        raise ValueError("Gemini model list response did not contain models.")

    candidates: list[str] = []
    for model in models:
        if not isinstance(model, dict):
            continue
        methods = model.get("supportedGenerationMethods", [])
        name = str(model.get("name", "")).removeprefix("models/")
        if "generateContent" in methods and "flash" in name:
            candidates.append(name)

    if not candidates:
        raise ValueError("No Gemini flash model supporting generateContent was found.")

    priority_terms = ("2.5-flash", "2.0-flash", "1.5-flash")
    for priority in priority_terms:
        for candidate in candidates:
            if priority in candidate:
                return candidate
    return candidates[0]


def _articles_from_group(group: pd.DataFrame) -> list[dict[str, str]]:
    sorted_group = group.sort_values("published_at", ascending=False)
    return [
        {
            "title": str(row["title"]),
            "description": str(row["description"]),
            "link": str(row["link"]),
        }
        for _, row in sorted_group.iterrows()
    ]


def _rule_based_analysis(articles: list[dict[str, str]]) -> dict[str, Any]:
    article_scores = [_article_sentiment_score(article) for article in articles]
    sentiment_score = float(np.mean(article_scores)) if article_scores else 0.0
    positive_count = sum(score > 0.1 for score in article_scores)
    negative_count = sum(score < -0.1 for score in article_scores)
    top_headline = articles[0]["title"] if articles else ""
    return {
        "summary": _rule_summary(articles, sentiment_score),
        "sentiment_score": sentiment_score,
        "positive_news_count": positive_count,
        "negative_news_count": negative_count,
        "top_headline": top_headline,
    }


def _article_sentiment_score(article: dict[str, str]) -> float:
    text = f"{article['title']} {article['description']}".lower()
    positive_hits = sum(1 for term in POSITIVE_TERMS if term in text)
    negative_hits = sum(1 for term in NEGATIVE_TERMS if term in text)
    raw_score = (positive_hits - negative_hits) / max(positive_hits + negative_hits, 1)
    return float(np.clip(raw_score, -1, 1))


def _rule_summary(articles: list[dict[str, str]], sentiment_score: float) -> str:
    if not articles:
        return "No news articles were available."
    if sentiment_score > 0.15:
        tone = "positive"
    elif sentiment_score < -0.15:
        tone = "negative"
    else:
        tone = "mixed"
    return (
        f"{len(articles)} article(s) show a {tone} news tone. Top headline: {articles[0]['title']}"
    )


def _normalize_ai_analysis(
    analysis: dict[str, Any],
    articles: list[dict[str, str]],
) -> dict[str, Any]:
    rule_fallback = _rule_based_analysis(articles)
    return {
        "summary": str(analysis.get("summary") or rule_fallback["summary"]),
        "sentiment_score": _safe_float(analysis.get("sentiment_score"), 0.0),
        "positive_news_count": _safe_int(
            analysis.get("positive_news_count"),
            int(rule_fallback["positive_news_count"]),
        ),
        "negative_news_count": _safe_int(
            analysis.get("negative_news_count"),
            int(rule_fallback["negative_news_count"]),
        ),
        "top_headline": str(analysis.get("top_headline") or rule_fallback["top_headline"]),
    }


def _news_score(sentiment_score: float, news_count: int) -> float:
    volume_bonus = min(max(news_count - 1, 0) * 2.0, 8.0)
    return float(np.clip(50.0 + sentiment_score * 35.0 + volume_bonus, 0, 100))


def _news_reason(sentiment_score: float, news_count: int) -> str:
    reasons = []
    if sentiment_score > 0.15:
        reasons.append("news_sentiment_positive")
    elif sentiment_score < -0.15:
        reasons.append("news_sentiment_negative")
    else:
        reasons.append("news_sentiment_neutral")

    if news_count >= 5:
        reasons.append("news_volume_elevated")
    return ", ".join(reasons)


def _gemini_prompt(articles: list[dict[str, str]]) -> str:
    limited_articles = articles[:8]
    article_lines = "\n".join(
        f"{index}. title={article['title']} description={article['description']}"
        for index, article in enumerate(limited_articles, start=1)
    )
    return (
        "Analyze these stock-related news snippets for investment decision support. "
        "Return only JSON with keys summary, sentiment_score, positive_news_count, "
        "negative_news_count, top_headline. sentiment_score must be between -1 and 1.\n\n"
        f"{article_lines}"
    )


def _gemini_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates", [])
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("Gemini response did not contain candidates.")
    content = candidates[0].get("content", {})
    parts = content.get("parts", [])
    if not isinstance(parts, list) or not parts:
        raise ValueError("Gemini response did not contain text parts.")
    return str(parts[0].get("text", ""))


def _parse_json_object(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        stripped = stripped.replace("json\n", "", 1).strip()
    match = re.search(r"\{.*\}", stripped, flags=re.DOTALL)
    candidate = match.group(0) if match else stripped
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("Gemini response JSON is not an object.")
    return parsed


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
