from typing import Any

REQUIRED_NEWS_COLUMNS = {
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
}

REQUIRED_NEWS_SENTIMENT_COLUMNS = {
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
}


def validate_news_frame(frame: Any) -> None:
    missing_columns = REQUIRED_NEWS_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required news columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("News frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("News frame contains null dates.")

    if frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all() is False:
        raise ValueError("News frame ticker must be a six-digit code.")


def validate_news_sentiment_frame(frame: Any) -> None:
    missing_columns = REQUIRED_NEWS_SENTIMENT_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required news sentiment columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("News sentiment frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("News sentiment frame contains null dates.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("News sentiment frame contains duplicated date/ticker rows.")

    if frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all() is False:
        raise ValueError("News sentiment frame ticker must be a six-digit code.")

    if frame["sentiment_score"].dropna().between(-1, 1).all() is False:
        raise ValueError("sentiment_score values must be between -1 and 1.")

    if frame["news_score"].dropna().between(0, 100).all() is False:
        raise ValueError("news_score values must be between 0 and 100.")
