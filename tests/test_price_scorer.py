import pandas as pd

from krx_alpha.scoring.price_scorer import PriceScorer


def test_price_scorer_creates_scores_and_labels() -> None:
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-30", "2024-01-31"],
            "as_of_date": ["2024-01-30", "2024-01-31"],
            "ticker": ["005930", "005930"],
            "close": [74300, 72700],
            "volume": [1000, 1200],
            "trading_value": [74300000, 87240000],
            "return_1d": [0.01, -0.02],
            "ma_5": [74000, 73780],
            "ma_20": [74285, 74070],
            "close_to_ma_5": [0.004, -0.015],
            "close_to_ma_20": [0.001, -0.018],
            "volume_change_5d": [0.2, 0.1],
            "trading_value_change_5d": [0.35, 0.05],
            "range_pct": [0.02, 0.03],
            "volatility_5d": [0.01, 0.02],
            "volatility_20d": [0.015, 0.018],
            "rsi_14": [53, 48],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 2,
        }
    )

    score_frame = PriceScorer().score(feature_frame)

    assert len(score_frame) == 2
    assert set(score_frame["signal_label"]).issubset({"watch_buy", "watch", "neutral", "avoid"})
    assert score_frame["technical_score"].between(0, 100).all()
    assert score_frame["risk_score"].between(0, 100).all()
    assert score_frame["financial_score"].eq(50.0).all()
    assert score_frame["event_score"].eq(50.0).all()
    assert score_frame["event_risk_flag"].eq(False).all()
    assert score_frame["flow_score"].eq(50.0).all()
    assert score_frame["news_score"].eq(50.0).all()
    assert score_frame["total_score"].between(0, 100).all()
    assert "rsi_recovery_zone" in score_frame.loc[0, "score_reason"]
    assert score_frame.loc[0, "financial_reason"] == "no_financial_feature_available"
    assert score_frame.loc[0, "event_reason"] == "no_disclosure_event_available"
    assert score_frame.loc[0, "flow_reason"] == "no_investor_flow_available"
    assert score_frame.loc[0, "news_reason"] == "no_news_sentiment_available"


def test_price_scorer_blends_financial_score() -> None:
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "close": [72700],
            "volume": [1200],
            "trading_value": [87240000],
            "return_1d": [-0.02],
            "ma_5": [73780],
            "ma_20": [74070],
            "close_to_ma_5": [-0.015],
            "close_to_ma_20": [-0.018],
            "volume_change_5d": [0.1],
            "trading_value_change_5d": [0.05],
            "range_pct": [0.03],
            "volatility_5d": [0.02],
            "volatility_20d": [0.018],
            "rsi_14": [48],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    financial_feature_frame = pd.DataFrame(
        {
            "corp_code": ["00126380"],
            "ticker": ["005930"],
            "bsns_year": ["2023"],
            "reprt_code": ["11011"],
            "fs_div": ["CFS"],
            "revenue": [1200.0],
            "operating_income": [240.0],
            "net_income": [180.0],
            "total_assets": [3000.0],
            "total_liabilities": [900.0],
            "total_equity": [2100.0],
            "revenue_growth": [0.2],
            "operating_margin": [0.2],
            "net_margin": [0.15],
            "debt_ratio": [0.43],
            "roe": [0.086],
            "financial_score": [95.0],
            "financial_reason": ["revenue_growth_positive, operating_margin_healthy"],
            "source": ["opendart_demo"],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    score_frame = PriceScorer().score(feature_frame, financial_feature_frame)

    assert score_frame.loc[0, "financial_score"] == 95.0
    assert "operating_margin_healthy" in score_frame.loc[0, "financial_reason"]
    assert score_frame.loc[0, "total_score"] > 50.0


def test_price_scorer_prefers_consolidated_financial_score() -> None:
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "close": [72700],
            "volume": [1200],
            "trading_value": [87240000],
            "return_1d": [-0.02],
            "ma_5": [73780],
            "ma_20": [74070],
            "close_to_ma_5": [-0.015],
            "close_to_ma_20": [-0.018],
            "volume_change_5d": [0.1],
            "trading_value_change_5d": [0.05],
            "range_pct": [0.03],
            "volatility_5d": [0.02],
            "volatility_20d": [0.018],
            "rsi_14": [48],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    financial_feature_frame = pd.DataFrame(
        {
            "corp_code": ["00126380", "00126380"],
            "ticker": ["005930", "005930"],
            "bsns_year": ["2023", "2023"],
            "reprt_code": ["11011", "11011"],
            "fs_div": ["CFS", "OFS"],
            "revenue": [1200.0, 1100.0],
            "operating_income": [240.0, -10.0],
            "net_income": [180.0, 120.0],
            "total_assets": [3000.0, 2800.0],
            "total_liabilities": [900.0, 1000.0],
            "total_equity": [2100.0, 1800.0],
            "revenue_growth": [0.2, -0.1],
            "operating_margin": [0.2, -0.01],
            "net_margin": [0.15, 0.11],
            "debt_ratio": [0.43, 0.55],
            "roe": [0.086, 0.067],
            "financial_score": [95.0, 55.0],
            "financial_reason": ["consolidated", "separate"],
            "source": ["opendart"] * 2,
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 2,
        }
    )

    score_frame = PriceScorer().score(feature_frame, financial_feature_frame)

    assert score_frame.loc[0, "financial_score"] == 95.0
    assert score_frame.loc[0, "financial_reason"] == "consolidated"


def test_price_scorer_blends_disclosure_event_score() -> None:
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "close": [72700],
            "volume": [1200],
            "trading_value": [87240000],
            "return_1d": [-0.02],
            "ma_5": [73780],
            "ma_20": [74070],
            "close_to_ma_5": [-0.015],
            "close_to_ma_20": [-0.018],
            "volume_change_5d": [0.1],
            "trading_value_change_5d": [0.05],
            "range_pct": [0.03],
            "volatility_5d": [0.02],
            "volatility_20d": [0.018],
            "rsi_14": [48],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    event_feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "corp_code": ["00126380"],
            "ticker": ["005930"],
            "report_nm": ["Capital increase decision"],
            "rcept_no": ["20240131000001"],
            "event_category": ["capital_raise_risk"],
            "event_score": [20.0],
            "event_risk_flag": [True],
            "event_reason": ["disclosure_risk_capital_increase"],
            "source": ["opendart_demo"],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    score_frame = PriceScorer().score(feature_frame, event_feature_frame=event_feature_frame)

    assert score_frame.loc[0, "event_score"] == 20.0
    assert bool(score_frame.loc[0, "event_risk_flag"]) is True
    assert score_frame.loc[0, "event_reason"] == "disclosure_risk_capital_increase"


def test_price_scorer_blends_investor_flow_score() -> None:
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "close": [72700],
            "volume": [1200],
            "trading_value": [87240000],
            "return_1d": [-0.02],
            "ma_5": [73780],
            "ma_20": [74070],
            "close_to_ma_5": [-0.015],
            "close_to_ma_20": [-0.018],
            "volume_change_5d": [0.1],
            "trading_value_change_5d": [0.05],
            "range_pct": [0.03],
            "volatility_5d": [0.02],
            "volatility_20d": [0.018],
            "rsi_14": [48],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    flow_feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "individual_net_buy_value": [-8_000_000_000.0],
            "foreign_net_buy_value": [5_000_000_000.0],
            "institution_net_buy_value": [3_000_000_000.0],
            "smart_money_net_buy_value": [8_000_000_000.0],
            "foreign_net_buy_value_5d": [25_000_000_000.0],
            "institution_net_buy_value_5d": [15_000_000_000.0],
            "smart_money_net_buy_value_5d": [40_000_000_000.0],
            "flow_score": [85.0],
            "flow_reason": ["strong_smart_money_inflow"],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    score_frame = PriceScorer().score(feature_frame, flow_feature_frame=flow_feature_frame)

    assert score_frame.loc[0, "flow_score"] == 85.0
    assert score_frame.loc[0, "flow_reason"] == "strong_smart_money_inflow"


def test_price_scorer_blends_news_sentiment_score() -> None:
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "close": [72700],
            "volume": [1200],
            "trading_value": [87240000],
            "return_1d": [-0.02],
            "ma_5": [73780],
            "ma_20": [74070],
            "close_to_ma_5": [-0.015],
            "close_to_ma_20": [-0.018],
            "volume_change_5d": [0.1],
            "trading_value_change_5d": [0.05],
            "range_pct": [0.03],
            "volatility_5d": [0.02],
            "volatility_20d": [0.018],
            "rsi_14": [48],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    news_feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "news_count": [3],
            "positive_news_count": [2],
            "negative_news_count": [0],
            "sentiment_score": [0.55],
            "news_score": [73.25],
            "news_reason": ["news_sentiment_positive"],
            "top_headline": ["Strong demand improves outlook"],
            "summary": ["Positive news tone."],
            "source": ["news_sentiment"],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    score_frame = PriceScorer().score(feature_frame, news_feature_frame=news_feature_frame)

    assert score_frame.loc[0, "news_score"] == 73.25
    assert score_frame.loc[0, "news_reason"] == "news_sentiment_positive"
