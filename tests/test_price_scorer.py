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
    assert score_frame["total_score"].between(0, 100).all()
    assert "rsi_recovery_zone" in score_frame.loc[0, "score_reason"]
    assert score_frame.loc[0, "financial_reason"] == "no_financial_feature_available"


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
