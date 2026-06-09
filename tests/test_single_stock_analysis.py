from datetime import date

import pandas as pd

from krx_alpha.reports.single_stock_analysis import (
    build_single_stock_analysis,
    format_single_stock_analysis_message,
)


def test_build_single_stock_analysis_creates_beginner_friendly_sections() -> None:
    score_frame = pd.DataFrame(
        {
            "date": [date(2024, 1, 31)],
            "ticker": ["005930"],
            "technical_score": [65.0],
            "risk_score": [70.0],
            "financial_score": [50.0],
            "event_score": [50.0],
            "flow_score": [50.0],
            "news_score": [50.0],
            "macro_score": [50.0],
            "total_score": [60.0],
            "score_reason": ["close_above_ma5, trading_value_increase"],
            "financial_reason": ["no_financial_feature_available"],
            "event_reason": ["no_disclosure_event_available"],
            "flow_reason": ["no_investor_flow_available"],
            "news_reason": ["no_news_sentiment_available"],
            "macro_reason": ["no_macro_feature_available"],
        }
    )
    feature_frame = pd.DataFrame(
        {
            "date": [date(2024, 1, 31)],
            "ticker": ["005930"],
            "close": [73000],
            "return_1d": [0.01],
            "close_to_ma_5": [0.02],
            "close_to_ma_20": [0.03],
            "rsi_14": [55.0],
            "trading_value": [100_000_000_000],
            "trading_value_change_5d": [0.25],
            "volatility_5d": [0.02],
            "volatility_20d": [0.03],
        }
    )
    signal_frame = pd.DataFrame(
        {
            "date": [date(2024, 1, 31)],
            "ticker": ["005930"],
            "final_action": ["watch"],
            "confidence_score": [62.0],
            "market_regime": ["insufficient_data"],
            "risk_blocked": [False],
            "risk_flags": [""],
            "suggested_position_pct": [1.4],
            "signal_reason": ["close_above_ma5, trading_value_increase"],
        }
    )

    analysis = build_single_stock_analysis(
        stock_label="005930 삼성전자",
        query="삼성전자",
        matched_by="name",
        source="static_alias",
        start_date="2024-01-01",
        end_date="2024-01-31",
        score_frame=score_frame,
        feature_frame=feature_frame,
        signal_frame=signal_frame,
        data_quality_warning_count=0,
        data_quality_fail_count=0,
    )

    assert analysis.summary[0].label == "입력값"
    assert any(item.label == "기술적 점수" for item in analysis.score_breakdown)
    assert any("거래대금" in item.label for item in analysis.price_metrics)
    assert "최근 5일 기준 거래대금이 증가했습니다." in analysis.evidence[0].value
    assert any("120~180일" in note for note in analysis.beginner_notes)

    message = format_single_stock_analysis_message(
        analysis,
        signal_path="data/signals/final_signals_daily/005930.parquet",
        report_path="reports/daily/005930.md",
    )

    assert "KRX Alpha 단일 종목 상세 분석" in message
    assert "1. 한눈에 보는 결론" in message
    assert "2. 점수 분해" in message
    assert "3. 가격/기술 지표" in message
    assert "4. 판단 근거" in message
    assert "초보자용 해석 메모" in message
    assert "시그널 파일" in message
    assert "실제 주문은 보내지 않았습니다" in message
