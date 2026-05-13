import pandas as pd

from krx_alpha.reports.daily_report import DailyReportGenerator


def test_daily_report_generator_creates_markdown() -> None:
    score_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "technical_score": [62.0],
            "risk_score": [70.17],
            "financial_score": [82.0],
            "event_score": [50.0],
            "event_risk_flag": [False],
            "total_score": [64.45],
            "signal_label": ["watch"],
            "score_reason": ["rsi_recovery_zone, trading_value_increase"],
            "financial_reason": ["revenue_growth_positive, operating_margin_healthy"],
            "event_reason": ["disclosure_routine_report"],
            "scored_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "close": [72700],
            "volume": [15703560],
            "trading_value": [1141630812000],
            "return_1d": [-0.0215],
            "ma_5": [73780],
            "ma_20": [74070],
            "close_to_ma_5": [-0.0146],
            "close_to_ma_20": [-0.0185],
            "volume_change_5d": [0.42],
            "trading_value_change_5d": [0.38],
            "range_pct": [0.03],
            "volatility_5d": [0.013],
            "volatility_20d": [0.018],
            "rsi_14": [48.03],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    report = DailyReportGenerator().generate(score_frame, feature_frame)

    assert "# Daily Stock Report: 005930" in report
    assert "Signal: `watch`" in report
    assert "`rsi_recovery_zone`" in report
    assert "Financial score" in report
    assert "`revenue_growth_positive`" in report
    assert "Event score" in report
    assert "`disclosure_routine_report`" in report
    assert "This report is decision-support output" in report
