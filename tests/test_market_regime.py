import pandas as pd

from krx_alpha.features.price_features import PriceFeatureBuilder
from krx_alpha.regime.market_regime import MarketRegimeAnalyzer
from krx_alpha.reports.regime_report import MarketRegimeReportGenerator


def test_market_regime_analyzer_detects_bull_regime() -> None:
    feature_frame = _build_feature_frame([float(value) for value in range(100, 180)])

    regime_frame = MarketRegimeAnalyzer().analyze(feature_frame)
    latest = regime_frame.iloc[-1]

    assert latest["regime"] == "bull"
    assert latest["risk_level"] == "low"
    assert latest["regime_score"] > 60


def test_market_regime_analyzer_detects_high_volatility() -> None:
    feature_frame = _build_feature_frame([float(value) for value in range(100, 180)])
    feature_frame.loc[feature_frame.index[-1], "volatility_20d"] = 0.05

    regime_frame = MarketRegimeAnalyzer().analyze(feature_frame)
    latest = regime_frame.iloc[-1]

    assert latest["regime"] == "high_volatility"
    assert latest["risk_level"] == "high"


def test_market_regime_report_generator_creates_markdown() -> None:
    feature_frame = _build_feature_frame([float(value) for value in range(100, 180)])
    regime_frame = MarketRegimeAnalyzer().analyze(feature_frame)

    report = MarketRegimeReportGenerator().generate(regime_frame)

    assert "# Market Regime Report: 005930" in report
    assert "Regime: `bull`" in report
    assert "20-day return" in report


def _build_feature_frame(close_values: list[float]) -> pd.DataFrame:
    dates = pd.date_range("2024-01-02", periods=len(close_values), freq="D")
    close = pd.Series(close_values)
    processed_frame = pd.DataFrame(
        {
            "date": dates,
            "as_of_date": dates,
            "ticker": ["005930"] * len(close_values),
            "open": close,
            "high": close + 1,
            "low": close - 1,
            "close": close,
            "volume": [1000] * len(close_values),
            "trading_value": close * 1000,
            "trading_value_is_estimated": [False] * len(close_values),
            "return_1d": close.pct_change(),
            "log_return_1d": close.pct_change(),
            "range_pct": [0.02] * len(close_values),
            "change_rate": [0.0] * len(close_values),
            "source": ["test"] * len(close_values),
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * len(close_values),
            "processed_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * len(close_values),
        }
    )
    return PriceFeatureBuilder().build(processed_frame)
