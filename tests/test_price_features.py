import pandas as pd

from krx_alpha.features.price_features import PriceFeatureBuilder


def test_price_feature_builder_creates_technical_features() -> None:
    processed_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=25, freq="D"),
            "as_of_date": pd.date_range("2024-01-02", periods=25, freq="D"),
            "ticker": ["005930"] * 25,
            "open": range(100, 125),
            "high": range(102, 127),
            "low": range(99, 124),
            "close": range(101, 126),
            "volume": range(1000, 1025),
            "trading_value": range(101000, 126000, 1000),
            "trading_value_is_estimated": [False] * 25,
            "return_1d": [float("nan")] + [0.01] * 24,
            "log_return_1d": [float("nan")] + [0.00995] * 24,
            "range_pct": [0.03] * 25,
            "change_rate": [1.0] * 25,
            "source": ["test"] * 25,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 25,
            "processed_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 25,
        }
    )

    feature_frame = PriceFeatureBuilder().build(processed_frame)

    assert len(feature_frame) == 25
    assert "ma_5" in feature_frame.columns
    assert "ma_20" in feature_frame.columns
    assert "rsi_14" in feature_frame.columns
    assert feature_frame.loc[4, "ma_5"] == 103
    assert pd.notna(feature_frame.loc[19, "ma_20"])
    assert pd.notna(feature_frame.loc[14, "rsi_14"])
