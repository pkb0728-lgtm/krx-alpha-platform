import pandas as pd

from krx_alpha.features.macro_features import MacroFeatureBuilder


def test_macro_feature_builder_scores_rising_rate_and_fx_pressure() -> None:
    dates = pd.date_range("2024-01-02", periods=8, freq="B").date
    rows: list[dict[str, object]] = []
    for index, current_date in enumerate(dates):
        rows.extend(
            [
                {
                    "date": current_date,
                    "series_id": "DGS10",
                    "series_name": "US 10Y",
                    "value": 4.40 + index * 0.04,
                    "source": "fred",
                    "collected_at": pd.Timestamp("2026-05-14T00:00:00Z"),
                },
                {
                    "date": current_date,
                    "series_id": "DFF",
                    "series_name": "Fed Funds",
                    "value": 5.33,
                    "source": "fred",
                    "collected_at": pd.Timestamp("2026-05-14T00:00:00Z"),
                },
                {
                    "date": current_date,
                    "series_id": "DEXKOUS",
                    "series_name": "USD/KRW",
                    "value": 1300.0 + index * 10.0,
                    "source": "fred",
                    "collected_at": pd.Timestamp("2026-05-14T00:00:00Z"),
                },
            ]
        )
    macro_frame = pd.DataFrame(rows)

    feature_frame = MacroFeatureBuilder().build(macro_frame)

    latest = feature_frame.sort_values("date").iloc[-1]
    assert len(feature_frame) == 8
    assert latest["macro_score"] < 50.0
    assert "fed_funds_rate_restrictive" in latest["macro_reason"]
    assert "us_10y_yield_rising" in latest["macro_reason"]
    assert "usdkrw_rising_fx_pressure" in latest["macro_reason"]


def test_macro_feature_builder_handles_missing_optional_series() -> None:
    macro_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=3, freq="B").date,
            "series_id": ["DGS10", "DGS10", "DGS10"],
            "series_name": ["US 10Y"] * 3,
            "value": [3.2, 3.1, 3.0],
            "source": ["fred"] * 3,
            "collected_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 3,
        }
    )

    feature_frame = MacroFeatureBuilder().build(macro_frame)

    assert len(feature_frame) == 3
    assert feature_frame["usdkrw"].isna().all()
    assert feature_frame["macro_score"].between(0, 100).all()
