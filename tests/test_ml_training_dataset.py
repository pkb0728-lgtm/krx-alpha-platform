import pandas as pd
import pytest

from krx_alpha.models.training_dataset import (
    MODEL_FEATURE_COLUMNS,
    MLTrainingDatasetBuilder,
    MLTrainingDatasetConfig,
)


def test_ml_training_dataset_builder_creates_forward_return_labels() -> None:
    feature_frame = _feature_frame(periods=10)
    price_frame = _processed_price_frame(periods=10)

    training_frame = MLTrainingDatasetBuilder(
        MLTrainingDatasetConfig(holding_days=2, minimum_forward_return=0.01)
    ).build(feature_frame, price_frame)

    assert len(training_frame) == 8
    assert training_frame.loc[0, "ticker"] == "005930"
    assert training_frame.loc[0, "label_end_date"] == pd.Timestamp("2024-01-03").date()
    assert training_frame.loc[0, "forward_return"] == pytest.approx(0.02)
    assert training_frame.loc[0, "target_positive_forward_return"] == 1
    assert training_frame["date"].max() == pd.Timestamp("2024-01-08").date()
    assert "future_close" in training_frame.columns


def test_ml_training_dataset_builder_can_drop_missing_feature_rows() -> None:
    feature_frame = _feature_frame(periods=10)
    feature_frame.loc[0, "rsi_14"] = float("nan")
    price_frame = _processed_price_frame(periods=10)

    training_frame = MLTrainingDatasetBuilder(
        MLTrainingDatasetConfig(holding_days=2, dropna_features=True)
    ).build(feature_frame, price_frame)

    assert len(training_frame) == 7
    assert not bool(training_frame[MODEL_FEATURE_COLUMNS].isna().any().any())


def test_ml_training_dataset_builder_rejects_invalid_horizon() -> None:
    with pytest.raises(ValueError, match="holding_days"):
        MLTrainingDatasetBuilder(MLTrainingDatasetConfig(holding_days=0))


def _feature_frame(periods: int) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=periods, freq="D")
    close = [100.0 + index for index in range(periods)]
    frame = pd.DataFrame(
        {
            "date": dates,
            "as_of_date": dates,
            "ticker": ["5930"] * periods,
            "close": close,
            "return_1d": [0.01] * periods,
            "close_to_ma_5": [0.02] * periods,
            "close_to_ma_20": [0.03] * periods,
            "volume_change_5d": [0.04] * periods,
            "trading_value_change_5d": [0.05] * periods,
            "range_pct": [0.06] * periods,
            "volatility_5d": [0.07] * periods,
            "volatility_20d": [0.08] * periods,
            "rsi_14": [55.0] * periods,
        }
    )
    return frame


def _processed_price_frame(periods: int) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=periods, freq="D")
    return pd.DataFrame(
        {
            "date": dates,
            "ticker": ["005930"] * periods,
            "close": [100.0 + index for index in range(periods)],
        }
    )
