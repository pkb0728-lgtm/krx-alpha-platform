import pandas as pd
import pytest

from krx_alpha.models.probability_baseline import (
    ML_PROBABILITY_BASELINE_MODEL_NAME,
    MLProbabilityBaselineConfig,
    MLProbabilityBaselineTrainer,
)
from krx_alpha.models.training_dataset import MODEL_FEATURE_COLUMNS


def test_probability_baseline_trains_and_returns_out_of_sample_metrics() -> None:
    training_frame = _training_frame(periods=40)

    result = MLProbabilityBaselineTrainer(
        MLProbabilityBaselineConfig(train_fraction=0.7, min_train_rows=20)
    ).train_evaluate(training_frame)

    assert set(result.predictions["split"]) == {"train", "test"}
    assert len(result.metrics) == 2
    assert result.metrics.loc[result.metrics["split"] == "test", "row_count"].iloc[0] == 12
    assert result.predictions["probability_positive_forward_return"].between(0, 1).all()
    assert result.feature_importance.iloc[0]["abs_weight"] >= 0
    assert result.artifact["model_name"] == ML_PROBABILITY_BASELINE_MODEL_NAME


def test_probability_baseline_rejects_too_little_training_data() -> None:
    with pytest.raises(ValueError, match="Not enough rows"):
        MLProbabilityBaselineTrainer(MLProbabilityBaselineConfig(min_train_rows=20)).train_evaluate(
            _training_frame(periods=10)
        )


def _training_frame(periods: int) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=periods, freq="D")
    target = [1 if index % 3 != 0 else 0 for index in range(periods)]
    frame = pd.DataFrame(
        {
            "date": dates,
            "as_of_date": dates,
            "ticker": ["005930"] * periods,
            "forward_return": [0.02 if value == 1 else -0.01 for value in target],
            "label_end_date": dates + pd.Timedelta(days=5),
            "target_positive_forward_return": target,
        }
    )
    for column in MODEL_FEATURE_COLUMNS:
        frame[column] = [
            float(index) if target[index] == 1 else -float(index + 1) for index in range(periods)
        ]
    return frame
