from dataclasses import dataclass
from typing import Any

import pandas as pd

from krx_alpha.contracts.ml_dataset_contract import validate_ml_training_frame

MODEL_FEATURE_COLUMNS = [
    "return_1d",
    "close_to_ma_5",
    "close_to_ma_20",
    "volume_change_5d",
    "trading_value_change_5d",
    "range_pct",
    "volatility_5d",
    "volatility_20d",
    "rsi_14",
]

ML_TRAINING_COLUMNS = [
    "date",
    "as_of_date",
    "ticker",
    *MODEL_FEATURE_COLUMNS,
    "close",
    "future_close",
    "label_end_date",
    "holding_days",
    "forward_return",
    "target_positive_forward_return",
    "label_created_at",
]


@dataclass(frozen=True)
class MLTrainingDatasetConfig:
    holding_days: int = 5
    minimum_forward_return: float = 0.0
    dropna_features: bool = False


class MLTrainingDatasetBuilder:
    """Build point-in-time supervised labels for probability-style models."""

    def __init__(self, config: MLTrainingDatasetConfig | None = None) -> None:
        self.config = config or MLTrainingDatasetConfig()
        if self.config.holding_days <= 0:
            raise ValueError("holding_days must be positive.")

    def build(self, feature_frame: Any, processed_price_frame: Any) -> Any:
        features = self._prepare_features(feature_frame)
        labels = self._build_forward_labels(processed_price_frame)
        frame = features.merge(labels, on=["date", "ticker"], how="inner")
        frame["holding_days"] = self.config.holding_days
        frame["forward_return"] = frame["future_close"] / frame["close"] - 1
        frame["target_positive_forward_return"] = (
            frame["forward_return"] > self.config.minimum_forward_return
        ).astype(int)
        frame["label_created_at"] = pd.Timestamp.now(tz="UTC")

        frame = frame.dropna(subset=["future_close", "label_end_date", "forward_return"])
        if self.config.dropna_features:
            frame = frame.dropna(subset=MODEL_FEATURE_COLUMNS)

        training_frame = frame[ML_TRAINING_COLUMNS].sort_values(["ticker", "date"])
        training_frame = training_frame.reset_index(drop=True)
        validate_ml_training_frame(training_frame)
        return training_frame

    def _prepare_features(self, feature_frame: Any) -> pd.DataFrame:
        frame = feature_frame.copy()
        missing_columns = set(["date", "as_of_date", "ticker", "close", *MODEL_FEATURE_COLUMNS])
        missing_columns -= set(frame.columns)
        if missing_columns:
            raise ValueError(f"Missing required feature columns: {sorted(missing_columns)}")

        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.date
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)
        return frame[["date", "as_of_date", "ticker", "close", *MODEL_FEATURE_COLUMNS]]

    def _build_forward_labels(self, processed_price_frame: Any) -> pd.DataFrame:
        frame = processed_price_frame.copy()
        missing_columns = {"date", "ticker", "close"} - set(frame.columns)
        if missing_columns:
            raise ValueError(f"Missing required processed price columns: {sorted(missing_columns)}")

        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)
        frame = frame.sort_values(["ticker", "date"]).reset_index(drop=True)
        groups = frame.groupby("ticker", group_keys=False)
        frame["future_close"] = groups["close"].shift(-self.config.holding_days)
        frame["label_end_date"] = groups["date"].shift(-self.config.holding_days)
        return frame[["date", "ticker", "future_close", "label_end_date"]]
