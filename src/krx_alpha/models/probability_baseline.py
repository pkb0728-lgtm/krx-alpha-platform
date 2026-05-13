from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from math import exp, log
from typing import Any

import pandas as pd

from krx_alpha.contracts.ml_model_contract import (
    validate_ml_feature_importance_frame,
    validate_ml_metric_frame,
    validate_ml_prediction_frame,
)
from krx_alpha.models.training_dataset import MODEL_FEATURE_COLUMNS

ML_PROBABILITY_BASELINE_MODEL_NAME = "scorecard_probability_baseline"
ML_PROBABILITY_BASELINE_MODEL_VERSION = "v0"


@dataclass(frozen=True)
class MLProbabilityBaselineConfig:
    train_fraction: float = 0.7
    probability_threshold: float = 0.55
    min_train_rows: int = 20
    score_scale: float = 2.0


@dataclass(frozen=True)
class MLProbabilityBaselineState:
    feature_columns: list[str]
    feature_medians: dict[str, float]
    feature_stds: dict[str, float]
    feature_weights: dict[str, float]
    base_rate: float
    intercept: float
    trained_at: str


@dataclass(frozen=True)
class MLProbabilityBaselineResult:
    predictions: Any
    metrics: Any
    feature_importance: Any
    artifact: dict[str, object]


class MLProbabilityBaselineTrainer:
    """Train an explainable probability scorecard without heavyweight ML dependencies."""

    def __init__(self, config: MLProbabilityBaselineConfig | None = None) -> None:
        self.config = config or MLProbabilityBaselineConfig()
        if not 0 < self.config.train_fraction < 1:
            raise ValueError("train_fraction must be between 0 and 1.")
        if not 0 <= self.config.probability_threshold <= 1:
            raise ValueError("probability_threshold must be between 0 and 1.")
        if self.config.min_train_rows <= 0:
            raise ValueError("min_train_rows must be positive.")

    def train_evaluate(self, training_frame: Any) -> MLProbabilityBaselineResult:
        frame = _prepare_training_frame(training_frame)
        train_frame, test_frame = self._time_split(frame)
        state = self._fit(train_frame)
        predictions = pd.concat(
            [
                self._predict(train_frame, state, split="train"),
                self._predict(test_frame, state, split="test"),
            ],
            ignore_index=True,
        )
        metrics = _build_metrics(predictions)
        feature_importance = _build_feature_importance(state)
        artifact = _build_artifact(state, self.config)

        validate_ml_prediction_frame(predictions)
        validate_ml_metric_frame(metrics)
        validate_ml_feature_importance_frame(feature_importance)
        return MLProbabilityBaselineResult(
            predictions=predictions,
            metrics=metrics,
            feature_importance=feature_importance,
            artifact=artifact,
        )

    def _time_split(self, frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        row_count = len(frame)
        if row_count < self.config.min_train_rows + 1:
            raise ValueError(
                "Not enough rows to train and evaluate ML baseline. "
                f"Need at least {self.config.min_train_rows + 1}, got {row_count}."
            )

        split_index = int(row_count * self.config.train_fraction)
        split_index = max(self.config.min_train_rows, split_index)
        split_index = min(split_index, row_count - 1)
        return frame.iloc[:split_index].copy(), frame.iloc[split_index:].copy()

    def _fit(self, train_frame: pd.DataFrame) -> MLProbabilityBaselineState:
        features = _feature_matrix(train_frame)
        target = train_frame["target_positive_forward_return"].astype(int)
        medians = _series_to_float_dict(features.median())
        stds = _safe_std_dict(features)
        weights = _fit_feature_weights(features, target, medians, stds)
        base_rate = _clip_probability(float(target.mean()))
        return MLProbabilityBaselineState(
            feature_columns=list(MODEL_FEATURE_COLUMNS),
            feature_medians=medians,
            feature_stds=stds,
            feature_weights=weights,
            base_rate=base_rate,
            intercept=_logit(base_rate),
            trained_at=datetime.now(UTC).isoformat(),
        )

    def _predict(
        self,
        frame: pd.DataFrame,
        state: MLProbabilityBaselineState,
        split: str,
    ) -> pd.DataFrame:
        contributions = _feature_contributions(frame, state, self.config.score_scale)
        raw_scores = contributions.sum(axis=1) + state.intercept
        probabilities = raw_scores.map(_sigmoid)
        predicted_labels = (probabilities >= self.config.probability_threshold).astype(int)

        prediction_frame = pd.DataFrame(
            {
                "date": frame["date"],
                "as_of_date": frame["as_of_date"],
                "ticker": frame["ticker"],
                "split": split,
                "probability_positive_forward_return": probabilities,
                "predicted_label": predicted_labels,
                "target_positive_forward_return": frame["target_positive_forward_return"].astype(
                    int
                ),
                "forward_return": frame["forward_return"].astype(float),
                "label_end_date": frame["label_end_date"],
                "top_feature_reason": contributions.apply(_top_feature_reason, axis=1),
                "model_name": ML_PROBABILITY_BASELINE_MODEL_NAME,
                "model_version": ML_PROBABILITY_BASELINE_MODEL_VERSION,
            }
        )
        return prediction_frame.reset_index(drop=True)


def _prepare_training_frame(frame: Any) -> pd.DataFrame:
    missing_columns = set(
        [
            "date",
            "as_of_date",
            "ticker",
            "target_positive_forward_return",
            "forward_return",
            "label_end_date",
            *MODEL_FEATURE_COLUMNS,
        ]
    ) - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required ML training columns: {sorted(missing_columns)}")

    prepared = frame.copy()
    prepared["date"] = pd.to_datetime(prepared["date"]).dt.date
    prepared["as_of_date"] = pd.to_datetime(prepared["as_of_date"]).dt.date
    prepared["label_end_date"] = pd.to_datetime(prepared["label_end_date"]).dt.date
    prepared["ticker"] = prepared["ticker"].astype(str).str.zfill(6)
    prepared = prepared.dropna(subset=["target_positive_forward_return", "forward_return"])
    prepared = prepared.sort_values(["ticker", "date"]).reset_index(drop=True)
    if prepared.empty:
        raise ValueError("ML training frame is empty after dropping rows without labels.")
    return prepared


def _feature_matrix(frame: pd.DataFrame) -> pd.DataFrame:
    return frame[MODEL_FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")


def _series_to_float_dict(series: pd.Series) -> dict[str, float]:
    result: dict[str, float] = {}
    for key, value in series.items():
        parsed = float(value) if not pd.isna(value) else 0.0
        result[str(key)] = parsed
    return result


def _safe_std_dict(features: pd.DataFrame) -> dict[str, float]:
    result: dict[str, float] = {}
    for column, value in features.std(ddof=0).items():
        parsed = float(value) if not pd.isna(value) else 0.0
        result[str(column)] = parsed if parsed > 0 else 1.0
    return result


def _fit_feature_weights(
    features: pd.DataFrame,
    target: pd.Series,
    medians: dict[str, float],
    stds: dict[str, float],
) -> dict[str, float]:
    if target.nunique() < 2:
        return {column: 0.0 for column in MODEL_FEATURE_COLUMNS}

    filled = features.fillna(medians)
    positive_mean = filled[target == 1].mean()
    negative_mean = filled[target == 0].mean()
    raw_weights: dict[str, float] = {}
    for column in MODEL_FEATURE_COLUMNS:
        raw_value = (float(positive_mean[column]) - float(negative_mean[column])) / stds[column]
        raw_weights[column] = raw_value if not pd.isna(raw_value) else 0.0

    denominator = sum(abs(value) for value in raw_weights.values())
    if denominator == 0:
        return {column: 0.0 for column in MODEL_FEATURE_COLUMNS}
    return {column: value / denominator for column, value in raw_weights.items()}


def _feature_contributions(
    frame: pd.DataFrame,
    state: MLProbabilityBaselineState,
    score_scale: float,
) -> pd.DataFrame:
    features = _feature_matrix(frame).fillna(state.feature_medians)
    contributions = pd.DataFrame(index=frame.index)
    for column in state.feature_columns:
        z_score = (features[column] - state.feature_medians[column]) / state.feature_stds[column]
        contributions[column] = z_score * state.feature_weights[column] * score_scale
    return contributions


def _top_feature_reason(row: pd.Series) -> str:
    if row.empty:
        return "no_feature_contribution"

    feature = str(row.abs().idxmax())
    contribution = float(row[feature])
    if contribution > 0:
        return f"{feature}_supports_positive_probability"
    if contribution < 0:
        return f"{feature}_supports_negative_probability"
    return "baseline_probability_only"


def _build_metrics(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for split, split_frame in predictions.groupby("split", sort=False):
        target = split_frame["target_positive_forward_return"].astype(int)
        predicted = split_frame["predicted_label"].astype(int)
        probability = split_frame["probability_positive_forward_return"].astype(float)
        rows.append(
            {
                "split": split,
                "row_count": int(len(split_frame)),
                "positive_label_rate": float(target.mean()),
                "predicted_positive_rate": float(predicted.mean()),
                "accuracy": _accuracy(target, predicted),
                "precision": _precision(target, predicted),
                "recall": _recall(target, predicted),
                "f1_score": _f1_score(target, predicted),
                "roc_auc": _roc_auc(target, probability),
                "brier_score": float(((probability - target) ** 2).mean()),
                "average_probability": float(probability.mean()),
            }
        )
    return pd.DataFrame(rows)


def _build_feature_importance(state: MLProbabilityBaselineState) -> pd.DataFrame:
    rows = []
    for feature, weight in state.feature_weights.items():
        direction = "positive" if weight > 0 else "negative" if weight < 0 else "neutral"
        rows.append(
            {
                "feature": feature,
                "weight": float(weight),
                "abs_weight": abs(float(weight)),
                "direction": direction,
            }
        )
    return pd.DataFrame(rows).sort_values("abs_weight", ascending=False).reset_index(drop=True)


def _build_artifact(
    state: MLProbabilityBaselineState,
    config: MLProbabilityBaselineConfig,
) -> dict[str, object]:
    artifact = asdict(state)
    artifact["model_name"] = ML_PROBABILITY_BASELINE_MODEL_NAME
    artifact["model_version"] = ML_PROBABILITY_BASELINE_MODEL_VERSION
    artifact["config"] = asdict(config)
    return artifact


def _accuracy(target: pd.Series, predicted: pd.Series) -> float:
    return float((target == predicted).mean())


def _precision(target: pd.Series, predicted: pd.Series) -> float:
    true_positive = int(((target == 1) & (predicted == 1)).sum())
    false_positive = int(((target == 0) & (predicted == 1)).sum())
    denominator = true_positive + false_positive
    return true_positive / denominator if denominator else 0.0


def _recall(target: pd.Series, predicted: pd.Series) -> float:
    true_positive = int(((target == 1) & (predicted == 1)).sum())
    false_negative = int(((target == 1) & (predicted == 0)).sum())
    denominator = true_positive + false_negative
    return true_positive / denominator if denominator else 0.0


def _f1_score(target: pd.Series, predicted: pd.Series) -> float:
    precision = _precision(target, predicted)
    recall = _recall(target, predicted)
    denominator = precision + recall
    return 2 * precision * recall / denominator if denominator else 0.0


def _roc_auc(target: pd.Series, probability: pd.Series) -> float:
    positives = int((target == 1).sum())
    negatives = int((target == 0).sum())
    if positives == 0 or negatives == 0:
        return 0.5

    ranks = probability.rank(method="average")
    positive_rank_sum = float(ranks[target == 1].sum())
    return (positive_rank_sum - positives * (positives + 1) / 2) / (positives * negatives)


def _clip_probability(value: float) -> float:
    return min(max(value, 0.01), 0.99)


def _logit(value: float) -> float:
    clipped = _clip_probability(value)
    return log(clipped / (1 - clipped))


def _sigmoid(value: float) -> float:
    if value >= 0:
        return 1 / (1 + exp(-value))
    exponential = exp(value)
    return exponential / (1 + exponential)
