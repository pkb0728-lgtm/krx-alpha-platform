from typing import Any

REQUIRED_ML_PREDICTION_COLUMNS = {
    "date",
    "as_of_date",
    "ticker",
    "split",
    "probability_positive_forward_return",
    "predicted_label",
    "target_positive_forward_return",
    "target_excess_forward_return",
    "forward_return",
    "benchmark_forward_return",
    "excess_forward_return",
    "label_end_date",
    "top_feature_reason",
    "model_name",
    "model_version",
}

REQUIRED_ML_METRIC_COLUMNS = {
    "split",
    "row_count",
    "positive_label_rate",
    "excess_label_rate",
    "model_target_label_rate",
    "predicted_positive_rate",
    "accuracy",
    "precision",
    "recall",
    "f1_score",
    "roc_auc",
    "brier_score",
    "average_probability",
    "selected_count",
    "selected_average_forward_return",
    "selected_average_excess_return",
    "top_k_count",
    "precision_at_top_k",
    "top_k_average_forward_return",
    "top_k_average_excess_return",
}

REQUIRED_ML_FEATURE_IMPORTANCE_COLUMNS = {
    "feature",
    "weight",
    "abs_weight",
    "direction",
}


def validate_ml_prediction_frame(frame: Any) -> None:
    missing_columns = REQUIRED_ML_PREDICTION_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required ML prediction columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("ML prediction frame is empty.")

    if frame[["date", "as_of_date", "label_end_date"]].isna().any().any():
        raise ValueError("ML prediction frame contains null dates.")

    probability = frame["probability_positive_forward_return"]
    if probability.dropna().between(0, 1).all() is False:
        raise ValueError("ML probabilities must be between 0 and 1.")

    if not set(frame["predicted_label"].astype(int).unique()).issubset({0, 1}):
        raise ValueError("predicted_label must be binary.")

    if not set(frame["target_excess_forward_return"].astype(int).unique()).issubset({0, 1}):
        raise ValueError("target_excess_forward_return must be binary.")


def validate_ml_metric_frame(frame: Any) -> None:
    missing_columns = REQUIRED_ML_METRIC_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required ML metric columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("ML metric frame is empty.")

    bounded_columns = [
        "positive_label_rate",
        "excess_label_rate",
        "model_target_label_rate",
        "predicted_positive_rate",
        "accuracy",
        "precision",
        "recall",
        "f1_score",
        "roc_auc",
        "brier_score",
        "average_probability",
        "precision_at_top_k",
    ]
    bounded_values = frame[bounded_columns].dropna()
    if ((bounded_values < 0) | (bounded_values > 1)).any().any():
        raise ValueError("ML metric values must be between 0 and 1.")


def validate_ml_feature_importance_frame(frame: Any) -> None:
    missing_columns = REQUIRED_ML_FEATURE_IMPORTANCE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required ML feature importance columns: {sorted(missing_columns)}"
        )

    if frame.empty:
        raise ValueError("ML feature importance frame is empty.")

    if (frame["abs_weight"] < 0).any():
        raise ValueError("ML feature importance abs_weight must be non-negative.")
