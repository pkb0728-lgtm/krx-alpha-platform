from typing import Any

from krx_alpha.contracts.ml_model_contract import (
    validate_ml_feature_importance_frame,
    validate_ml_metric_frame,
)


class MLProbabilityBaselineReportGenerator:
    """Generate a Markdown report for the first probability baseline model."""

    def generate(self, metrics: Any, feature_importance: Any) -> str:
        validate_ml_metric_frame(metrics)
        validate_ml_feature_importance_frame(feature_importance)
        test_metric = _metric_for_split(metrics, "test")

        return "\n".join(
            [
                "# ML Probability Baseline Report",
                "",
                "## Test Summary",
                "",
                f"- Rows: {int(test_metric['row_count'])}",
                f"- Positive label rate: {_format_percent(test_metric['positive_label_rate'])}",
                (
                    "- Predicted positive rate: "
                    f"{_format_percent(test_metric['predicted_positive_rate'])}"
                ),
                f"- Accuracy: {_format_percent(test_metric['accuracy'])}",
                f"- Precision: {_format_percent(test_metric['precision'])}",
                f"- Recall: {_format_percent(test_metric['recall'])}",
                f"- F1-score: {_format_percent(test_metric['f1_score'])}",
                f"- ROC-AUC: {float(test_metric['roc_auc']):.3f}",
                f"- Brier score: {float(test_metric['brier_score']):.3f}",
                "",
                "## Top Feature Weights",
                "",
                _format_feature_table(feature_importance.head(10)),
                "",
                "## Method",
                "",
                "- Uses a time-based train/test split.",
                "- Learns simple directional feature weights from the training window only.",
                "- Converts weighted feature z-scores into a positive-forward-return probability.",
                "- Keeps future-return columns out of model inputs.",
                "",
                "## Risk Note",
                "",
                "This is a transparent baseline for research workflow validation. It is not a "
                "production trading model.",
                "",
            ]
        )


def _metric_for_split(metrics: Any, split: str) -> Any:
    rows = metrics[metrics["split"] == split]
    if rows.empty:
        return metrics.iloc[-1]
    return rows.iloc[0]


def _format_feature_table(feature_importance: Any) -> str:
    rows = [
        "| Feature | Direction | Weight |",
        "| --- | --- | ---: |",
    ]
    for _, row in feature_importance.iterrows():
        rows.append(f"| {row['feature']} | {row['direction']} | {float(row['weight']):.4f} |")
    return "\n".join(rows)


def _format_percent(value: Any) -> str:
    return f"{float(value) * 100:.2f}%"
