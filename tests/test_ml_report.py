import pandas as pd

from krx_alpha.reports.ml_report import MLProbabilityBaselineReportGenerator


def test_ml_probability_baseline_report_contains_metrics_and_feature_weights() -> None:
    metrics = pd.DataFrame(
        {
            "split": ["test"],
            "row_count": [12],
            "positive_label_rate": [0.5],
            "predicted_positive_rate": [0.4],
            "accuracy": [0.75],
            "precision": [0.8],
            "recall": [0.6],
            "f1_score": [0.6857],
            "roc_auc": [0.77],
            "brier_score": [0.19],
            "average_probability": [0.52],
        }
    )
    feature_importance = pd.DataFrame(
        {
            "feature": ["rsi_14"],
            "weight": [0.25],
            "abs_weight": [0.25],
            "direction": ["positive"],
        }
    )

    report = MLProbabilityBaselineReportGenerator().generate(metrics, feature_importance)

    assert "ML Probability Baseline Report" in report
    assert "ROC-AUC: 0.770" in report
    assert "| rsi_14 | positive | 0.2500 |" in report
