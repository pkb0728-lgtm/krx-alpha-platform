# Model Card: Scorecard Probability Baseline V0

## Purpose

This is the first explainable ML-style baseline for the project. It estimates
the probability of a positive or excess forward return over the configured
holding horizon.

It is not a direct price prediction model and it is not an automated trading
model.

## Inputs

- leakage-aware ML training dataset
- technical feature columns from `MODEL_FEATURE_COLUMNS`
- binary target `target_excess_forward_return` when available
- fallback binary target `target_positive_forward_return`

The model does not use `future_close`, `forward_return`, or
target label columns as input features.

## Method

- Sorts rows by ticker and date
- Uses a time-ordered train/test split
- Preferentially trains on excess-forward-return labels
- Learns simple directional feature weights from the training window
- Imputes missing features with training medians
- Converts weighted feature z-scores into a positive-return probability

This scorecard is intentionally simple so reviewers can inspect every step
before heavier models such as RandomForest, LightGBM, or XGBoost are added.

## Outputs

- probability of positive forward return
- predicted binary label
- train/test metrics
- feature weights and top feature reason
- JSON model artifact
- Markdown model report

## Metrics

- Accuracy
- Precision
- Recall
- F1-score
- ROC-AUC
- Brier score
- Positive label rate
- Predicted positive rate
- Excess label rate
- Precision@TopK
- TopK average forward/excess return
- Selected-candidate average forward/excess return

## Limitations

- It is a baseline, not a production model
- It does not tune hyperparameters
- It does not yet use cross-sectional universe ranking
- It does not yet run walk-forward ML validation
- It uses a simple benchmark-return fallback unless a benchmark frame is provided
- Feature weights are linear and simplified for explainability

## Prohibited Use

Do not use this model for real investment decisions without longer historical
validation, walk-forward ML testing, risk controls, transaction cost review,
and human oversight.
