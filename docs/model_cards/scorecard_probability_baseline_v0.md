# Model Card: Scorecard Probability Baseline V0

## Purpose

This is the first explainable ML-style baseline for the project. It estimates
the probability of a positive forward return over the configured holding
horizon.

It is not a direct price prediction model and it is not an automated trading
model.

## Inputs

- leakage-aware ML training dataset
- technical feature columns from `MODEL_FEATURE_COLUMNS`
- binary target `target_positive_forward_return`

The model does not use `future_close`, `forward_return`, or
`target_positive_forward_return` as input features.

## Method

- Sorts rows by ticker and date
- Uses a time-ordered train/test split
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

## Limitations

- It is a baseline, not a production model
- It does not tune hyperparameters
- It does not yet use cross-sectional universe ranking
- It does not yet run walk-forward ML validation
- Feature weights are linear and simplified for explainability

## Prohibited Use

Do not use this model for real investment decisions without longer historical
validation, walk-forward ML testing, risk controls, transaction cost review,
and human oversight.
