# Model/Data Card: ML Training Dataset V0

## Purpose

This dataset prepares point-in-time technical features and future-return labels
for probability-style stock screening models.

It is not a direct stock price prediction artifact. The intended target is a
probability such as "positive forward return over the configured holding
horizon."

## Inputs

- processed daily prices
- reusable price feature table
- configurable holding horizon
- configurable minimum forward return threshold

## Outputs

- technical model input features
- `future_close`
- `label_end_date`
- `forward_return`
- binary `target_positive_forward_return`

## Leakage Controls

- Features are tied to `as_of_date`
- Labels use a later `label_end_date`
- The final rows without enough future data are dropped
- Future-return columns are kept for audit and must not be model inputs

## Limitations

- Current MVP uses price-derived technical features only
- It does not yet include market-relative alpha labels
- It does not yet include cross-sectional universe labels
- Class balance depends heavily on period, ticker, and holding horizon

## Prohibited Use

Do not use this dataset to train an automated trading system without separate
walk-forward validation, transaction cost modeling, risk controls, and human
review.
