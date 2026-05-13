# Model Card: Rule-Based Signal V0

## Purpose

This is an explainable rule-based scoring system for Korean stock screening.
It is not a price prediction model.

## Inputs

- moving averages
- RSI
- trading value change
- volatility
- daily price range

## Outputs

- technical score
- risk score
- total score
- signal label
- final action
- suggested position percentage

## Limitations

- No financial statement data yet
- No investor flow data yet
- Market regime is generated separately but not yet applied as an automatic filter
- Only simple single-stock backtest validation so far
- No machine learning model yet

## Prohibited Use

Do not use this output as direct investment advice or automated order logic.
