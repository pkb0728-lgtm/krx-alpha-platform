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
- optional OpenDART financial feature output for separate review

## Outputs

- technical score
- risk score
- total score
- signal label
- final action
- suggested position percentage

## Limitations

- Financial features exist, but are not yet blended into final action scoring
- No investor flow data yet
- Market regime blocks `bear` and `high_volatility` final signals
- Only simple single-stock backtest validation so far
- No machine learning model yet

## Prohibited Use

Do not use this output as direct investment advice or automated order logic.
