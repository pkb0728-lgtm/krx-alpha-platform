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
- optional OpenDART financial score
- optional OpenDART disclosure event score

## Outputs

- technical score
- risk score
- total score
- signal label
- final action
- suggested position percentage

## Limitations

- Financial features use the latest available demo snapshot unless a more
  precise point-in-time mapping is added later
- Disclosure event rules are conservative and keyword-based in the MVP
- No investor flow data yet
- Market regime blocks `bear` and `high_volatility` final signals
- Only simple single-stock backtest validation so far
- No machine learning model yet

## Prohibited Use

Do not use this output as direct investment advice or automated order logic.
