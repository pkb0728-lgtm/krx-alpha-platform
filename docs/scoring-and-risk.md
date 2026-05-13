# Scoring And Risk

## Current Scoring Approach

The MVP uses an explainable rule-based scoring system. It does not predict a
future price. Instead, it scores whether the current condition is worth watching.

Current score components:

```text
technical_score
risk_score
total_score
signal_label
score_reason
```

OpenDART financial statements can also be converted into a separate
`financial_score` and `financial_reason`. The MVP keeps this score separate so
it can be reviewed before it is blended into the final stock score.

## Technical Score

The technical score starts from a neutral base and adjusts for:

- close price versus 5-day moving average
- close price versus 20-day moving average
- RSI recovery zone
- trading value increase

## Risk Score

The risk score penalizes:

- high 5-day volatility
- high 20-day volatility
- wide daily trading range

## Final Signal

The Signal Engine converts scores into human-readable actions:

| Action | Meaning |
| --- | --- |
| `buy_candidate` | Candidate passed current score and risk filters. |
| `watch` | Worth monitoring but needs more confirmation. |
| `hold` | No strong action from current rules. |
| `avoid` | Weak score condition. |
| `blocked` | Risk filter blocked the signal. |

## Risk Filters

Current risk filters include:

- insufficient history
- low liquidity
- wide daily range
- high short-term volatility
- weak risk score
- unfavorable market regime, currently `bear` and `high_volatility`

## Market Regime

The MVP generates a market regime context with labels such as `bull`, `bear`,
`sideways`, `high_volatility`, and `rebound`. The Signal Engine now carries
regime columns into final signals and blocks buy candidates during `bear` or
`high_volatility` regimes.

## Position Size

Suggested position percentage is intentionally conservative. It is a
decision-support hint, not an order instruction.

## Backtest Link

The current backtest validates only `buy_candidate` final signals. It enters on
the next trading day's open, exits after the configured holding period, and
applies simple transaction cost and slippage assumptions.
