# Scoring And Risk

## Current Scoring Approach

The MVP uses an explainable rule-based scoring system. It does not predict a
future price. Instead, it scores whether the current condition is worth watching.

Current score components:

```text
technical_score
risk_score
financial_score
event_score
event_risk_flag
flow_score
news_score
macro_score
total_score
signal_label
score_reason
financial_reason
event_reason
flow_reason
news_reason
macro_reason
```

OpenDART financial statements can also be converted into a separate
`financial_score` and `financial_reason`. When a financial feature file is
attached, the total score blends technical, risk, and financial evidence.
Without a financial feature file, the score uses a neutral financial value.
OpenDART disclosure event features can also be attached. Risky disclosure
events are given a lower `event_score` and can set `event_risk_flag`.
Investor flow features can also be attached. Foreign and institutional net-buy
strength becomes `flow_score` and `flow_reason`.
News sentiment features can also be attached. Naver news and rule/Gemini-based
sentiment analysis become `news_score` and `news_reason`.
Macro features can also be attached. FRED US 10-year yields, the effective
federal funds rate, and USD/KRW become `macro_score` and `macro_reason`.

Current MVP weighting:

```text
total_score = technical_score * 0.30 + risk_score * 0.20 + financial_score * 0.15 + event_score * 0.10 + flow_score * 0.10 + news_score * 0.10 + macro_score * 0.05
```

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
- disclosure event risk
- weak investor flow
- unfavorable macro environment
- unfavorable market regime, currently `bear` and `high_volatility`

## Market Regime

The MVP generates a market regime context with labels such as `bull`, `bear`,
`sideways`, `high_volatility`, and `rebound`. The Signal Engine now carries
regime columns into final signals and blocks buy candidates during `bear` or
`high_volatility` regimes.

## Position Size

Suggested position percentage is intentionally conservative. It is a
decision-support hint, not an order instruction.

## Paper Trading

Paper trading uses final signals and processed prices to create virtual fills.
It buys only `buy_candidate` signals, exits on `avoid` or `blocked`, applies
configured cost/slippage assumptions, and records a virtual ledger, open
positions, summary, and Markdown report.

Paper trading never calls a broker API and never sends real orders.

## Backtest Link

The current backtest validates only `buy_candidate` final signals. It enters on
the next trading day's open, exits after the configured holding period, and
applies simple transaction cost and slippage assumptions.

Walk-forward validation repeats the same next-day-entry backtest across rolling
train/test windows. This helps review whether signal behavior is stable across
time instead of depending on a single backtest period.
