# Data Design

## Data Layers

| Layer | Purpose |
| --- | --- |
| `data/raw` | Source-shaped price and OpenDART data with minimal changes. |
| `data/processed` | Cleaned prices and named universe snapshots. |
| `data/features` | Reusable model/scoring features. |
| `data/signals` | Scores, regimes, final signals, and universe summaries. |
| `data/backtest` | Backtest trades and aggregate metrics. |

## Current Datasets

### Named Universes

Path:

```text
data/processed/universe/{universe_name}.parquet
```

Important columns:

```text
ticker
name
market
sector
reason
is_active
```

The current MVP includes static universes such as `demo`, `large_cap`, and
`semiconductor_auto`. Dynamic KOSPI200/KOSDAQ150 collectors are planned later.

### Raw Daily Prices

Path:

```text
data/raw/prices_daily/{ticker}_{start}_{end}.parquet
```

Important columns:

```text
date
ticker
open
high
low
close
volume
trading_value
trading_value_is_estimated
source
collected_at
```

### Raw OpenDART Company Overview

Path:

```text
data/raw/dart_company/{corp_code}.parquet
```

Important columns:

```text
corp_code
stock_code
stock_name
corp_name
corp_cls
ceo_nm
induty_code
est_dt
acc_mt
```

### Raw OpenDART Financial Statements

Path:

```text
data/raw/dart_financials/{corp_code}_{year}_{report_code}.parquet
```

Important columns:

```text
corp_code
ticker
bsns_year
reprt_code
fs_div
sj_div
account_nm
account_id
thstrm_amount
thstrm_amount_value
frmtrm_amount
frmtrm_amount_value
currency
```

### OpenDART Financial Features

Path:

```text
data/features/dart_financials/{corp_code}_{year}_{report_code}.parquet
```

Important columns:

```text
corp_code
ticker
bsns_year
reprt_code
fs_div
revenue
operating_income
net_income
total_assets
total_liabilities
total_equity
revenue_growth
operating_margin
net_margin
debt_ratio
roe
financial_score
financial_reason
```

These features convert normalized OpenDART accounts into reusable financial
signals. When a financial year is provided to the scoring command or daily
pipeline, the latest financial score is blended into the final daily score.

### OpenDART Disclosure Event Features

Path:

```text
data/features/dart_disclosure_events/{corp_code}_{start}_{end}.parquet
```

Important columns:

```text
date
as_of_date
corp_code
ticker
report_nm
rcept_no
event_category
event_score
event_risk_flag
event_reason
```

The event builder turns disclosure names into conservative event scores. Risk
events such as capital increases, convertible bonds, lawsuits, governance
issues, audit/designation warnings, listing risk, or trading suspension are
flagged so the final risk filter can block a candidate.

### Raw OpenDART Disclosures

Path:

```text
data/raw/dart_disclosures/{corp_code}_{start}_{end}.parquet
```

Important columns:

```text
corp_code
corp_name
stock_code
report_nm
rcept_no
rcept_dt
flr_nm
rm
```

### Processed Daily Prices

Adds return and range features:

```text
return_1d
log_return_1d
range_pct
as_of_date
processed_at
```

### Price Features

Current feature set:

```text
ma_5
ma_20
close_to_ma_5
close_to_ma_20
volume_change_5d
trading_value_change_5d
volatility_5d
volatility_20d
rsi_14
```

### ML Training Dataset

Path:

```text
data/features/ml_training/{ticker}_{start}_{end}_h{holding_days}.parquet
```

Important columns:

```text
date
as_of_date
ticker
return_1d
close_to_ma_5
close_to_ma_20
volume_change_5d
trading_value_change_5d
range_pct
volatility_5d
volatility_20d
rsi_14
close
future_close
label_end_date
holding_days
forward_return
target_positive_forward_return
label_created_at
```

This dataset is designed for probability-style ML models, not direct price
prediction. Features are known as of `as_of_date`; labels are created from a
future close at `label_end_date`. The future-return columns are labels/audit
columns and must not be used as model input features.

### Investor Flow Features

Raw path:

```text
data/raw/investor_flow_daily/{ticker}_{start}_{end}.parquet
```

Feature path:

```text
data/features/investor_flow_daily/{ticker}_{start}_{end}.parquet
```

Important feature columns:

```text
foreign_net_buy_value
institution_net_buy_value
smart_money_net_buy_value
foreign_net_buy_value_5d
institution_net_buy_value_5d
smart_money_net_buy_value_5d
flow_score
flow_reason
```

`smart_money_net_buy_value` is the combined foreign and institutional net-buy
value. The MVP scores recent inflow/outflow conservatively and keeps the reason
codes explainable.

### News Sentiment Features

Raw path:

```text
data/raw/news_daily/{ticker}_{start}_{end}.parquet
```

Feature path:

```text
data/features/news_sentiment_daily/{ticker}_{start}_{end}.parquet
```

Important feature columns:

```text
news_count
positive_news_count
negative_news_count
sentiment_score
news_score
news_reason
top_headline
summary
```

The raw collector supports Naver news search in live mode and deterministic
demo news in demo mode. The sentiment builder can use either a deterministic
rule-based analyzer or Gemini-compatible JSON output. The score remains
human-readable through reason codes such as `news_sentiment_positive`,
`news_sentiment_negative`, and `news_volume_elevated`.

### Macro/FRED Features

Raw path:

```text
data/raw/macro_fred_daily/macro_{start}_{end}_{series_slug}.parquet
```

Feature path:

```text
data/features/macro_fred_daily/macro_{start}_{end}_{series_slug}.parquet
```

Important feature columns:

```text
us_10y_yield
fed_funds_rate
usdkrw
us_10y_yield_change_5d
usdkrw_change_5d
usdkrw_change_pct_5d
macro_score
macro_reason
```

The default FRED series are `DGS10`, `DFF`, and `DEXKOUS`. The feature builder
keeps the output date-aligned and explainable through reason codes such as
`us_10y_yield_rising`, `fed_funds_rate_restrictive`, and
`usdkrw_rising_fx_pressure`.

### Daily Scores

Path:

```text
data/signals/scores_daily/{ticker}_{start}_{end}.parquet
```

Important columns:

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

### Market Regime

Path:

```text
data/signals/market_regime_daily/{ticker}_{start}_{end}.parquet
```

Important columns:

```text
regime
regime_score
risk_level
regime_reason
return_20d
volatility_20d
close_to_ma_60
```

The regime output is used both as human review context and as an input to the
final risk filter.

### Backtest Outputs

Trade-level results:

```text
data/backtest/trades/{ticker}_{start}_{end}.parquet
```

Metric-level results:

```text
data/backtest/metrics/{ticker}_{start}_{end}.parquet
```

Important columns:

```text
trade_count
win_rate
average_return
cumulative_return
max_drawdown
sharpe_ratio
exposure_count
```

Walk-forward fold results:

```text
data/backtest/walk_forward_folds/{ticker}_{start}_{end}.parquet
```

Walk-forward summary:

```text
data/backtest/walk_forward_summary/{ticker}_{start}_{end}.parquet
```

Important walk-forward columns:

```text
fold
train_start
train_end
test_start
test_end
signal_count
trade_count
compounded_return
positive_fold_ratio
worst_max_drawdown
```

### Paper Trading Outputs

Paper trade ledger:

```text
data/backtest/paper_trade_ledger/{ticker}_{start}_{end}.parquet
```

Open paper positions:

```text
data/backtest/paper_positions/{ticker}_{start}_{end}.parquet
```

Paper summary:

```text
data/backtest/paper_summary/{ticker}_{start}_{end}.parquet
```

Portfolio paper outputs:

```text
data/backtest/paper_portfolio_trade_ledger/{universe}_{start}_{end}.parquet
data/backtest/paper_portfolio_positions/{universe}_{start}_{end}.parquet
data/backtest/paper_portfolio_summary/{universe}_{start}_{end}.parquet
```

Important paper trading columns:

```text
universe
side
status
shares
execution_price
cash_after
position_qty_after
equity_after
realized_pnl
unrealized_pnl
cumulative_return
loaded_ticker_count
skipped_tickers
gross_exposure_pct
cash_pct
mode
```

`mode` is always `paper`. These artifacts are virtual execution records created
from local final signals and local processed prices. The portfolio variant
combines multiple ticker signals into one shared virtual cash book. No broker
API is called.

The dashboard can also load all files in `paper_portfolio_summary` and derive a
history table with `run_sequence`, `equity_high_watermark`, `drawdown`, and
`cumulative_trade_count`. These columns are computed views, not separate stored
contracts.

### Screening Outputs

Auto screener tables:

```text
data/signals/screening_daily/{report_name}.parquet
data/signals/screening_daily/{report_name}.csv
```

Auto screener Markdown reports:

```text
reports/screening/{report_name}.md
```

Important screening columns:

```text
screen_date
ticker
passed
review_priority
screen_score
final_action
confidence_score
market_regime
risk_blocked
risk_flags
suggested_position_pct
trading_value
trading_value_change_5d
rsi_14
volatility_5d
reasons
evidence_summary
caution_summary
review_checklist
signal_path
screened_at
```

The screener is a repeatable human-review shortlist. It combines final signal
state with recent technical context from the feature store. It is not an order
execution instruction. The explanation columns summarize positive evidence,
remaining caution points, and the manual review checklist for each candidate.

### Monitoring Outputs

Drift result tables:

```text
data/signals/drift/{report_name}.parquet
```

Drift Markdown reports:

```text
reports/monitoring/{report_name}.md
```

Operations health tables:

```text
data/signals/operations_health/{report_name}.parquet
```

Operations health Markdown reports:

```text
reports/monitoring/{report_name}.md
```

Important data drift columns:

```text
feature
mean_shift_score
std_ratio
missing_rate_delta
drift_detected
drift_reason
```

Important performance drift columns:

```text
run_type
metric
baseline_mean
recent_mean
absolute_change
relative_change
drift_detected
drift_reason
```

Important operations health columns:

```text
check_name
category
status
severity
path
row_count
modified_at
age_hours
detail
```

## Data Contract Rules

The current contracts check:

- required columns
- empty datasets
- duplicate `date` and `ticker`
- null dates
- negative market values
- invalid `high < low`
- score ranges between 0 and 100
- RSI range between 0 and 100
- screener scores, confidence, and suggested position sizes within valid ranges
- ML label horizon must end after each feature `as_of_date`
- ML target labels must be binary
- OpenDART corp_code and ticker formats

## Point-In-Time Principle

The project stores `as_of_date`, `collected_at`, `processed_at`, and
`feature_created_at` to prepare for future point-in-time backtesting and
data-leakage prevention.

ML training datasets additionally store `label_end_date` and
`label_created_at` so reviewers can verify that the feature date and future
label horizon are separated.

### ML Probability Baseline Outputs

Prediction path:

```text
data/signals/ml_predictions/{ticker}_{start}_{end}_h{holding_days}.parquet
```

Metric path:

```text
data/signals/ml_metrics/{ticker}_{start}_{end}_h{holding_days}.parquet
```

Model artifact path:

```text
models/probability_baseline/{ticker}_{start}_{end}_h{holding_days}.json
```

Report path:

```text
reports/modeling/probability_baseline_{ticker}_{start}_{end}_h{holding_days}.md
```

Important prediction columns:

```text
date
as_of_date
ticker
split
probability_positive_forward_return
predicted_label
target_positive_forward_return
forward_return
label_end_date
top_feature_reason
model_name
model_version
```

Important metric columns:

```text
split
row_count
positive_label_rate
predicted_positive_rate
accuracy
precision
recall
f1_score
roc_auc
brier_score
average_probability
```
