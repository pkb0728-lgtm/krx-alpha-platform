# Data Design

## Data Layers

| Layer | Purpose |
| --- | --- |
| `data/raw` | Source-shaped data with minimal changes. |
| `data/processed` | Cleaned prices and named universe snapshots. |
| `data/features` | Reusable model/scoring features. |
| `data/signals` | Scores, final signals, and universe summaries. |
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

## Point-In-Time Principle

The project stores `as_of_date`, `collected_at`, `processed_at`, and
`feature_created_at` to prepare for future point-in-time backtesting and
data-leakage prevention.
