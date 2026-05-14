# Data Card: Macro/FRED Data V0

## Purpose

Macro/FRED data provides market-wide context for Korean stock screening. It is
used as decision-support evidence, not as a direct price prediction target.

## Source

- Demo mode: deterministic built-in sample data
- Live mode: FRED observations API
- Default series: `DGS10`, `DFF`, `DEXKOUS`

## Stored Paths

```text
data/raw/macro_fred_daily/macro_{start}_{end}_{series_slug}.parquet
data/features/macro_fred_daily/macro_{start}_{end}_{series_slug}.parquet
```

## Key Features

```text
us_10y_yield
fed_funds_rate
usdkrw
us_10y_yield_change_5d
usdkrw_change_pct_5d
macro_score
macro_reason
```

## Known Limitations

- FRED observations may be missing on holidays and are forward-filled only
  inside the feature date window.
- Macro evidence is broad market context and should not override stock-specific
  liquidity, disclosure, news, and risk checks.
- The current score is rule-based and intentionally conservative.

## Use Restrictions

Do not use this dataset alone to make investment decisions. It is designed for
human review as one component of a broader screening workflow.
