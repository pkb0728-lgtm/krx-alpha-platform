# Data Card: Investor Flow Data V0

## Purpose

Investor flow data adds foreign and institutional net-buy context to the
price-based screening flow. This is especially important in the Korean market,
where 수급 can strongly affect short-term candidate quality.

## Source

- Demo mode: built-in deterministic sample data
- Live mode: `pykrx` investor trading value and volume by date

## Current Storage

```text
data/raw/investor_flow_daily/
data/features/investor_flow_daily/
```

## Feature Summary

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

## Limitations

- Demo mode is for repeatable portfolio demonstration only.
- Live pykrx data can fail when upstream KRX endpoints are unavailable.
- The MVP uses rule-based scoring, not a calibrated statistical flow model.

## Next Steps

- Add short-selling and program trading features.
- Add market-cap-adjusted flow ratios.
- Validate flow score thresholds through walk-forward backtesting.
