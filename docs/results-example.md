# Result Example

## Universe Screening

Example command:

```powershell
python main.py list-universe --universe demo
python main.py run-universe --universe demo --start 2024-01-01 --end 2024-01-31
```

Example output:

```text
Universe pipeline completed.
Total: 3
Success: 3
Failed: 0

Ticker  Status   Action         Confidence
005380  success  buy_candidate  72.83
005930  success  watch          63.78
000660  success  watch          59.37
```

## Interpretation

The output does not mean "buy this stock." It means the stock passed the current
rule-based screening and risk-filter logic for the selected date range.

## OpenDART Demo Collection

Example command:

```powershell
python main.py collect-dart-financials --ticker 005930 --year 2023 --report-code 11011 --demo
python main.py build-dart-financial-features --ticker 005930 --year 2023 --report-code 11011
python main.py collect-dart-disclosures --ticker 005930 --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-dart-disclosure-events --ticker 005930 --start 2024-01-01 --end 2024-01-31
python main.py collect-investor-flow --ticker 005930 --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-investor-flow-features --ticker 005930 --start 2024-01-01 --end 2024-01-31
```

Example output:

```text
Collected DART financial statements.
Ticker: 005930
Corp code: 00126380
Rows: 6
Source: opendart_demo
```

Example financial feature output:

```text
Built DART financial features.
Ticker: 005930
Corp code: 00126380
Financial score: 100.00
Reason: revenue_growth_positive, operating_margin_healthy, net_margin_positive, debt_ratio_conservative
```

Example multi-factor pipeline command:

```powershell
python main.py run-pipeline --ticker 005930 --start 2024-01-01 --end 2024-01-31 --financial-year 2023 --event-start 2024-01-01 --event-end 2024-01-31 --flow-start 2024-01-01 --flow-end 2024-01-31
```

Example scoring line:

```text
Financial score: 100.00
Event score: 50.00
Flow score: 85.00
```

| Action | Meaning |
| --- | --- |
| `buy_candidate` | Candidate passed score and risk filters. |
| `watch` | Worth monitoring, but confirmation is still needed. |
| `blocked` | Risk filter blocked the signal. |
| `avoid` | Weak evidence or weak score. |

## Dashboard

The Streamlit dashboard displays:

- number of tickers
- success and failed counts
- ranked candidate table
- action distribution chart
- latest backtest metrics
- latest backtest trades
- latest walk-forward summary
- fold-level walk-forward validation table
- selected stock Markdown report

Run:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

## Telegram Brief

Example command:

```powershell
python main.py send-telegram-daily --dry-run
```

Example output:

```text
KRX Alpha Daily Brief
Generated: 2026-05-13 09:00

Universe
- Tickers: 3
- Success: 3
- Failed: 0

Top candidates
1. 005380 | buy_candidate | score 72.83 | regime bull | F 80.00 / E 55.00 / Flow 70.00

Backtest
- 005380 | trades 7 | win 57.14% | return 78.67% | MDD -10.35% | Sharpe 4.33

Walk-forward
- 005380 | folds 3 | trades 2 | compounded 3.64% | worst MDD -5.20% | positive folds 66.67%
```

## Daily Job

Example command:

```powershell
python main.py run-daily-job --universe demo --start 2024-01-01 --end 2024-01-31 --telegram-dry-run
```

Example output:

```text
Daily scheduled job completed.
Universe: demo
Period: 2024-01-01 to 2024-01-31
Total: 3
Success: 3
Failed: 0
Experiment log: experiments/experiment_log.csv
Telegram: dry-run
```

## Experiment Tracking

Example command:

```powershell
python main.py show-experiments --limit 5
```

Example fields:

```text
created_at | experiment_name | run_type | ticker | universe | start_date | end_date | metrics_json | artifact_path
```

Backtest runs store return, drawdown, win rate, Sharpe ratio, and trade count.
Daily job runs store ticker counts, success count, failed count, and the report
artifact path.

## Drift Monitoring

Example commands:

```powershell
python main.py detect-performance-drift --run-type backtest --metric cumulative_return --baseline-window 1 --recent-window 1
python main.py detect-data-drift --reference-path data/features/prices_daily/005930_20240101_20240131.parquet --current-path data/features/prices_daily/005380_20240101_20240131.parquet --columns rsi_14,volatility_5d,trading_value_change_5d
```

Example output:

```text
Performance drift detection completed.
Run type: backtest
Metric: cumulative_return
Drift detected: False
Report: reports/monitoring/performance_drift_backtest_cumulative_return.md

Data drift detection completed.
Checked features: 3
Drifted features: 1
Report: reports/monitoring/data_drift_005930_vs_005380.md
```

The dashboard displays the latest drift result in the `Drift Monitoring`
section. The Telegram daily brief also includes a compact `Drift` section after
the walk-forward summary.

## Backtest

Example command:

```powershell
python main.py run-pipeline --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py analyze-regime --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py backtest-stock --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py walk-forward-backtest --ticker 005380 --start 2024-01-01 --end 2024-03-31 --train-size 20 --test-size 5 --step-size 5
```

Example regime output:

```text
Market regime analyzed.
Latest regime: neutral
Regime score: 50.00
Risk level: medium
```

Example output:

```text
Backtest completed.
Trades: 7
Win rate: 57.14%
Cumulative return: 78.67%
Max drawdown: -10.35%
Sharpe ratio: 4.33
```

Example walk-forward output:

```text
Walk-forward backtest completed.
Folds: 3
Trades: 0
Compounded return: 0.00%
Positive fold ratio: 0.00%
```

This is a simple MVP backtest for signal validation. It is not a production
execution simulator.
