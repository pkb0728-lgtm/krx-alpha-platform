# Usage Guide

## 1. Open The Project

```powershell
cd C:\Users\USER\Documents\Codex\2026-05-13\role-python-mlops-github-vscode-python
code .
```

Open a VSCode terminal and activate the virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

## 2. Check The Environment

```powershell
python main.py doctor
pytest
```

Expected result:

```text
KRX Alpha Platform is ready.
pytest: all tests passed
```

## 3. Run A Single Stock Pipeline

```powershell
python main.py run-pipeline --ticker 005930 --start 2024-01-01 --end 2024-01-31
```

This creates:

```text
data/raw/prices_daily/
data/processed/prices_daily/
data/features/prices_daily/
data/signals/market_regime_daily/
data/signals/scores_daily/
data/signals/final_signals_daily/
reports/daily/
reports/regime/
```

Optional investor flow features can be prepared before running the multi-factor
pipeline:

```powershell
python main.py collect-investor-flow --ticker 005930 --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-investor-flow-features --ticker 005930 --start 2024-01-01 --end 2024-01-31
```

## 4. List A Named Universe

List available universes:

```powershell
python main.py list-universe --universe all
```

Save and inspect the beginner-friendly demo universe:

```powershell
python main.py list-universe --universe demo
```

This creates:

```text
data/processed/universe/demo.parquet
data/processed/universe/demo.csv
```

## 5. Collect OpenDART Demo Data

These commands work without an API key because they use built-in demo data:

```powershell
python main.py collect-dart-company --ticker 005930 --demo
python main.py collect-dart-financials --ticker 005930 --year 2023 --report-code 11011 --demo
python main.py build-dart-financial-features --ticker 005930 --year 2023 --report-code 11011
python main.py collect-dart-disclosures --ticker 005930 --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-dart-disclosure-events --ticker 005930 --start 2024-01-01 --end 2024-01-31
```

Output examples:

```text
data/raw/dart_company/00126380.parquet
data/raw/dart_financials/00126380_2023_11011.parquet
data/features/dart_financials/00126380_2023_11011.parquet
data/raw/dart_disclosures/00126380_20240101_20240131.parquet
data/features/dart_disclosure_events/00126380_20240101_20240131.parquet
```

The financial feature output includes revenue growth, operating margin, debt
ratio, ROE, a 0-100 financial score, and reason labels for explainability.

Use the financial and disclosure event feature files in the daily pipeline:

```powershell
python main.py run-pipeline --ticker 005930 --start 2024-01-01 --end 2024-01-31 --financial-year 2023 --event-start 2024-01-01 --event-end 2024-01-31 --flow-start 2024-01-01 --flow-end 2024-01-31
```

When these options are provided, the score blends technical, risk, financial,
event, and investor flow evidence. Without those options, the pipeline uses
neutral financial, event, and flow scores of `50.0`.

To use the live OpenDART API, put `DART_API_KEY` in `.env` and replace
`--demo` with `--live`.

## 6. Run A Universe Pipeline

```powershell
python main.py run-universe --universe demo --start 2024-01-01 --end 2024-01-31
python main.py generate-universe-report --start 2024-01-01 --end 2024-01-31
```

Manual tickers are also supported:

```powershell
python main.py run-universe --tickers 005930,000660,005380 --start 2024-01-01 --end 2024-01-31
```

Output examples:

```text
data/signals/universe_summary_daily/universe_20240101_20240131.parquet
data/signals/universe_summary_daily/universe_20240101_20240131.csv
reports/universe/universe_20240101_20240131.md
```

## 7. Analyze Market Regime

`run-pipeline` creates regime outputs automatically. You can also refresh the
regime analysis separately:

```powershell
python main.py analyze-regime --ticker 005380 --start 2024-01-01 --end 2024-03-31
```

Output examples:

```text
data/signals/market_regime_daily/005380_20240101_20240331.parquet
reports/regime/005380_20240101_20240331.md
```

The MVP regime analyzer classifies conditions such as `bull`, `bear`,
`sideways`, `high_volatility`, `rebound`, and `neutral`. `bear` and
`high_volatility` regimes are connected to the final risk filter.

## 8. Backtest A Stock Signal

Run this after `run-pipeline` has created processed prices and final signals:

```powershell
python main.py run-pipeline --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py analyze-regime --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py backtest-stock --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py walk-forward-backtest --ticker 005380 --start 2024-01-01 --end 2024-03-31 --train-size 20 --test-size 5 --step-size 5
```

Output examples:

```text
data/backtest/trades/005380_20240101_20240331.parquet
data/backtest/metrics/005380_20240101_20240331.parquet
data/backtest/walk_forward_folds/005380_20240101_20240331.parquet
data/backtest/walk_forward_summary/005380_20240101_20240331.parquet
reports/backtest/005380_20240101_20240331.md
reports/backtest/walk_forward_005380_20240101_20240331.md
```

The MVP backtest enters on the next trading day's open after a
`buy_candidate` signal, exits after the configured holding period, and applies
simple transaction cost and slippage assumptions. Walk-forward validation
repeats that backtest across rolling out-of-sample windows.

## 9. Open The Dashboard

Install dashboard dependencies if needed:

```powershell
python -m pip install -e ".[dashboard]"
```

Run Streamlit:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

Open:

```text
http://localhost:8501
```

The dashboard shows the latest universe summary, action distribution, latest
backtest metrics, backtest trades, walk-forward summary, fold-level validation
results, and selected Markdown report.
