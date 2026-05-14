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

Optional news sentiment features can also be prepared. The default demo/rule
path works without API keys:

```powershell
python main.py collect-news --ticker 005930 --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-news-sentiment --ticker 005930 --start 2024-01-01 --end 2024-01-31 --rule-based
```

After `NAVER_CLIENT_ID`, `NAVER_CLIENT_SECRET`, and `GEMINI_API_KEY` are set in
`.env`, replace `--demo` with `--live` for Naver collection and use `--gemini`
for Gemini-based summarization and sentiment scoring.

Optional macro features can be prepared from FRED. The demo path works without
an API key:

```powershell
python main.py collect-macro --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-macro-features --start 2024-01-01 --end 2024-01-31
```

After `FRED_API_KEY` is set in `.env`, replace `--demo` with `--live` to collect
real FRED observations. The default series are `DGS10`, `DFF`, and `DEXKOUS`.

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

Use the financial, disclosure event, investor flow, news, and macro feature
files in the daily pipeline:

```powershell
python main.py run-pipeline --ticker 005930 --start 2024-01-01 --end 2024-01-31 --financial-year 2023 --event-start 2024-01-01 --event-end 2024-01-31 --flow-start 2024-01-01 --flow-end 2024-01-31 --news-start 2024-01-01 --news-end 2024-01-31 --macro-start 2024-01-01 --macro-end 2024-01-31
```

When these options are provided, the score blends technical, risk, financial,
event, investor flow, news sentiment, and macro evidence. Without those
options, the pipeline uses neutral financial, event, flow, news, and macro
scores of `50.0`.

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
python main.py paper-trade --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py paper-trade-universe --universe demo --start 2024-01-01 --end 2024-03-31
python main.py backtest-stock --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py walk-forward-backtest --ticker 005380 --start 2024-01-01 --end 2024-03-31 --train-size 20 --test-size 5 --step-size 5
```

`paper-trade` and `paper-trade-universe` are paper-only operations. They do not
call Korea Investment, Telegram, or any broker endpoint. They only read local
signal/price files and create virtual ledgers, open-position snapshots,
summaries, and Markdown reports.

Prepare a leakage-aware ML dataset for later probability modeling:

```powershell
python main.py build-ml-dataset --ticker 005380 --start 2024-01-01 --end 2024-03-31 --holding-days 5
python main.py train-ml-baseline --ticker 005380 --start 2024-01-01 --end 2024-03-31 --holding-days 5
```

Output example:

```text
data/features/ml_training/005380_20240101_20240331_h5.parquet
data/signals/ml_predictions/005380_20240101_20240331_h5.parquet
data/signals/ml_metrics/005380_20240101_20240331_h5.parquet
models/probability_baseline/005380_20240101_20240331_h5.json
reports/modeling/probability_baseline_005380_20240101_20240331_h5.md
```

The ML dataset uses features available on `as_of_date` and stores the future
return label separately with `label_end_date`. Do not train on `future_close`,
`forward_return`, or `target_positive_forward_return` as input features. The
baseline command uses a time-ordered train/test split and reports out-of-sample
classification metrics.

Output examples:

```text
data/backtest/trades/005380_20240101_20240331.parquet
data/backtest/metrics/005380_20240101_20240331.parquet
data/backtest/paper_trade_ledger/005380_20240101_20240331.parquet
data/backtest/paper_positions/005380_20240101_20240331.parquet
data/backtest/paper_summary/005380_20240101_20240331.parquet
data/backtest/paper_portfolio_trade_ledger/demo_20240101_20240331.parquet
data/backtest/paper_portfolio_positions/demo_20240101_20240331.parquet
data/backtest/paper_portfolio_summary/demo_20240101_20240331.parquet
data/backtest/walk_forward_folds/005380_20240101_20240331.parquet
data/backtest/walk_forward_summary/005380_20240101_20240331.parquet
reports/paper_trading/005380_20240101_20240331.md
reports/paper_trading/portfolio_demo_20240101_20240331.md
reports/backtest/005380_20240101_20240331.md
reports/backtest/walk_forward_005380_20240101_20240331.md
data/features/ml_training/005380_20240101_20240331_h5.parquet
data/signals/ml_metrics/005380_20240101_20240331_h5.parquet
reports/modeling/probability_baseline_005380_20240101_20240331_h5.md
experiments/experiment_log.csv
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
news sentiment feature, latest macro feature, backtest metrics, backtest trades,
paper trading summary, paper trade ledger, paper portfolio history,
walk-forward summary, fold-level validation results, ML probability baseline
metrics and predictions, latest drift monitoring result, latest operations
health result, and selected Markdown report.

## 10. Preview Or Send Telegram Brief

Preview the daily brief without sending anything:

```powershell
python main.py send-telegram-daily --dry-run
```

The command uses the latest universe summary and, when available, the latest
paper portfolio, backtest, walk-forward validation, drift monitoring, and
operations health results.

To send the message, set these values in `.env`:

```text
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
TELEGRAM_TIMEOUT_SECONDS=10
TELEGRAM_MAX_RETRIES=2
TELEGRAM_RETRY_SLEEP_SECONDS=1
```

Then run:

```powershell
python main.py send-telegram-daily --send
```

The default mode is `--dry-run` so beginners can verify the message safely
before sending it to Telegram. Real sends retry transient network failures and
retryable Telegram API responses such as `429` and `5xx`.

## 11. Run The After-Market Daily Job

The daily job combines the operational steps into one command:

```text
run universe pipeline -> generate universe report -> run paper portfolio -> refresh operations health -> build Telegram brief
```

Run it safely in preview mode:

```powershell
python main.py run-daily-job --universe demo --start 2024-01-01 --end 2024-01-31 --telegram-dry-run
```

Paper portfolio simulation is enabled by default and never sends real orders.
It can be disabled for a faster operations check:

```powershell
python main.py run-daily-job --universe demo --start 2024-01-01 --end 2024-01-31 --no-paper-trading --telegram-dry-run
```

When `--start` is omitted, the job uses `--lookback-days` and today's date:

```powershell
python main.py run-daily-job --universe demo --lookback-days 60 --telegram-dry-run
```

After Telegram credentials are configured in `.env`, send the brief:

```powershell
python main.py run-daily-job --universe demo --lookback-days 60 --telegram-send
```

Windows Task Scheduler can run the same command after market close. Use the
full Python path from your virtual environment:

```powershell
schtasks /Create /SC DAILY /TN KRXAlphaDaily /ST 16:30 /TR "C:\Users\USER\Documents\Codex\2026-05-13\role-python-mlops-github-vscode-python\.venv\Scripts\python.exe C:\Users\USER\Documents\Codex\2026-05-13\role-python-mlops-github-vscode-python\main.py run-daily-job --universe demo --lookback-days 60 --telegram-send"
```

Keep the first scheduled runs in `--telegram-dry-run` mode if you are still
checking the output.

## 12. Check Operations Health

Operations health checks confirm that the latest local artifacts exist, are
readable, and are fresh enough for review:

```powershell
python main.py check-operations --skip-apis
```

This command does not call external APIs by default. It writes:

```text
data/signals/operations_health/
reports/monitoring/
```

After `.env` credentials are configured, you can include API connectivity
checks as well:

```powershell
python main.py check-operations --include-apis --skip-pykrx
```

Use `--include-pykrx` when you also want to verify live pykrx price collection.

## 13. Review Experiment Logs

Backtest, walk-forward validation, and daily job runs append metadata to:

```text
experiments/experiment_log.csv
```

Show recent rows from the CLI:

```powershell
python main.py show-experiments --limit 10
```

The CSV stores run ID, model version, parameters, metrics, date range, and the
main artifact path. The file is intentionally ignored by Git because it is a
local run artifact.

## 14. Detect Drift

Data drift compares numeric feature distributions between a reference dataset
and a current dataset:

```powershell
python main.py detect-data-drift --reference-path data/features/prices_daily/005930_20240101_20240131.parquet --current-path data/features/prices_daily/005380_20240101_20240131.parquet --columns rsi_14,volatility_5d,trading_value_change_5d
```

Performance drift compares recent experiment metrics with an earlier baseline:

```powershell
python main.py detect-performance-drift --run-type backtest --metric cumulative_return --baseline-window 1 --recent-window 1
```

Outputs:

```text
data/signals/drift/
reports/monitoring/
```

The MVP uses simple thresholds so the result is easy to explain in an interview.
It is an operations warning, not an automatic trading rule.

After drift detection has created a result, the Streamlit dashboard and
Telegram daily brief automatically include the latest drift status.
