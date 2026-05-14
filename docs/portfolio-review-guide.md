# Portfolio Review Guide

## One-Minute Explanation

This project is an explainable Korean stock decision-support platform. It
collects market data, validates it through contracts, builds reusable features,
generates explainable scores, applies risk filters, and produces reports and a
dashboard for human review.

## What To Show In An Interview

1. Run the universe pipeline:

```powershell
python main.py list-universe --universe demo
python main.py run-universe --universe demo --start 2024-01-01 --end 2024-01-31
python main.py screen-universe
```

2. Show the summary output:

```text
Ticker  Action         Confidence
005380  buy_candidate  72.83
005930  watch          63.78
000660  watch          59.37
```

3. Open the dashboard:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

4. Run one backtest:

```powershell
python main.py run-pipeline --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py analyze-regime --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py backtest-stock --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py walk-forward-backtest --ticker 005380 --start 2024-01-01 --end 2024-03-31 --train-size 20 --test-size 5 --step-size 5
python main.py build-ml-dataset --ticker 005380 --start 2024-01-01 --end 2024-03-31 --holding-days 5
python main.py train-ml-baseline --ticker 005380 --start 2024-01-01 --end 2024-03-31 --holding-days 5
```

5. Show OpenDART financial feature scoring:

```powershell
python main.py collect-dart-financials --ticker 005930 --year 2023 --report-code 11011 --demo
python main.py build-dart-financial-features --ticker 005930 --year 2023 --report-code 11011
python main.py collect-dart-disclosures --ticker 005930 --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-dart-disclosure-events --ticker 005930 --start 2024-01-01 --end 2024-01-31
python main.py collect-investor-flow --ticker 005930 --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-investor-flow-features --ticker 005930 --start 2024-01-01 --end 2024-01-31
python main.py run-pipeline --ticker 005930 --start 2024-01-01 --end 2024-01-31 --financial-year 2023 --event-start 2024-01-01 --event-end 2024-01-31 --flow-start 2024-01-01 --flow-end 2024-01-31
```

6. Preview the Telegram operations brief:

```powershell
python main.py send-telegram-daily --dry-run
```

7. Run the daily operations job:

```powershell
python main.py run-daily-job --universe demo --start 2024-01-01 --end 2024-01-31 --telegram-dry-run
```

This command also builds the auto-screener shortlist and runs paper portfolio
simulation by default. The paper output is virtual only and is included in the
Telegram preview so reviewers can see the operational loop without any broker
order being sent.

8. Show experiment tracking:

```powershell
python main.py show-experiments --limit 10
```

9. Show drift monitoring:

```powershell
python main.py detect-performance-drift --run-type backtest --metric cumulative_return --baseline-window 1 --recent-window 1
python main.py send-telegram-daily --dry-run
```

10. Explain why this is not just price prediction:

- The system uses data contracts.
- Each data layer is persisted.
- OpenDART collectors work in demo mode without exposing API keys.
- DART accounts are transformed into reusable financial ratios and reasons.
- Financial evidence is blended into the daily stock score when requested.
- DART disclosures are transformed into event scores and risk flags.
- Foreign and institutional net-buy flow is transformed into flow scores.
- Named universes make screening repeatable.
- Auto screener converts final signals into a human-review shortlist.
- Features are reusable.
- Market regime can block signals before they become buy candidates.
- Signals are explainable.
- Risk filters can block actions.
- Backtesting validates signals after costs and slippage.
- Walk-forward validation checks signal stability across rolling test windows.
- ML training datasets separate as-of features from future-return labels.
- The first ML baseline predicts positive forward-return probability and logs test metrics.
- Experiment tracking records parameters, model version, metrics, and artifacts.
- Drift monitoring warns when feature distributions or performance metrics change.
- Telegram brief turns the pipeline output, latest drift status, and operations health into an operations-ready alert.
- Daily job runner ties the workflow together for scheduled operation.
- Reports support human-in-the-loop review.

## Engineering Points

- `src/` package layout
- CLI with Typer
- Parquet data storage
- named universe registry
- OpenDART raw data collectors
- OpenDART financial feature scoring
- OpenDART disclosure event risk scoring
- investor flow feature scoring
- multi-factor score blending
- market regime analyzer
- testable modules
- CI-ready quality checks
- Streamlit dashboard with universe, auto screener, backtest, walk-forward, ML, drift, and operations health review
- scheduler-ready daily job runner
- daily-job integrated auto screener
- Telegram daily brief with dry-run safety
- separated pipeline orchestration
- simple backtest engine with explicit assumptions
- walk-forward validation with fold-level metrics
- leakage-aware ML dataset generation
- explainable probability baseline with feature weights
- CSV experiment tracking as an MLflow-ready stepping stone
- data drift and performance drift monitoring

## Next Portfolio Improvements

- Portfolio-level backtesting constraints
- Calibrate market regime thresholds with longer validation windows
- OpenDART disclosure event scoring features
- point-in-time release-date handling for financial and disclosure features
- Walk-forward validation for ML baseline predictions
- MLflow experiment tracking
- richer drift thresholds and scheduled Telegram warnings
- APScheduler daemon mode
