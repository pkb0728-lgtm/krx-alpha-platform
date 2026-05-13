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
```

5. Show OpenDART financial feature scoring:

```powershell
python main.py collect-dart-financials --ticker 005930 --year 2023 --report-code 11011 --demo
python main.py build-dart-financial-features --ticker 005930 --year 2023 --report-code 11011
python main.py collect-dart-disclosures --ticker 005930 --start 2024-01-01 --end 2024-01-31 --demo
python main.py build-dart-disclosure-events --ticker 005930 --start 2024-01-01 --end 2024-01-31
python main.py run-pipeline --ticker 005930 --start 2024-01-01 --end 2024-01-31 --financial-year 2023 --event-start 2024-01-01 --event-end 2024-01-31
```

6. Explain why this is not just price prediction:

- The system uses data contracts.
- Each data layer is persisted.
- OpenDART collectors work in demo mode without exposing API keys.
- DART accounts are transformed into reusable financial ratios and reasons.
- Financial evidence is blended into the daily stock score when requested.
- DART disclosures are transformed into event scores and risk flags.
- Named universes make screening repeatable.
- Features are reusable.
- Market regime can block signals before they become buy candidates.
- Signals are explainable.
- Risk filters can block actions.
- Backtesting validates signals after costs and slippage.
- Reports support human-in-the-loop review.

## Engineering Points

- `src/` package layout
- CLI with Typer
- Parquet data storage
- named universe registry
- OpenDART raw data collectors
- OpenDART financial feature scoring
- OpenDART disclosure event risk scoring
- multi-factor score blending
- market regime analyzer
- testable modules
- CI-ready quality checks
- Streamlit dashboard with universe and backtest review
- separated pipeline orchestration
- simple backtest engine with explicit assumptions

## Next Portfolio Improvements

- Walk-forward backtesting and portfolio-level constraints
- Calibrate market regime thresholds with longer validation windows
- OpenDART disclosure event scoring features
- point-in-time release-date handling for financial and disclosure features
- Investor flow features
- ML baseline with walk-forward validation
- MLflow experiment tracking
- Telegram notification summary
