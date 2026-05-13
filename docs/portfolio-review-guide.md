# Portfolio Review Guide

## One-Minute Explanation

This project is an explainable Korean stock decision-support platform. It
collects market data, validates it through contracts, builds reusable features,
generates explainable scores, applies risk filters, and produces reports and a
dashboard for human review.

## What To Show In An Interview

1. Run the universe pipeline:

```powershell
python main.py run-universe --tickers 005930,000660,005380 --start 2024-01-01 --end 2024-01-31
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
python main.py backtest-stock --ticker 005380 --start 2024-01-01 --end 2024-03-31
```

5. Explain why this is not just price prediction:

- The system uses data contracts.
- Each data layer is persisted.
- Features are reusable.
- Signals are explainable.
- Risk filters can block actions.
- Backtesting validates signals after costs and slippage.
- Reports support human-in-the-loop review.

## Engineering Points

- `src/` package layout
- CLI with Typer
- Parquet data storage
- testable modules
- CI-ready quality checks
- Streamlit dashboard with universe and backtest review
- separated pipeline orchestration
- simple backtest engine with explicit assumptions

## Next Portfolio Improvements

- Walk-forward backtesting and portfolio-level constraints
- Market regime detection
- OpenDART disclosure features
- Investor flow features
- ML baseline with walk-forward validation
- MLflow experiment tracking
- Telegram notification summary
