# KRX Alpha Platform

Explainable Korean stock investment decision-support platform built with Python.

This project is not a simple stock price prediction script. It is a small but
operational financial data platform that demonstrates data collection, ETL,
data validation, feature engineering, explainable scoring, risk filtering,
backtesting, report generation, and a Streamlit dashboard.

> This project is for education and portfolio review. It is not investment advice.

## What This Project Shows

- Python backend development with a modular `src/` layout
- Korean stock data collection with `pykrx`
- ETL data layers: `raw`, `processed`, `features`, `signals`, `backtest`
- Data contracts and validation checks
- Named universe management for repeatable screening
- Technical feature engineering
- Explainable rule-based scoring
- Risk filtering before final signals
- Simple signal backtesting with costs and slippage
- Markdown reports for single-stock and universe screening
- Streamlit dashboard for universe, report, and backtest review
- Tests, linting, type checking, Docker, and GitHub Actions

## Current MVP

The current MVP supports this end-to-end flow:

```text
select named universe
-> collect price data
-> process raw data
-> build price features
-> score each stock
-> apply risk filters
-> generate final signals
-> backtest buy-candidate signals
-> generate Markdown reports
-> view results in Streamlit
```

Example universe result:

```text
Ticker  Action         Confidence
005380  buy_candidate  72.83
005930  watch          63.78
000660  watch          59.37
```

## Architecture

```mermaid
flowchart LR
    A["pykrx collector"] --> B["raw parquet"]
    B --> C["price processor"]
    C --> D["processed parquet"]
    D --> E["feature builder"]
    E --> F["feature store"]
    F --> G["price scorer"]
    G --> H["daily scores"]
    H --> I["signal engine"]
    F --> I
    I --> J["final signals"]
    H --> K["daily report"]
    F --> K
    J --> L["universe summary"]
    L --> M["Streamlit dashboard"]
    J --> N["backtest engine"]
    D --> N
    N --> O["backtest report"]
    P["universe registry"] --> A
    P --> L
```

## Project Structure

```text
src/krx_alpha/
  collectors/    data collection
  processors/    raw to processed ETL
  features/      feature engineering
  scoring/       explainable scoring
  risk/          risk filters
  signals/       final signal generation
  backtest/      signal backtesting
  universe/      named universe definitions
  reports/       Markdown report generation
  dashboard/     Streamlit dashboard
  pipelines/     single-stock and universe pipelines
  contracts/     dataset validation rules
  database/      file paths and storage helpers
  configs/       environment settings
  utils/         logging and utilities
```

## Quick Start

Use PowerShell in VSCode.

```powershell
cd C:\Users\USER\Documents\Codex\2026-05-13\role-python-mlops-github-vscode-python
.\.venv\Scripts\Activate.ps1
python main.py doctor
```

If you start from a fresh clone:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[data,dashboard,dev]"
```

## Run The Pipeline

Single stock:

```powershell
python main.py run-pipeline --ticker 005930 --start 2024-01-01 --end 2024-01-31
```

Multiple stocks:

```powershell
python main.py list-universe --universe all
python main.py list-universe --universe demo
python main.py run-universe --universe demo --start 2024-01-01 --end 2024-01-31
python main.py generate-universe-report --start 2024-01-01 --end 2024-01-31
```

Manual tickers are still supported:

```powershell
python main.py run-universe --tickers 005930,000660,005380 --start 2024-01-01 --end 2024-01-31
```

Backtest one stock after running its pipeline:

```powershell
python main.py backtest-stock --ticker 005380 --start 2024-01-01 --end 2024-03-31
```

Dashboard:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

Open:

```text
http://localhost:8501
```

## Quality Checks

```powershell
ruff check .
mypy src
pytest
```

Current verified result:

```text
pytest: 24 passed
ruff: all checks passed
mypy: no issues found
```

## Data Outputs

```text
data/raw/prices_daily/
data/processed/universe/
data/processed/prices_daily/
data/features/prices_daily/
data/signals/scores_daily/
data/signals/final_signals_daily/
data/signals/universe_summary_daily/
data/backtest/trades/
data/backtest/metrics/
reports/daily/
reports/universe/
reports/backtest/
```

## Documentation

- [Architecture](docs/architecture.md)
- [Usage Guide](docs/usage.md)
- [Data Design](docs/data-design.md)
- [Scoring and Risk](docs/scoring-and-risk.md)
- [Result Example](docs/results-example.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Security](docs/security.md)
- [Portfolio Review Guide](docs/portfolio-review-guide.md)
- [ADR 0001: MVP Scope](docs/adr/0001-mvp-scope.md)

## Security

Do not commit real API keys. Use `.env` locally and keep `.env.example` as the
only committed environment file.

## Roadmap

- Add dynamic KOSPI200/KOSDAQ150 universe collectors and liquidity filters
- Add OpenDART financial/disclosure features
- Add investor flow and short-selling features
- Expand backtesting with walk-forward validation and portfolio-level constraints
- Add ML baselines with walk-forward validation
- Add MLflow experiment tracking
- Add Telegram daily notifications
- Add Docker Compose dashboard profile
