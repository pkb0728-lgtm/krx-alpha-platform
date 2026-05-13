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
data/signals/scores_daily/
data/signals/final_signals_daily/
reports/daily/
```

## 4. Run A Universe Pipeline

```powershell
python main.py run-universe --tickers 005930,000660,005380 --start 2024-01-01 --end 2024-01-31
python main.py generate-universe-report --start 2024-01-01 --end 2024-01-31
```

Output examples:

```text
data/signals/universe_summary_daily/universe_20240101_20240131.parquet
data/signals/universe_summary_daily/universe_20240101_20240131.csv
reports/universe/universe_20240101_20240131.md
```

## 5. Open The Dashboard

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

