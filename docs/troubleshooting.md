# Troubleshooting

## `KRX login failed`

You may see:

```text
KRX login failed: KRX_ID or KRX_PW environment variable is not set.
```

For the current public OHLCV pipeline, this warning can appear while the data
still downloads correctly. If rows are saved, the MVP pipeline is working.

## `ModuleNotFoundError: No module named 'streamlit'`

Install dashboard dependencies:

```powershell
python -m pip install -e ".[dashboard]"
```

## PowerShell Cannot Activate `.venv`

Run once:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

Then:

```powershell
.\.venv\Scripts\Activate.ps1
```

## No Universe Summary Found In Dashboard

Run:

```powershell
python main.py run-universe --tickers 005930,000660,005380 --start 2024-01-01 --end 2024-01-31
```

Then restart or refresh Streamlit:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

## Tests Fail After Schema Changes

When a new required column is added to a data contract, update both production
code and test fixtures. This is expected because contracts protect the pipeline
from silent schema drift.

