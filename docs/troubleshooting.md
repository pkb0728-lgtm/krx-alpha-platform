# Troubleshooting

## Public Price Collection Warning

The current MVP uses public OHLCV collection and does not require exchange
login credentials. If rows are saved under `data/raw/prices_daily`, the price
collection step is working.

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
python main.py run-universe --universe demo --start 2024-01-01 --end 2024-01-31
```

Then restart or refresh Streamlit:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

## Tests Fail After Schema Changes

When a new required column is added to a data contract, update both production
code and test fixtures. This is expected because contracts protect the pipeline
from silent schema drift.

## Telegram Token Or Chat ID Missing

Preview mode does not need Telegram credentials:

```powershell
python main.py send-telegram-daily --dry-run
```

To actually send a message, add these values to `.env`:

```text
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

Then run:

```powershell
python main.py send-telegram-daily --send
```

## Telegram SSL Certificate Verification Failed

If `--send` fails with:

```text
CERTIFICATE_VERIFY_FAILED
self-signed certificate in certificate chain
```

the Telegram token may still be correct. This usually means Python cannot
verify the HTTPS certificate chain on the current network.

Common causes:

- antivirus HTTPS inspection
- company/school proxy certificate
- missing or outdated local certificate trust settings

Recommended checks:

```powershell
python -m pip install --upgrade certifi
python main.py send-telegram-daily --send
```

If it still fails, try another trusted network or disable HTTPS inspection for
`api.telegram.org` in your antivirus/proxy settings. Do not disable SSL
verification in code.
