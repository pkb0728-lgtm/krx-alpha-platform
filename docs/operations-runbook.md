# Operations Runbook

This runbook describes how to operate the KRX Alpha Platform as a local
decision-support system. It is designed for a beginner-friendly VSCode workflow
and for portfolio reviewers who want to see how the project would be used in
practice.

The platform does not place real orders. KIS integration is currently limited
to mock-investment token, balance, and paper review candidate generation.

## Daily Operator Flow

Use this flow after market close or when you want to refresh the demo universe:

```powershell
.\.venv\Scripts\Activate.ps1
python main.py run-daily-job --universe demo --start 2024-01-01 --end 2024-01-31 --kis-paper-candidates --telegram-dry-run
```

Expected high-level result:

```text
Daily scheduled job completed.
Success: 3
Screening passed: n/3
KIS review candidates: n/3
Operations health: ...operations_health_latest.parquet
Telegram: dry-run
```

The command performs:

1. Universe pipeline execution.
2. Universe Markdown report generation.
3. Auto screener candidate review queue generation.
4. Optional KIS mock-investment balance-based candidate sizing.
5. Paper portfolio simulation.
6. Operations health checks.
7. Telegram daily brief preview or send.
8. Experiment log update.

## First Checks After A Run

Check the terminal output in this order:

| Check | Healthy Output | Action If Not Healthy |
| --- | --- | --- |
| Universe | `Success` equals `Total` | Read the `error` column in the universe CSV. |
| Screening | `Screening passed` appears | Review the screening report for blocked reasons. |
| KIS candidates | `No order was sent` appears | Stop if this text is missing. |
| Operations health | `problems 0` in Telegram preview | Open `reports/monitoring/operations_health_latest.md`. |
| Telegram | `dry-run` during testing | Use `--telegram-send` only after reviewing the message. |

Useful files:

```text
data/signals/universe_summary_daily/
data/signals/screening_daily/
data/signals/kis_paper_candidates/
data/backtest/paper_portfolio_summary/
data/signals/operations_health/
reports/universe/
reports/screening/
reports/kis_paper_candidates/
reports/paper_trading/
reports/monitoring/
experiments/experiment_log.csv
```

## Dashboard Review

Open the dashboard in a separate terminal:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

Review sections in this order:

1. `Universe Ranking`: confirm each ticker completed successfully.
2. `Auto Screener`: inspect passed rows and review queue reasons.
3. `KIS Paper Review Candidates`: confirm candidate actions and `Orders sent = 0`.
4. `Paper Portfolio`: confirm trades, cash, and exposure.
5. `Drift` and `Operations Health`: confirm the local system is not stale or broken.

## How To Interpret Candidate Actions

| Action | Meaning | Human Decision |
| --- | --- | --- |
| `review_buy` | Candidate passed screening and sizing checks. | Review manually before any action. |
| `review_add` | Existing mock position is below target allocation. | Review manually before adding. |
| `manual_price_required` | Candidate is interesting but no reference price was available. | Refresh features or check price manually. |
| `hold_review` | Keep watching; no immediate candidate sizing. | Monitor only. |
| `skip` | Failed screening or risk conditions. | Do not act unless manually overridden with evidence. |

Important: a candidate action is not an order instruction. It is a queue for
human review.

## Human Review Checklist

Before treating any `review_buy` or `review_add` row as actionable, confirm:

- The ticker is not under trading halt, management issue, or abnormal disclosure.
- Liquidity is sufficient for the intended position size.
- Recent news does not contradict the signal.
- Market regime is not `bear` or `high_volatility`.
- The signal is not driven only by one weak factor.
- The expected position size is acceptable for account risk.
- There is no earnings, rate, or major event risk you are unwilling to hold through.
- The latest data date matches the intended review date.

If one of these checks fails, keep the row as `hold_review` or `skip` manually.

## Safe Command Modes

Use safe preview modes while developing:

```powershell
python main.py send-telegram-daily --dry-run
python main.py run-daily-job --universe demo --lookback-days 60 --telegram-dry-run
python main.py run-daily-job --universe demo --lookback-days 60 --kis-paper-candidates --telegram-dry-run
```

Only send Telegram after the preview looks correct:

```powershell
python main.py run-daily-job --universe demo --lookback-days 60 --kis-paper-candidates --telegram-send
```

This still does not place broker orders.

## Common Problems

### KIS Account Format Error

If the CLI says:

```text
KIS_ACCOUNT_NO must be 10 digits or formatted as 8 digits-2 digits.
```

Set the `.env` value as:

```text
KIS_ACCOUNT_NO=12345678-01
```

Use your own 8-digit mock account number. Do not commit `.env`.

### No KIS Review Candidates

This is often normal. It means the screener did not produce a strong enough
buy/add candidate. Review:

```text
reports/screening/
reports/kis_paper_candidates/
```

Typical reasons:

- `final_action_watch`
- `confidence_below_threshold`
- `action_not_allowed`
- `risk_blocked`

### Dashboard Does Not Show New Results

Refresh the browser. If it still shows old data, rerun:

```powershell
python main.py run-daily-job --universe demo --start 2024-01-01 --end 2024-01-31 --kis-paper-candidates --telegram-dry-run
```

Then refresh Streamlit again.

### Telegram SSL Error

See `docs/troubleshooting.md`. Do not disable SSL verification in source code.

## Weekly Maintenance

Once per week:

```powershell
pytest
python main.py check-operations --skip-apis
python main.py show-experiments --limit 20
```

Review whether:

- Tests still pass.
- Operations health is still clean.
- Experiment runs are being logged.
- Candidate rules are too strict or too loose.
- Drift checks are warning about stale behavior.

## Portfolio Interview Talking Points

This runbook demonstrates that the project is not only a collection of scripts.
It has an operational loop:

- repeatable daily command,
- persisted artifacts,
- dashboard inspection,
- dry-run alerting,
- broker-safe KIS mock workflow,
- health checks,
- experiment tracking,
- human-in-the-loop review.

The safest way to explain it is:

```text
The system does not predict exact prices or send orders. It builds an
explainable review queue, validates the data and operations state, and leaves
the final decision to a human operator.
```
