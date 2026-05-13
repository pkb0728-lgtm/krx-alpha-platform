# Result Example

## Universe Screening

Example command:

```powershell
python main.py run-universe --tickers 005930,000660,005380 --start 2024-01-01 --end 2024-01-31
```

Example output:

```text
Universe pipeline completed.
Total: 3
Success: 3
Failed: 0

Ticker  Status   Action         Confidence
005380  success  buy_candidate  72.83
005930  success  watch          63.78
000660  success  watch          59.37
```

## Interpretation

The output does not mean "buy this stock." It means the stock passed the current
rule-based screening and risk-filter logic for the selected date range.

| Action | Meaning |
| --- | --- |
| `buy_candidate` | Candidate passed score and risk filters. |
| `watch` | Worth monitoring, but confirmation is still needed. |
| `blocked` | Risk filter blocked the signal. |
| `avoid` | Weak evidence or weak score. |

## Dashboard

The Streamlit dashboard displays:

- number of tickers
- success and failed counts
- ranked candidate table
- action distribution chart
- latest backtest metrics
- latest backtest trades
- selected stock Markdown report

Run:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

## Backtest

Example command:

```powershell
python main.py run-pipeline --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py backtest-stock --ticker 005380 --start 2024-01-01 --end 2024-03-31
```

Example output:

```text
Backtest completed.
Trades: 7
Win rate: 57.14%
Cumulative return: 78.67%
Max drawdown: -10.35%
Sharpe ratio: 4.33
```

This is a simple MVP backtest for signal validation. It is not a production
execution simulator.
