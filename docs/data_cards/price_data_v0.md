# Data Card: Price Data V0

## Source

Current MVP price data is collected through `pykrx`.

## Coverage

The current implementation supports Korean stock tickers and daily OHLCV-like
data for user-selected date ranges.

## Transformations

- normalize ticker to six digits
- standardize column names
- estimate trading value when source data does not provide it
- calculate daily returns
- calculate range percentage
- generate moving averages, RSI, and volatility features

## Known Issues

- Public OHLCV collection does not require storing exchange login credentials.
- Trading value may be estimated as `close * volume` when missing.
- The MVP does not yet handle corporate actions beyond source-adjusted prices.

## Data Leakage Notes

The project stores `as_of_date` and timestamp columns to support future
point-in-time validation.
