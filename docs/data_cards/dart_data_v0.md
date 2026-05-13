# Data Card: OpenDART Data V0

## Purpose

OpenDART data adds company overview, financial statement, and disclosure
context to the price-based MVP.

## Source

- OpenDART company overview: `company.json`
- OpenDART single-company major accounts: `fnlttSinglAcnt.json`
- OpenDART disclosure search: `list.json`

## Current Storage

```text
data/raw/dart_company/
data/raw/dart_financials/
data/raw/dart_disclosures/
data/features/dart_financials/
data/features/dart_disclosure_events/
```

## Demo Mode

The project includes demo OpenDART responses so tests and portfolio demos run
without an API key.

## Limitations

- Corp-code mapping is currently a small built-in dictionary.
- Financial statement data is converted into reusable financial features.
- Financial features can be blended into the daily scoring flow with
  `--financial-year`.
- Disclosure data is converted into conservative event scores and risk flags.
- Disclosure event features can be blended with `--event-start` and
  `--event-end`.
- Live API usage requires `DART_API_KEY`.

## Next Steps

- Add a corp-code ZIP downloader/parser.
- Add stricter point-in-time release-date handling for financial statements.
- Expand event rules with more Korean disclosure categories.
