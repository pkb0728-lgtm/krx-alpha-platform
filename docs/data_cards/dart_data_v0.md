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
```

## Demo Mode

The project includes demo OpenDART responses so tests and portfolio demos run
without an API key.

## Limitations

- Corp-code mapping is currently a small built-in dictionary.
- Financial statement data is stored in the raw layer only.
- OpenDART data is not yet converted into scoring features.
- Live API usage requires `DART_API_KEY`.

## Next Steps

- Add a corp-code ZIP downloader/parser.
- Convert accounts into reusable financial features.
- Add disclosure event scoring.
