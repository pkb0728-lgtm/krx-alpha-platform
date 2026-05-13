# Security

## Secret Management

Never commit real API keys.

Use local `.env` files:

```text
.env
```

Only commit:

```text
.env.example
```

## Current Environment Variables

```text
DART_API_KEY
NAVER_CLIENT_ID
NAVER_CLIENT_SECRET
GEMINI_API_KEY
TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID
KIS_APP_KEY
KIS_APP_SECRET
KIS_ACCOUNT_NO
KRX_ID
KRX_PW
FRED_API_KEY
ALPHAVANTAGE_API_KEY
```

## Logging

Do not log API keys, account numbers, access tokens, or raw authentication
responses. Future collectors should sanitize errors before sending them to logs
or Telegram.

## GitHub Secrets

When CI/CD or scheduled runs are added, production secrets should be stored in
GitHub Secrets, not in source files.

## OpenDART

Live OpenDART collection uses `DART_API_KEY`. The default examples use `--demo`
so that no secret is required for local portfolio review.
