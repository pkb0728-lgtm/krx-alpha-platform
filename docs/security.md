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
FRED_API_KEY
```

## Logging

Do not log API keys, account numbers, access tokens, or raw authentication
responses. Future collectors should sanitize errors before sending them to logs
or Telegram.

## Local Token Cache

KIS mock-investment access tokens can be cached locally at:

```text
.cache/kis_paper_token.json
```

This file can contain a live access token and must never be committed. The
repository ignores `.cache/` and explicitly ignores the KIS token cache path.
If credentials are rotated, delete this file and let the next KIS command issue
a fresh token.

## GitHub Secrets

When CI/CD or scheduled runs are added, production secrets should be stored in
GitHub Secrets, not in source files.

## OpenDART

Live OpenDART collection uses `DART_API_KEY`. The default examples use `--demo`
so that no secret is required for local portfolio review.

## Telegram

Telegram alerts use `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. The default
command mode is `--dry-run`, which prints the message locally without sending
anything. Use `--send` only after checking that `.env` contains the correct
bot token and chat ID.
