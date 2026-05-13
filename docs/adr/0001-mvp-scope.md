# ADR 0001: MVP Scope

## Status

Accepted

## Context

The project should become an explainable Korean stock investment decision support platform, but the first version must remain executable on a personal Windows PC.

## Decision

The MVP starts with local files, DuckDB/SQLite-ready storage abstractions,
daily price data, OpenDART demo ingestion, reusable price and financial
features, investor flow features, rule-based scoring, risk filtering, simple
backtesting, Markdown reports, and a Streamlit dashboard. Machine learning,
Telegram notifications, and richer data sources remain planned extensions.

## Consequences

The early architecture favors clear module boundaries and testability over premature distributed infrastructure.
