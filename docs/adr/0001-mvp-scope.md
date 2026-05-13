# ADR 0001: MVP Scope

## Status

Accepted

## Context

The project should become an explainable Korean stock investment decision support platform, but the first version must remain executable on a personal Windows PC.

## Decision

The MVP starts with local files, DuckDB/SQLite-ready storage abstractions, daily price data, reusable features, rule-based scoring, a simple ML baseline, backtesting, Streamlit dashboard, and Telegram notification hooks.

## Consequences

The early architecture favors clear module boundaries and testability over premature distributed infrastructure.

