# Architecture

## Goal

KRX Alpha Platform is designed as an explainable decision-support system for
Korean equities. The platform prioritizes reproducible data pipelines, risk
control, and human review over direct price prediction.

## System Diagram

```mermaid
flowchart TD
    U["Universe registry"] --> A
    A["Collector: pykrx"] --> B["Raw data layer"]
    A --> W["Raw investor flow layer"]
    X["Collector: OpenDART"] --> Y["Raw DART data layer"]
    Y --> Z["DART financial feature builder"]
    Y --> V["DART disclosure event builder"]
    Z --> G
    V --> G
    W --> S["Investor flow feature builder"]
    S --> G
    B --> C["Data contract checks"]
    Y --> C
    C --> D["Processor"]
    D --> E["Processed data layer"]
    E --> F["Feature builder"]
    F --> G["Feature store"]
    G --> R["Market regime analyzer"]
    G --> H["Multi-factor scoring engine"]
    H --> I["Risk filter"]
    I --> J["Final signal engine"]
    J --> K["Reports"]
    J --> L["Universe summary"]
    L --> DJ["Daily job runner"]
    L --> M["Streamlit dashboard"]
    DJ --> M
    L --> T["Telegram brief"]
    J --> N["Backtest engine"]
    E --> N
    N --> Q["Walk-forward validation"]
    N --> EL["Experiment log"]
    N --> O["Backtest report"]
    Q --> M
    Q --> O
    Q --> EL
    DJ --> EL
    R --> P["Regime report"]
```

## Module Responsibilities

| Module | Responsibility |
| --- | --- |
| `collectors` | Collect raw price, OpenDART, and other source data from APIs or libraries. |
| `processors` | Clean raw data and create processed datasets. |
| `features` | Build reusable features for scoring and models. |
| `regime` | Classify market context before reviewing signals. |
| `contracts` | Validate schemas, required columns, ranges, and duplicates. |
| `scoring` | Generate explainable technical and risk scores. |
| `risk` | Block or reduce signals when risk conditions are weak. |
| `signals` | Convert scores into final actions. |
| `backtest` | Validate historical signal behavior with cost, slippage, and walk-forward folds. |
| `experiments` | Append run parameters, model version, metrics, and artifact links to a CSV log. |
| `universe` | Manage named ticker lists for repeatable screening. |
| `reports` | Generate Markdown reports for human review. |
| `dashboard` | Display universe, backtest, and walk-forward validation results through Streamlit. |
| `scheduler` | Orchestrate after-market daily jobs for universe runs, reports, and alerts. |
| `telegram` | Build and send compact daily operations briefs through Telegram. |
| `pipelines` | Orchestrate single-stock and universe workflows. |

## Why This Architecture

The project uses explicit data layers because financial systems need traceable
inputs and outputs. Each layer is saved separately so bugs can be isolated:

```text
universe -> raw -> processed -> features -> regime/scores -> final signals -> reports/backtest
OpenDART raw -> financial/event features -> multi-factor scoring
```

This separation helps avoid data leakage during backtesting and prepares the
project for walk-forward machine learning validation later.
