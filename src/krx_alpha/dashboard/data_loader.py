from pathlib import Path
from typing import Any

import pandas as pd

SCREENING_PRIORITY_ORDER = {
    "high": 0,
    "medium": 1,
    "watchlist": 2,
    "low": 3,
    "blocked": 4,
}


def find_latest_universe_summary(project_root: Path) -> Path | None:
    summary_dir = project_root / "data" / "signals" / "universe_summary_daily"
    if not summary_dir.exists():
        return None

    files = sorted(summary_dir.glob("universe_*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_backtest_metrics(project_root: Path) -> Path | None:
    metrics_dir = project_root / "data" / "backtest" / "metrics"
    if not metrics_dir.exists():
        return None

    files = sorted(metrics_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_walk_forward_summary(project_root: Path) -> Path | None:
    summary_dir = project_root / "data" / "backtest" / "walk_forward_summary"
    if not summary_dir.exists():
        return None

    files = sorted(summary_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_paper_summary(project_root: Path) -> Path | None:
    summary_dir = project_root / "data" / "backtest" / "paper_summary"
    if not summary_dir.exists():
        return None

    files = sorted(summary_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_paper_portfolio_summary(project_root: Path) -> Path | None:
    summary_dir = project_root / "data" / "backtest" / "paper_portfolio_summary"
    if not summary_dir.exists():
        return None

    files = sorted(summary_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_drift_result(project_root: Path) -> Path | None:
    drift_dir = project_root / "data" / "signals" / "drift"
    if not drift_dir.exists():
        return None

    files = sorted(drift_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_operations_health(project_root: Path) -> Path | None:
    health_dir = project_root / "data" / "signals" / "operations_health"
    if not health_dir.exists():
        return None

    files = sorted(health_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_api_health(project_root: Path) -> Path | None:
    health_dir = project_root / "data" / "signals" / "api_health"
    if not health_dir.exists():
        return None

    files = sorted(health_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_screening_result(project_root: Path) -> Path | None:
    screening_dir = project_root / "data" / "signals" / "screening_daily"
    if not screening_dir.exists():
        return None

    files = sorted(screening_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_ml_metrics(project_root: Path) -> Path | None:
    metrics_dir = project_root / "data" / "signals" / "ml_metrics"
    if not metrics_dir.exists():
        return None

    files = sorted(metrics_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_news_sentiment(project_root: Path) -> Path | None:
    news_dir = project_root / "data" / "features" / "news_sentiment_daily"
    if not news_dir.exists():
        return None

    files = sorted(news_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def find_latest_macro_features(project_root: Path) -> Path | None:
    macro_dir = project_root / "data" / "features" / "macro_fred_daily"
    if not macro_dir.exists():
        return None

    files = sorted(macro_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
    return files[-1] if files else None


def load_universe_summary(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame

    return frame.sort_values(
        ["status", "latest_confidence_score"],
        ascending=[False, False],
    ).reset_index(drop=True)


def load_backtest_metrics(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame

    return frame.sort_values(
        ["cumulative_return", "sharpe_ratio"],
        ascending=[False, False],
    ).reset_index(drop=True)


def load_backtest_trades(metrics_path: Path) -> Any:
    trades_path = metrics_path.parents[1] / "trades" / metrics_path.name
    if not trades_path.exists():
        return pd.DataFrame()

    return pd.read_parquet(trades_path)


def load_walk_forward_summary(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame

    return frame.sort_values(
        ["compounded_return", "positive_fold_ratio", "average_sharpe_ratio"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def load_walk_forward_folds(summary_path: Path) -> Any:
    folds_path = summary_path.parents[1] / "walk_forward_folds" / summary_path.name
    if not folds_path.exists():
        return pd.DataFrame()

    frame = pd.read_parquet(folds_path)
    if frame.empty:
        return frame

    return frame.sort_values("fold").reset_index(drop=True)


def load_paper_summary(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame

    return frame.sort_values("generated_at", ascending=False).reset_index(drop=True)


def load_paper_trades(summary_path: Path) -> Any:
    ledger_path = summary_path.parents[1] / "paper_trade_ledger" / summary_path.name
    if not ledger_path.exists():
        return pd.DataFrame()

    frame = pd.read_parquet(ledger_path)
    if frame.empty:
        return frame

    return frame.sort_values(["date", "side"]).reset_index(drop=True)


def load_paper_portfolio_summary(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame

    return frame.sort_values("generated_at", ascending=False).reset_index(drop=True)


def load_paper_portfolio_trades(summary_path: Path) -> Any:
    ledger_path = summary_path.parents[1] / "paper_portfolio_trade_ledger" / summary_path.name
    if not ledger_path.exists():
        return pd.DataFrame()

    frame = pd.read_parquet(ledger_path)
    if frame.empty:
        return frame

    return frame.sort_values(["date", "ticker", "side"]).reset_index(drop=True)


def load_paper_portfolio_history(project_root: Path) -> Any:
    summary_dir = project_root / "data" / "backtest" / "paper_portfolio_summary"
    if not summary_dir.exists():
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for path in sorted(summary_dir.glob("*.parquet"), key=lambda value: value.stat().st_mtime):
        frame = pd.read_parquet(path)
        if frame.empty:
            continue
        frame = frame.copy()
        frame["summary_file"] = path.name
        frame["summary_mtime"] = pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    history = pd.concat(frames, ignore_index=True)
    history["generated_at"] = pd.to_datetime(history["generated_at"], errors="coerce")
    history = history.sort_values(["universe", "generated_at", "summary_file"]).reset_index(
        drop=True
    )
    history["run_sequence"] = history.groupby("universe").cumcount() + 1
    history["equity_high_watermark"] = history.groupby("universe")["ending_equity"].cummax()
    history["drawdown"] = (history["ending_equity"] / history["equity_high_watermark"] - 1).fillna(
        0.0
    )
    history["cumulative_trade_count"] = history.groupby("universe")["trade_count"].cumsum()
    return history


def load_drift_result(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "drift_detected" not in frame.columns:
        return frame

    return frame.sort_values("drift_detected", ascending=False).reset_index(drop=True)


def load_operations_health(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "severity" not in frame.columns:
        return frame

    return frame.sort_values(["severity", "category", "check_name"]).reset_index(drop=True)


def load_api_health(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "ok" not in frame.columns:
        return frame

    result = frame.copy()
    result["_ok_order"] = result["ok"].map({False: 0, True: 1}).fillna(2)
    return (
        result.sort_values(["_ok_order", "api"]).drop(columns=["_ok_order"]).reset_index(drop=True)
    )


def load_screening_result(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "screen_score" not in frame.columns:
        return frame

    if "review_priority" in frame.columns:
        frame = frame.copy()
        frame["_priority_order"] = frame["review_priority"].map(SCREENING_PRIORITY_ORDER).fillna(99)
        return (
            frame.sort_values(
                ["_priority_order", "screen_score", "confidence_score"],
                ascending=[True, False, False],
            )
            .drop(columns=["_priority_order"])
            .reset_index(drop=True)
        )

    return frame.sort_values(
        ["passed", "screen_score", "confidence_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def filter_screening_result(
    frame: Any,
    priorities: list[str] | None = None,
    status_reasons: list[str] | None = None,
    passed_only: bool = False,
) -> Any:
    result = frame.copy()
    if passed_only and "passed" in result.columns:
        result = result[result["passed"]]

    priority_filter = set(priorities or [])
    if priority_filter and "review_priority" in result.columns:
        result = result[result["review_priority"].astype(str).isin(priority_filter)]

    status_filter = set(status_reasons or [])
    if status_filter and "screen_status_reason" in result.columns:
        result = result[result["screen_status_reason"].astype(str).isin(status_filter)]

    return result.reset_index(drop=True)


def screening_review_queue(frame: Any, limit: int = 5) -> Any:
    if frame.empty or "passed" not in frame.columns:
        return frame.head(0)

    result = frame.copy()
    passed_mask = _screening_passed_mask(result["passed"])
    result = result[~passed_mask]
    if result.empty:
        return result.reset_index(drop=True)

    if "review_priority" in result.columns:
        result["_priority_order"] = (
            result["review_priority"].map(SCREENING_PRIORITY_ORDER).fillna(99)
        )
        result = result.sort_values(
            ["_priority_order", "screen_score", "confidence_score"],
            ascending=[True, False, False],
        ).drop(columns=["_priority_order"])

    return result.head(limit).reset_index(drop=True)


def _screening_passed_mask(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    return series.fillna(False).astype(str).str.lower().isin({"true", "1", "yes"})


def load_ml_metrics(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "split" not in frame.columns:
        return frame

    split_order = {"test": 0, "train": 1}
    frame = frame.copy()
    frame["_split_order"] = frame["split"].map(split_order).fillna(2)
    return frame.sort_values("_split_order").drop(columns=["_split_order"]).reset_index(drop=True)


def load_ml_predictions(metrics_path: Path) -> Any:
    predictions_path = metrics_path.parents[1] / "ml_predictions" / metrics_path.name
    if not predictions_path.exists():
        return pd.DataFrame()

    frame = pd.read_parquet(predictions_path)
    if frame.empty or "probability_positive_forward_return" not in frame.columns:
        return frame

    split_order = {"test": 0, "train": 1}
    frame = frame.copy()
    frame["_split_order"] = frame["split"].map(split_order).fillna(2)
    return (
        frame.sort_values(
            ["_split_order", "probability_positive_forward_return"],
            ascending=[True, False],
        )
        .drop(columns=["_split_order"])
        .reset_index(drop=True)
    )


def load_news_sentiment(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame

    return frame.sort_values(
        ["date", "news_score"],
        ascending=[False, False],
    ).reset_index(drop=True)


def load_macro_features(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return frame

    return frame.sort_values("date", ascending=False).reset_index(drop=True)


def load_markdown(path: Path) -> str:
    if not path.exists():
        return "Report file does not exist."
    return path.read_text(encoding="utf-8")


def action_counts(frame: Any) -> Any:
    if frame.empty or "latest_action" not in frame.columns:
        return pd.DataFrame(columns=["latest_action", "count"])

    return (
        frame.groupby("latest_action", dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
