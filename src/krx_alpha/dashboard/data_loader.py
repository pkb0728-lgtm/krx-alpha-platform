from pathlib import Path
from typing import Any

import pandas as pd


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


def find_latest_drift_result(project_root: Path) -> Path | None:
    drift_dir = project_root / "data" / "signals" / "drift"
    if not drift_dir.exists():
        return None

    files = sorted(drift_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
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


def load_drift_result(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "drift_detected" not in frame.columns:
        return frame

    return frame.sort_values("drift_detected", ascending=False).reset_index(drop=True)


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
