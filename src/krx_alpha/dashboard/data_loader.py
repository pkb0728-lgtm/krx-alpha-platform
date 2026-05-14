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

KIS_CANDIDATE_ACTION_ORDER = {
    "review_buy": 0,
    "review_add": 1,
    "manual_price_required": 2,
    "hold_review": 3,
    "skip": 4,
}

STOCK_NAME_MAP = {
    "000270": "기아",
    "000660": "SK하이닉스",
    "035420": "NAVER",
    "035720": "카카오",
    "042700": "한미반도체",
    "005380": "현대차",
    "005930": "삼성전자",
    "051910": "LG화학",
    "055550": "신한지주",
    "068270": "셀트리온",
    "105560": "KB금융",
}

FINAL_ACTION_KO = {
    "buy_candidate": "매수 검토",
    "watch": "관망",
    "hold": "보유/중립",
    "avoid": "회피",
    "blocked": "리스크 차단",
}

CANDIDATE_ACTION_KO = {
    "review_buy": "매수 검토",
    "review_add": "추가매수 검토",
    "manual_price_required": "가격 확인 필요",
    "hold_review": "관망",
    "skip": "제외",
}

SCREEN_STATUS_KO = {
    "passed": "조건 통과",
    "confidence_below_threshold": "신뢰도 부족",
    "confidence_and_score_below_threshold": "신뢰도/점수 부족",
    "score_below_threshold": "점수 부족",
    "action_not_allowed": "허용되지 않은 판단",
    "risk_blocked": "리스크 차단",
    "signal_file_missing_or_empty": "신호 파일 없음",
}

REVIEW_PRIORITY_KO = {
    "high": "높음",
    "medium": "중간",
    "watchlist": "관찰",
    "low": "낮음",
    "blocked": "차단",
}

REGIME_KO = {
    "bull": "상승장",
    "bullish": "상승 우위",
    "uptrend": "상승 추세",
    "bear": "하락장",
    "downtrend": "하락 추세",
    "sideways": "횡보장",
    "neutral": "중립",
    "rebound": "반등",
    "high_volatility": "고변동성",
    "insufficient_data": "데이터 부족",
    "unknown": "알 수 없음",
}

STATUS_KO = {
    "success": "성공",
    "failed": "실패",
    "OK": "정상",
    "WARN": "주의",
    "STALE": "오래됨",
    "MISSING": "누락",
    "EMPTY": "비어 있음",
    "FAILED": "실패",
    "filled": "체결",
    "skipped": "건너뜀",
    "pending": "대기",
    "blocked": "차단",
}

SIDE_KO = {
    "buy": "매수",
    "sell": "매도",
}

SPLIT_KO = {
    "train": "학습",
    "test": "검증",
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


def find_latest_kis_paper_candidates(project_root: Path) -> Path | None:
    candidate_dir = project_root / "data" / "signals" / "kis_paper_candidates"
    if not candidate_dir.exists():
        return None

    files = sorted(candidate_dir.glob("*.parquet"), key=lambda path: path.stat().st_mtime)
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
        return _with_readable_columns(frame)

    result = frame.sort_values(
        ["status", "latest_confidence_score"],
        ascending=[False, False],
    ).reset_index(drop=True)
    return _with_readable_columns(result)


def load_backtest_metrics(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return _with_readable_columns(frame)

    result = frame.sort_values(
        ["cumulative_return", "sharpe_ratio"],
        ascending=[False, False],
    ).reset_index(drop=True)
    return _with_readable_columns(result)


def load_backtest_trades(metrics_path: Path) -> Any:
    trades_path = metrics_path.parents[1] / "trades" / metrics_path.name
    if not trades_path.exists():
        return pd.DataFrame()

    return _with_readable_columns(pd.read_parquet(trades_path))


def load_walk_forward_summary(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return _with_readable_columns(frame)

    result = frame.sort_values(
        ["compounded_return", "positive_fold_ratio", "average_sharpe_ratio"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    return _with_readable_columns(result)


def load_walk_forward_folds(summary_path: Path) -> Any:
    folds_path = summary_path.parents[1] / "walk_forward_folds" / summary_path.name
    if not folds_path.exists():
        return pd.DataFrame()

    frame = pd.read_parquet(folds_path)
    if frame.empty:
        return _with_readable_columns(frame)

    return _with_readable_columns(frame.sort_values("fold").reset_index(drop=True))


def load_paper_summary(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return _with_readable_columns(frame)

    return _with_readable_columns(
        frame.sort_values("generated_at", ascending=False).reset_index(drop=True)
    )


def load_paper_trades(summary_path: Path) -> Any:
    ledger_path = summary_path.parents[1] / "paper_trade_ledger" / summary_path.name
    if not ledger_path.exists():
        return pd.DataFrame()

    frame = pd.read_parquet(ledger_path)
    if frame.empty:
        return _with_readable_columns(frame)

    return _with_readable_columns(frame.sort_values(["date", "side"]).reset_index(drop=True))


def load_paper_portfolio_summary(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return _with_readable_columns(frame)

    return _with_readable_columns(
        frame.sort_values("generated_at", ascending=False).reset_index(drop=True)
    )


def load_paper_portfolio_trades(summary_path: Path) -> Any:
    ledger_path = summary_path.parents[1] / "paper_portfolio_trade_ledger" / summary_path.name
    if not ledger_path.exists():
        return pd.DataFrame()

    frame = pd.read_parquet(ledger_path)
    if frame.empty:
        return _with_readable_columns(frame)

    return _with_readable_columns(
        frame.sort_values(["date", "ticker", "side"]).reset_index(drop=True)
    )


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
    return _with_readable_columns(history)


def load_drift_result(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "drift_detected" not in frame.columns:
        return _with_readable_columns(frame)

    result = frame.sort_values("drift_detected", ascending=False).reset_index(drop=True)
    return _with_readable_columns(result)


def load_operations_health(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "severity" not in frame.columns:
        return _with_readable_columns(frame)

    result = frame.sort_values(["severity", "category", "check_name"]).reset_index(drop=True)
    return _with_readable_columns(result)


def load_api_health(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "ok" not in frame.columns:
        return _with_readable_columns(frame)

    result = frame.copy()
    result["_ok_order"] = result["ok"].map({False: 0, True: 1}).fillna(2)
    result = (
        result.sort_values(["_ok_order", "api"]).drop(columns=["_ok_order"]).reset_index(drop=True)
    )
    return _with_readable_columns(result)


def load_screening_result(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "screen_score" not in frame.columns:
        return _with_readable_columns(frame)

    if "review_priority" in frame.columns:
        frame = frame.copy()
        frame["_priority_order"] = frame["review_priority"].map(SCREENING_PRIORITY_ORDER).fillna(99)
        result = (
            frame.sort_values(
                ["_priority_order", "screen_score", "confidence_score"],
                ascending=[True, False, False],
            )
            .drop(columns=["_priority_order"])
            .reset_index(drop=True)
        )
        return _with_readable_columns(result)

    result = frame.sort_values(
        ["passed", "screen_score", "confidence_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    return _with_readable_columns(result)


def load_kis_paper_candidates(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty or "candidate_action" not in frame.columns:
        return _with_readable_columns(frame)

    result = frame.copy()
    result["_action_order"] = result["candidate_action"].map(KIS_CANDIDATE_ACTION_ORDER).fillna(99)
    for column in ("estimated_amount", "confidence_score", "screen_score"):
        if column not in result.columns:
            result[column] = 0.0

    result = (
        result.sort_values(
            ["_action_order", "estimated_amount", "confidence_score", "screen_score"],
            ascending=[True, False, False, False],
        )
        .drop(columns=["_action_order"])
        .reset_index(drop=True)
    )
    return _with_readable_columns(result)


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
        return _with_readable_columns(frame)

    split_order = {"test": 0, "train": 1}
    frame = frame.copy()
    frame["_split_order"] = frame["split"].map(split_order).fillna(2)
    result = frame.sort_values("_split_order").drop(columns=["_split_order"]).reset_index(drop=True)
    return _with_readable_columns(result)


def load_ml_predictions(metrics_path: Path) -> Any:
    predictions_path = metrics_path.parents[1] / "ml_predictions" / metrics_path.name
    if not predictions_path.exists():
        return pd.DataFrame()

    frame = pd.read_parquet(predictions_path)
    if frame.empty or "probability_positive_forward_return" not in frame.columns:
        return _with_readable_columns(frame)

    split_order = {"test": 0, "train": 1}
    frame = frame.copy()
    frame["_split_order"] = frame["split"].map(split_order).fillna(2)
    result = (
        frame.sort_values(
            ["_split_order", "probability_positive_forward_return"],
            ascending=[True, False],
        )
        .drop(columns=["_split_order"])
        .reset_index(drop=True)
    )
    return _with_readable_columns(result)


def load_news_sentiment(path: Path) -> Any:
    frame = pd.read_parquet(path)
    if frame.empty:
        return _with_readable_columns(frame)

    result = frame.sort_values(
        ["date", "news_score"],
        ascending=[False, False],
    ).reset_index(drop=True)
    return _with_readable_columns(result)


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


def _with_readable_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if "ticker" in result.columns and "stock_name" not in result.columns:
        result["stock_name"] = result["ticker"].map(_stock_name)
    if "status" in result.columns and "status_ko" not in result.columns:
        result["status_ko"] = result["status"].map(_map_status)
    if "side" in result.columns and "side_ko" not in result.columns:
        result["side_ko"] = result["side"].map(_map_side)
    if "split" in result.columns and "split_ko" not in result.columns:
        result["split_ko"] = result["split"].map(_map_split)
    if "latest_action" in result.columns and "latest_action_ko" not in result.columns:
        result["latest_action_ko"] = result["latest_action"].map(_map_final_action)
    if "final_action" in result.columns and "final_action_ko" not in result.columns:
        result["final_action_ko"] = result["final_action"].map(_map_final_action)
    if "signal_action" in result.columns and "signal_action_ko" not in result.columns:
        result["signal_action_ko"] = result["signal_action"].map(_map_final_action)
    if "candidate_action" in result.columns and "candidate_action_ko" not in result.columns:
        result["candidate_action_ko"] = result["candidate_action"].map(_map_candidate_action)
    if "screen_status_reason" in result.columns and "screen_status_reason_ko" not in result.columns:
        result["screen_status_reason_ko"] = result["screen_status_reason"].map(_map_screen_status)
    if "review_priority" in result.columns and "review_priority_ko" not in result.columns:
        result["review_priority_ko"] = result["review_priority"].map(_map_review_priority)
    if "market_regime" in result.columns and "market_regime_ko" not in result.columns:
        result["market_regime_ko"] = result["market_regime"].map(_map_regime)
    if "latest_market_regime" in result.columns and "latest_market_regime_ko" not in result.columns:
        result["latest_market_regime_ko"] = result["latest_market_regime"].map(_map_regime)
    return result


def _stock_name(value: object) -> str:
    ticker = str(value).zfill(6)
    return STOCK_NAME_MAP.get(ticker, "")


def _map_final_action(value: object) -> str:
    text = str(value)
    return FINAL_ACTION_KO.get(text, text)


def _map_candidate_action(value: object) -> str:
    text = str(value)
    return CANDIDATE_ACTION_KO.get(text, text)


def _map_screen_status(value: object) -> str:
    text = str(value)
    return SCREEN_STATUS_KO.get(text, text)


def _map_review_priority(value: object) -> str:
    text = str(value)
    return REVIEW_PRIORITY_KO.get(text, text)


def _map_regime(value: object) -> str:
    text = str(value)
    return REGIME_KO.get(text, text)


def _map_status(value: object) -> str:
    text = str(value)
    return STATUS_KO.get(text, text)


def _map_side(value: object) -> str:
    text = str(value)
    return SIDE_KO.get(text, text)


def _map_split(value: object) -> str:
    text = str(value)
    return SPLIT_KO.get(text, text)
