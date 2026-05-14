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

RISK_FLAG_KO = {
    "insufficient_history": "분석에 필요한 과거 데이터가 아직 부족합니다.",
    "low_liquidity": "거래대금이 낮아 원하는 가격에 사고팔기 어려울 수 있습니다.",
    "wide_daily_range": "하루 가격 변동폭이 커서 단기 위험이 큽니다.",
    "high_short_term_volatility": "최근 5일 변동성이 높아 주가가 크게 흔들리고 있습니다.",
    "weak_risk_score": "변동성 등을 반영한 위험 점수가 낮습니다.",
    "disclosure_event_risk": "공시 이벤트로 인한 추가 확인이 필요합니다.",
    "weak_investor_flow": "외국인/기관 수급 흐름이 약합니다.",
    "weak_macro_environment": "금리, 환율 등 거시 환경이 우호적이지 않습니다.",
    "market_regime_bear": "시장 국면이 하락장으로 판단되어 신규 매수를 막았습니다.",
    "market_regime_high_volatility": "시장 국면이 고변동성장으로 판단되어 신규 매수를 막았습니다.",
    "signal_file_missing_or_empty": "최종 신호 파일이 없거나 비어 있어 판단을 보류했습니다.",
}

NEXT_CHECK_KO = {
    "insufficient_history": "며칠 뒤 데이터가 더 쌓인 다음 다시 확인하세요.",
    "low_liquidity": "최근 거래대금과 호가가 충분한지 확인하세요.",
    "wide_daily_range": "당일 급등락 사유가 뉴스나 공시 때문인지 확인하세요.",
    "high_short_term_volatility": "변동성이 4% 아래로 낮아지는지 며칠 더 지켜보세요.",
    "weak_risk_score": "포지션 비중을 줄이거나 변동성이 낮아질 때까지 기다리세요.",
    "disclosure_event_risk": "DART 공시 내용을 먼저 읽고 이벤트 성격을 확인하세요.",
    "weak_investor_flow": "외국인/기관 순매수 흐름이 개선되는지 확인하세요.",
    "weak_macro_environment": "금리, 환율, 미국 시장 흐름을 같이 확인하세요.",
    "market_regime_bear": "시장 지수가 회복되는지 먼저 확인하세요.",
    "market_regime_high_volatility": "시장 전체 변동성이 안정되는지 먼저 확인하세요.",
    "signal_file_missing_or_empty": "파이프라인을 다시 실행해 신호 파일을 생성하세요.",
}

ACTION_BEGINNER_KO = {
    "buy_candidate": "조건은 좋아 보이지만, 바로 매수하지 말고 뉴스와 공시를 다시 확인해야 합니다.",
    "watch": "관심 종목으로 지켜볼 수 있지만, 아직 적극적인 매수 후보는 아닙니다.",
    "hold": "뚜렷한 매수/회피 신호가 약해 중립적으로 보는 상태입니다.",
    "avoid": "현재 조건이 좋지 않아 신규 매수 검토에서 제외하는 편이 안전합니다.",
    "blocked": "리스크 필터가 작동해서 신규 매수 검토를 막은 상태입니다.",
}

CANDIDATE_BEGINNER_KO = {
    "review_buy": (
        "모의계좌 기준으로 매수 검토 후보입니다. 실제 매매 전에는 사람이 다시 확인해야 합니다."
    ),
    "review_add": "이미 보유 중인 종목의 추가매수 검토 후보입니다. 현재 비중을 먼저 확인하세요.",
    "manual_price_required": "가격 기준이 부족해 사람이 현재가를 직접 확인해야 합니다.",
    "hold_review": "지켜볼 수는 있지만 지금 바로 매수 후보로 보기는 어렵습니다.",
    "skip": "조건 미달 또는 리스크 차단으로 후보에서 제외되었습니다.",
}

CAUTION_PHRASE_KO = {
    "candidate did not pass all screen thresholds": (
        "스크리너 통과 기준을 모두 만족하지 못했습니다."
    ),
    "risk filter blocked the signal": "리스크 필터가 신호를 차단했습니다.",
    "market regime is not supportive": "시장 국면이 우호적이지 않습니다.",
    "market regime needs more data": "시장 국면 판단에 필요한 데이터가 더 필요합니다.",
    "financial score is weak": "재무 점수가 약합니다.",
    "disclosure event score is weak": "공시 이벤트 점수가 약합니다.",
    "news sentiment is weak or missing": "뉴스 감성 점수가 약하거나 없습니다.",
    "short-term volatility is elevated": "단기 변동성이 높습니다.",
    "RSI looks overheated": "RSI가 과열 구간에 가깝습니다.",
    "no hard block, but confirm latest news and disclosures": (
        "강한 차단 사유는 없지만 최신 뉴스와 공시는 확인해야 합니다."
    ),
}

CHECKLIST_KO = {
    "identify_failed_threshold": "어떤 기준을 통과하지 못했는지 확인",
    "confirm_recent_news": "최근 뉴스 확인",
    "check_dart_disclosures": "DART 공시 확인",
    "verify_liquidity": "거래대금과 유동성 확인",
    "review_position_size": "투자 비중 확인",
    "extend_price_history_for_regime": "시장 국면 분석을 위해 더 긴 가격 기간으로 재실행",
    "rebuild_price_features": "가격 피처 재생성",
    "rerun_pipeline": "파이프라인 다시 실행",
    "confirm_signal_artifact": "최종 신호 파일 생성 여부 확인",
}

EVIDENCE_PHRASE_KO = {
    "risk filter passed": "리스크 필터를 통과했습니다.",
    "trading value surged over the 5-day baseline": "거래대금이 최근 5일 기준보다 크게 늘었습니다.",
    "trading value increased over the 5-day baseline": "거래대금이 최근 5일 기준보다 늘었습니다.",
    "RSI is in a recovery-friendly range": "RSI가 반등을 기대할 수 있는 구간입니다.",
    "financial score is supportive": "재무 점수가 우호적입니다.",
    "investor flow score is supportive": "수급 점수가 우호적입니다.",
    "news sentiment score is supportive": "뉴스 감성 점수가 우호적입니다.",
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
    if "risk_flags" in result.columns and "risk_flags_ko" not in result.columns:
        result["risk_flags_ko"] = result["risk_flags"].map(_map_risk_flags)
    if "evidence_summary" in result.columns and "evidence_summary_ko" not in result.columns:
        result["evidence_summary_ko"] = result["evidence_summary"].map(_map_evidence_summary)
    if "caution_summary" in result.columns and "caution_summary_ko" not in result.columns:
        result["caution_summary_ko"] = result["caution_summary"].map(_map_caution_summary)
    if "review_checklist" in result.columns and "review_checklist_ko" not in result.columns:
        result["review_checklist_ko"] = result["review_checklist"].map(_map_review_checklist)
    if "beginner_summary_ko" not in result.columns:
        result["beginner_summary_ko"] = result.apply(_beginner_summary, axis=1)
    if "next_check_ko" not in result.columns:
        result["next_check_ko"] = result.apply(_next_check, axis=1)
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


def _map_risk_flags(value: object) -> str:
    flags = _split_flags(value)
    if not flags:
        return "특별한 리스크 표시 없음"
    return " / ".join(RISK_FLAG_KO.get(flag, flag) for flag in flags)


def _map_evidence_summary(value: object) -> str:
    phrases = _split_semicolon_phrases(value)
    if not phrases:
        return "표시할 근거가 없습니다."
    return " / ".join(_map_evidence_phrase(phrase) for phrase in phrases)


def _map_caution_summary(value: object) -> str:
    phrases = _split_semicolon_phrases(value)
    if not phrases:
        return "특별한 주의 문구가 없습니다."
    return " / ".join(CAUTION_PHRASE_KO.get(phrase, phrase) for phrase in phrases)


def _map_review_checklist(value: object) -> str:
    items = _split_comma_phrases(value)
    if not items:
        return "추가 확인 항목이 없습니다."
    return " / ".join(CHECKLIST_KO.get(item, item) for item in items)


def _map_evidence_phrase(phrase: str) -> str:
    if " action with screen score " in phrase:
        action, score = phrase.split(" action with screen score ", maxsplit=1)
        return f"최종 판단은 {_map_final_action(action)}이고 스크리너 점수는 {score}점입니다."
    if phrase.startswith("confidence "):
        return f"신뢰도는 {phrase.removeprefix('confidence ')}점입니다."
    if phrase.startswith("latest trading value "):
        return f"최근 거래대금은 {phrase.removeprefix('latest trading value ')}입니다."
    return EVIDENCE_PHRASE_KO.get(phrase, phrase)


def _beginner_summary(row: pd.Series) -> str:
    flags = _split_flags(row.get("risk_flags", ""))
    if flags:
        return "현재는 리스크가 먼저 감지되어 매수 후보로 보기 어렵습니다."

    candidate_action = str(row.get("candidate_action", ""))
    if candidate_action and candidate_action != "nan":
        return CANDIDATE_BEGINNER_KO.get(candidate_action, "후보 상태를 추가로 확인해야 합니다.")

    action = str(row.get("final_action", row.get("latest_action", "")))
    return ACTION_BEGINNER_KO.get(action, "점수와 리스크를 함께 보고 사람이 최종 판단해야 합니다.")


def _next_check(row: pd.Series) -> str:
    flags = _split_flags(row.get("risk_flags", ""))
    next_checks = [NEXT_CHECK_KO.get(flag, "") for flag in flags]
    next_checks = [check for check in next_checks if check]
    if next_checks:
        return " / ".join(dict.fromkeys(next_checks))

    candidate_action = str(row.get("candidate_action", ""))
    if candidate_action in {"review_buy", "review_add"}:
        return "뉴스, 공시, 유동성, 현재 계좌 비중을 확인하세요."
    if candidate_action == "manual_price_required":
        return "현재가와 기준 가격을 먼저 확인하세요."

    action = str(row.get("final_action", row.get("latest_action", "")))
    if action == "buy_candidate":
        return "뉴스, 공시, 시장 국면을 확인한 뒤 소액 기준으로 검토하세요."
    if action == "watch":
        return "신뢰도와 거래대금이 추가로 개선되는지 지켜보세요."
    return "무리하게 매수하지 말고 다음 분석 결과를 기다리세요."


def _split_flags(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan"}:
        return []
    return [flag.strip() for flag in text.split(",") if flag.strip()]


def _split_semicolon_phrases(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan"}:
        return []
    return [phrase.strip() for phrase in text.split(";") if phrase.strip()]


def _split_comma_phrases(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan"}:
        return []
    return [phrase.strip() for phrase in text.split(",") if phrase.strip()]
