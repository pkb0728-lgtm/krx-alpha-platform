from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from krx_alpha.database.storage import (
    decision_journal_csv_path,
    decision_journal_evaluation_csv_path,
    decision_journal_evaluation_file_path,
    decision_journal_file_path,
    decision_journal_report_file_path,
    read_parquet,
    write_csv,
    write_parquet,
    write_text,
)

JOURNAL_COLUMNS = [
    "journal_id",
    "generated_at",
    "universe",
    "analysis_start_date",
    "analysis_end_date",
    "decision_date",
    "ticker",
    "status",
    "latest_action",
    "latest_confidence_score",
    "latest_financial_score",
    "latest_event_score",
    "latest_flow_score",
    "latest_news_score",
    "latest_macro_score",
    "latest_market_regime",
    "screening_passed",
    "screen_status_reason",
    "review_priority",
    "screen_score",
    "risk_blocked",
    "risk_flags",
    "suggested_position_pct",
    "kis_candidate_action",
    "kis_estimated_quantity",
    "kis_estimated_amount",
    "kis_reason",
    "signal_path",
    "report_path",
    "error",
]

EVALUATION_COLUMNS = [
    "journal_id",
    "universe",
    "ticker",
    "decision_date",
    "latest_action",
    "latest_confidence_score",
    "latest_market_regime",
    "holding_days",
    "entry_date",
    "entry_close",
    "evaluation_date",
    "evaluation_close",
    "forward_return",
    "outcome_status",
    "outcome_ko",
    "favorable_outcome",
]


@dataclass(frozen=True)
class DecisionJournalWriteResult:
    frame: pd.DataFrame
    parquet_path: Path
    csv_path: Path
    appended_count: int
    total_count: int


@dataclass(frozen=True)
class DecisionJournalEvaluationResult:
    frame: pd.DataFrame
    summary: pd.DataFrame
    parquet_path: Path
    csv_path: Path
    report_path: Path
    evaluated_count: int
    pending_count: int


def build_decision_journal_frame(
    *,
    universe: str,
    start_date: str,
    end_date: str,
    summary_frame: pd.DataFrame,
    screening_frame: pd.DataFrame | None = None,
    kis_candidate_frame: pd.DataFrame | None = None,
    generated_at: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Create one durable journal row per ticker decision."""
    generated = generated_at or pd.Timestamp.now(tz="UTC")
    screening_lookup = _ticker_lookup(screening_frame)
    kis_lookup = _ticker_lookup(kis_candidate_frame)

    rows: list[dict[str, object]] = []
    for _, summary_row in summary_frame.iterrows():
        ticker = _ticker(summary_row.get("ticker"))
        if not ticker:
            continue

        signal_row = _latest_signal_row(summary_row.get("signal_path"))
        decision_date = _date_text(signal_row.get("date", end_date) if signal_row else end_date)
        screening_row = screening_lookup.get(ticker, {})
        kis_row = kis_lookup.get(ticker, {})
        journal_id = "|".join([decision_date, universe, ticker, start_date, end_date])

        rows.append(
            {
                "journal_id": journal_id,
                "generated_at": generated.isoformat(),
                "universe": universe,
                "analysis_start_date": start_date,
                "analysis_end_date": end_date,
                "decision_date": decision_date,
                "ticker": ticker,
                "status": _text(summary_row.get("status")),
                "latest_action": _text(summary_row.get("latest_action")),
                "latest_confidence_score": _float(summary_row.get("latest_confidence_score")),
                "latest_financial_score": _float(summary_row.get("latest_financial_score")),
                "latest_event_score": _float(summary_row.get("latest_event_score")),
                "latest_flow_score": _float(summary_row.get("latest_flow_score")),
                "latest_news_score": _float(summary_row.get("latest_news_score")),
                "latest_macro_score": _float(summary_row.get("latest_macro_score")),
                "latest_market_regime": _text(summary_row.get("latest_market_regime")),
                "screening_passed": _bool(screening_row.get("passed")),
                "screen_status_reason": _text(screening_row.get("screen_status_reason")),
                "review_priority": _text(screening_row.get("review_priority")),
                "screen_score": _float(screening_row.get("screen_score")),
                "risk_blocked": _bool(screening_row.get("risk_blocked")),
                "risk_flags": _text(screening_row.get("risk_flags")),
                "suggested_position_pct": _float(screening_row.get("suggested_position_pct")),
                "kis_candidate_action": _text(kis_row.get("candidate_action")),
                "kis_estimated_quantity": int(_float(kis_row.get("estimated_quantity"))),
                "kis_estimated_amount": _float(kis_row.get("estimated_amount")),
                "kis_reason": _text(kis_row.get("reason")),
                "signal_path": _text(summary_row.get("signal_path")),
                "report_path": _text(summary_row.get("report_path")),
                "error": _text(summary_row.get("error")),
            }
        )

    return pd.DataFrame(rows, columns=JOURNAL_COLUMNS)


def append_decision_journal(
    project_root: Path,
    journal_rows: pd.DataFrame,
    journal_name: str = "decision_journal",
) -> DecisionJournalWriteResult:
    """Append rows to the local decision journal, replacing duplicate run/ticker rows."""
    parquet_path = decision_journal_file_path(project_root, journal_name)
    csv_path = decision_journal_csv_path(project_root, journal_name)
    existing = read_parquet(parquet_path) if parquet_path.exists() else _empty_journal_frame()

    combined = journal_rows.copy() if existing.empty else pd.concat([existing, journal_rows])
    if not combined.empty:
        combined = (
            combined.drop_duplicates(subset=["journal_id"], keep="last")
            .sort_values(["decision_date", "universe", "ticker"])
            .reset_index(drop=True)
        )

    write_parquet(combined, parquet_path)
    write_csv(combined, csv_path)
    return DecisionJournalWriteResult(
        frame=combined,
        parquet_path=parquet_path,
        csv_path=csv_path,
        appended_count=len(journal_rows),
        total_count=len(combined),
    )


def evaluate_decision_journal(
    project_root: Path,
    holding_days: int,
    journal_frame: pd.DataFrame | None = None,
    journal_name: str = "decision_journal",
) -> DecisionJournalEvaluationResult:
    if holding_days <= 0:
        raise ValueError("holding_days must be positive.")

    if journal_frame is None:
        journal_path = decision_journal_file_path(project_root, journal_name)
        if not journal_path.exists():
            raise FileNotFoundError(f"Decision journal does not exist: {journal_path}")
        journal_frame = read_parquet(journal_path)

    evaluation_frame = evaluate_decision_journal_frame(
        project_root=project_root,
        journal_frame=journal_frame,
        holding_days=holding_days,
    )
    summary = summarize_decision_journal_evaluation(evaluation_frame)
    parquet_path = decision_journal_evaluation_file_path(project_root, holding_days, journal_name)
    csv_path = decision_journal_evaluation_csv_path(project_root, holding_days, journal_name)
    report_path = decision_journal_report_file_path(project_root, holding_days, journal_name)
    write_parquet(evaluation_frame, parquet_path)
    write_csv(evaluation_frame, csv_path)
    write_text(
        format_decision_journal_evaluation_report(evaluation_frame, summary, holding_days),
        report_path,
    )

    evaluated_count = int((evaluation_frame["outcome_status"] == "evaluated").sum())
    pending_count = int((evaluation_frame["outcome_status"] == "pending").sum())
    return DecisionJournalEvaluationResult(
        frame=evaluation_frame,
        summary=summary,
        parquet_path=parquet_path,
        csv_path=csv_path,
        report_path=report_path,
        evaluated_count=evaluated_count,
        pending_count=pending_count,
    )


def evaluate_decision_journal_frame(
    *,
    project_root: Path,
    journal_frame: pd.DataFrame,
    holding_days: int,
) -> pd.DataFrame:
    price_history = _load_processed_price_history(project_root)
    rows: list[dict[str, object]] = []

    for _, journal_row in journal_frame.iterrows():
        ticker = _ticker(journal_row.get("ticker"))
        prices = price_history.get(ticker, pd.DataFrame())
        rows.append(_evaluate_row(journal_row, prices, holding_days))

    return pd.DataFrame(rows, columns=EVALUATION_COLUMNS)


def summarize_decision_journal_evaluation(evaluation_frame: pd.DataFrame) -> pd.DataFrame:
    if evaluation_frame.empty:
        return pd.DataFrame(
            columns=[
                "latest_action",
                "decision_count",
                "evaluated_count",
                "average_forward_return",
                "positive_return_rate",
                "favorable_rate",
            ]
        )

    rows: list[dict[str, object]] = []
    for action, group in evaluation_frame.groupby("latest_action", dropna=False):
        evaluated = group[group["outcome_status"] == "evaluated"]
        rows.append(
            {
                "latest_action": str(action),
                "decision_count": len(group),
                "evaluated_count": len(evaluated),
                "average_forward_return": (
                    float(evaluated["forward_return"].mean()) if not evaluated.empty else 0.0
                ),
                "positive_return_rate": _mean_bool(evaluated["forward_return"] > 0),
                "favorable_rate": _mean_bool(evaluated["favorable_outcome"]),
            }
        )

    return pd.DataFrame(rows).sort_values("decision_count", ascending=False).reset_index(drop=True)


def format_decision_journal_evaluation_report(
    evaluation_frame: pd.DataFrame,
    summary_frame: pd.DataFrame,
    holding_days: int,
) -> str:
    evaluated_count = int((evaluation_frame["outcome_status"] == "evaluated").sum())
    pending_count = int((evaluation_frame["outcome_status"] == "pending").sum())
    lines = [
        f"# Decision Journal Evaluation h{holding_days}",
        "",
        "이 리포트는 과거에 저장한 판단이 이후 실제 가격 흐름과 얼마나 맞았는지 확인합니다.",
        (
            "매수 후보는 이후 수익률이 양수면 긍정적으로 보고, "
            "리스크 차단/회피는 이후 수익률이 음수면 방어 판단이 맞았다고 봅니다."
        ),
        "",
        "## Summary",
        "",
        f"- Total decisions: {len(evaluation_frame)}",
        f"- Evaluated: {evaluated_count}",
        f"- Pending: {pending_count}",
        "",
        "## By Action",
        "",
    ]
    if summary_frame.empty:
        lines.append("- No summary rows.")
    else:
        lines.append("| Action | Decisions | Evaluated | Avg Return | Positive | Favorable |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")
        for _, row in summary_frame.iterrows():
            lines.append(
                "| "
                f"{row['latest_action']} | "
                f"{int(row['decision_count'])} | "
                f"{int(row['evaluated_count'])} | "
                f"{float(row['average_forward_return']) * 100:.2f}% | "
                f"{float(row['positive_return_rate']) * 100:.2f}% | "
                f"{float(row['favorable_rate']) * 100:.2f}% |"
            )

    lines.extend(["", "## Recent Evaluated Rows", ""])
    evaluated = evaluation_frame[evaluation_frame["outcome_status"] == "evaluated"].tail(10)
    if evaluated.empty:
        lines.append(
            "- 아직 평가 가능한 행이 없습니다. 며칠 뒤 미래 가격 데이터가 쌓이면 다시 실행하세요."
        )
    else:
        lines.append("| Date | Ticker | Action | Return | Outcome |")
        lines.append("| --- | --- | --- | ---: | --- |")
        for _, row in evaluated.iterrows():
            lines.append(
                "| "
                f"{row['decision_date']} | "
                f"{row['ticker']} | "
                f"{row['latest_action']} | "
                f"{float(row['forward_return']) * 100:.2f}% | "
                f"{row['outcome_ko']} |"
            )
    return "\n".join(lines)


def _evaluate_row(
    journal_row: pd.Series,
    prices: pd.DataFrame,
    holding_days: int,
) -> dict[str, object]:
    base = {
        "journal_id": _text(journal_row.get("journal_id")),
        "universe": _text(journal_row.get("universe")),
        "ticker": _ticker(journal_row.get("ticker")),
        "decision_date": _date_text(journal_row.get("decision_date")),
        "latest_action": _text(journal_row.get("latest_action")),
        "latest_confidence_score": _float(journal_row.get("latest_confidence_score")),
        "latest_market_regime": _text(journal_row.get("latest_market_regime")),
        "holding_days": holding_days,
        "entry_date": "",
        "entry_close": 0.0,
        "evaluation_date": "",
        "evaluation_close": 0.0,
        "forward_return": 0.0,
        "outcome_status": "no_price_data",
        "outcome_ko": "가격 데이터 없음",
        "favorable_outcome": False,
    }
    if prices.empty:
        return base

    decision_date = pd.to_datetime(base["decision_date"], errors="coerce")
    if pd.isna(decision_date):
        base["outcome_status"] = "invalid_decision_date"
        base["outcome_ko"] = "판단일 오류"
        return base

    price_frame = prices.sort_values("date").reset_index(drop=True)
    entry_candidates = price_frame[price_frame["date"] <= decision_date]
    if entry_candidates.empty:
        base["outcome_status"] = "no_entry_price"
        base["outcome_ko"] = "진입 기준 가격 없음"
        return base

    entry_index = int(entry_candidates.index[-1])
    evaluation_index = entry_index + holding_days
    entry_row = price_frame.iloc[entry_index]
    base["entry_date"] = _date_text(entry_row["date"])
    base["entry_close"] = float(entry_row["close"])

    if evaluation_index >= len(price_frame):
        base["outcome_status"] = "pending"
        base["outcome_ko"] = "평가 대기"
        return base

    evaluation_row = price_frame.iloc[evaluation_index]
    entry_close = float(entry_row["close"])
    evaluation_close = float(evaluation_row["close"])
    forward_return = evaluation_close / entry_close - 1 if entry_close else 0.0
    action = str(base["latest_action"])
    favorable = _is_favorable(action, forward_return)
    base.update(
        {
            "evaluation_date": _date_text(evaluation_row["date"]),
            "evaluation_close": evaluation_close,
            "forward_return": forward_return,
            "outcome_status": "evaluated",
            "outcome_ko": _outcome_text(action, forward_return, favorable),
            "favorable_outcome": favorable,
        }
    )
    return base


def _load_processed_price_history(project_root: Path) -> dict[str, pd.DataFrame]:
    price_dir = project_root / "data" / "processed" / "prices_daily"
    if not price_dir.exists():
        return {}

    frames: list[pd.DataFrame] = []
    for path in sorted(price_dir.glob("*.parquet")):
        frame = read_parquet(path)
        if not frame.empty and {"date", "ticker", "close"}.issubset(frame.columns):
            frames.append(frame[["date", "ticker", "close"]])
    if not frames:
        return {}

    combined = pd.concat(frames, ignore_index=True)
    combined["ticker"] = combined["ticker"].astype(str).str.zfill(6)
    combined["date"] = pd.to_datetime(combined["date"], errors="coerce")
    combined = combined.dropna(subset=["date", "ticker", "close"])
    combined = combined.drop_duplicates(subset=["ticker", "date"], keep="last")

    return {
        ticker: group.sort_values("date").reset_index(drop=True)
        for ticker, group in combined.groupby("ticker")
    }


def _latest_signal_row(value: object) -> dict[str, object] | None:
    path_text = _text(value)
    if not path_text:
        return None
    path = Path(path_text)
    if not path.exists():
        return None
    frame = read_parquet(path)
    if frame.empty or "date" not in frame.columns:
        return None
    signal_frame = frame.copy()
    signal_frame["date"] = pd.to_datetime(signal_frame["date"], errors="coerce")
    signal_frame = signal_frame.dropna(subset=["date"])
    if signal_frame.empty:
        return None
    return dict(signal_frame.sort_values("date").iloc[-1])


def _ticker_lookup(frame: pd.DataFrame | None) -> dict[str, dict[str, object]]:
    if frame is None or frame.empty or "ticker" not in frame.columns:
        return {}
    result: dict[str, dict[str, object]] = {}
    for _, row in frame.iterrows():
        result[_ticker(row.get("ticker"))] = dict(row)
    return result


def _empty_journal_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=JOURNAL_COLUMNS)


def _mean_bool(series: Any) -> float:
    if len(series) == 0:
        return 0.0
    return float(series.mean())


def _is_favorable(action: str, forward_return: float) -> bool:
    if action == "buy_candidate":
        return forward_return > 0
    if action in {"blocked", "avoid"}:
        return forward_return <= 0
    if action in {"watch", "hold"}:
        return abs(forward_return) <= 0.03
    return False


def _outcome_text(action: str, forward_return: float, favorable: bool) -> str:
    if action == "buy_candidate":
        return "상승 적중" if favorable else "상승 실패"
    if action in {"blocked", "avoid"}:
        return "방어 적절" if favorable else "기회비용 발생"
    if action in {"watch", "hold"}:
        return "관망 적절" if favorable else "관망 후 큰 변동"
    return "평가 완료"


def _ticker(value: object) -> str:
    text = _text(value)
    return text.zfill(6) if text.isdigit() else text


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _float(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return float(cast(Any, value))


def _bool(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _date_text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return str(value)
    return str(timestamp.strftime("%Y-%m-%d"))
