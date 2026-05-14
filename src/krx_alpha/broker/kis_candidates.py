from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd

from krx_alpha.broker.kis_paper import KISPaperBalance, KISPaperHolding

CANDIDATE_COLUMNS = [
    "generated_at",
    "account",
    "ticker",
    "candidate_type",
    "candidate_action",
    "final_action",
    "passed",
    "review_priority",
    "screen_status_reason",
    "confidence_score",
    "screen_score",
    "target_position_pct",
    "current_quantity",
    "current_value",
    "total_equity",
    "cash_amount",
    "cash_buffer_pct",
    "cash_available_for_candidates",
    "target_value",
    "gap_value",
    "reference_price",
    "reference_price_source",
    "estimated_quantity",
    "estimated_amount",
    "cash_after_candidate",
    "risk_flags",
    "reason",
    "evidence_summary",
    "caution_summary",
    "orders_sent",
]

CANDIDATE_PRIORITY_ORDER = {
    "high": 0,
    "medium": 1,
    "watchlist": 2,
    "low": 3,
    "blocked": 4,
}


@dataclass(frozen=True)
class KISPaperCandidateConfig:
    max_candidates: int = 10
    cash_buffer_pct: float = 5.0


class KISPaperCandidateBuilder:
    """Build human-review candidates from screening signals and KIS paper balance.

    This class never sends orders. It only estimates review quantities from the
    latest screening result, current mock-account holdings, and available cash.
    """

    def __init__(self, config: KISPaperCandidateConfig | None = None) -> None:
        self.config = config or KISPaperCandidateConfig()

    def build(self, screening_frame: Any, balance: KISPaperBalance) -> pd.DataFrame:
        if screening_frame.empty:
            return pd.DataFrame(columns=CANDIDATE_COLUMNS)

        holdings = {holding.ticker: holding for holding in balance.holdings}
        total_equity = _total_equity(balance)
        cash_available = _cash_available(balance.cash_amount, self.config.cash_buffer_pct)
        remaining_cash = cash_available
        rows: list[dict[str, object]] = []

        sorted_frame = _sort_screening_frame(cast(pd.DataFrame, screening_frame))
        for _, row in sorted_frame.head(max(self.config.max_candidates, 0)).iterrows():
            ticker = str(_series_value(row, "ticker", "")).zfill(6)
            holding = holdings.get(ticker)
            target_position_pct = _safe_float(_series_value(row, "suggested_position_pct", 0.0))
            current_value = holding.evaluation_amount if holding else 0.0
            current_quantity = holding.quantity if holding else 0
            target_value = max(total_equity * target_position_pct / 100.0, 0.0)
            gap_value = max(target_value - current_value, 0.0)
            reference_price = _reference_price(row, holding)
            reference_price_source = _reference_price_source(row, holding)
            candidate_type, candidate_action, reason = _classify_candidate(row, holding, gap_value)

            estimated_quantity = 0
            estimated_amount = 0.0
            if candidate_action in {"review_buy", "review_add"}:
                if reference_price <= 0:
                    candidate_type = "manual_price_required"
                    candidate_action = "manual_price_required"
                    reason = _join_reason(reason, "reference_price_missing")
                elif remaining_cash <= 0:
                    candidate_type = "cash_unavailable"
                    candidate_action = "hold_review"
                    reason = _join_reason(reason, "cash_unavailable")
                else:
                    allocation = min(gap_value, remaining_cash)
                    estimated_quantity = int(allocation // reference_price)
                    estimated_amount = float(estimated_quantity * reference_price)
                    if estimated_quantity <= 0:
                        candidate_type = "below_one_share"
                        candidate_action = "hold_review"
                        reason = _join_reason(reason, "estimated_amount_below_one_share")
                    else:
                        remaining_cash = max(remaining_cash - estimated_amount, 0.0)
                        if estimated_amount < gap_value:
                            reason = _join_reason(reason, "cash_limited")

            rows.append(
                {
                    "generated_at": pd.Timestamp.now(tz="UTC"),
                    "account": balance.account,
                    "ticker": ticker,
                    "candidate_type": candidate_type,
                    "candidate_action": candidate_action,
                    "final_action": str(_series_value(row, "final_action", "")),
                    "passed": _safe_bool(_series_value(row, "passed", False)),
                    "review_priority": str(_series_value(row, "review_priority", "")),
                    "screen_status_reason": str(_series_value(row, "screen_status_reason", "")),
                    "confidence_score": _safe_float(_series_value(row, "confidence_score", 0.0)),
                    "screen_score": _safe_float(_series_value(row, "screen_score", 0.0)),
                    "target_position_pct": target_position_pct,
                    "current_quantity": current_quantity,
                    "current_value": current_value,
                    "total_equity": total_equity,
                    "cash_amount": balance.cash_amount,
                    "cash_buffer_pct": self.config.cash_buffer_pct,
                    "cash_available_for_candidates": cash_available,
                    "target_value": target_value,
                    "gap_value": gap_value,
                    "reference_price": reference_price,
                    "reference_price_source": reference_price_source,
                    "estimated_quantity": estimated_quantity,
                    "estimated_amount": estimated_amount,
                    "cash_after_candidate": remaining_cash,
                    "risk_flags": str(_series_value(row, "risk_flags", "")),
                    "reason": reason,
                    "evidence_summary": str(_series_value(row, "evidence_summary", "")),
                    "caution_summary": str(_series_value(row, "caution_summary", "")),
                    "orders_sent": 0,
                }
            )

        return pd.DataFrame(rows, columns=CANDIDATE_COLUMNS)


def enrich_screening_reference_prices(screening_frame: Any, project_root: Path) -> pd.DataFrame:
    """Attach latest close prices from feature artifacts when available."""
    result = cast(pd.DataFrame, screening_frame).copy()
    if result.empty:
        return result

    if "reference_price" not in result.columns:
        result["reference_price"] = pd.NA
    if "reference_price_source" not in result.columns:
        result["reference_price_source"] = ""

    for index, row in result.iterrows():
        existing_price = _safe_float(_series_value(row, "reference_price", 0.0))
        if existing_price > 0:
            if not str(_series_value(row, "reference_price_source", "")):
                result.at[index, "reference_price_source"] = "screening"
            continue

        price, source = _feature_close_price(row, project_root)
        if price > 0:
            result.at[index, "reference_price"] = price
            result.at[index, "reference_price_source"] = source

    return result


def format_kis_paper_candidate_report(candidate_frame: Any) -> str:
    frame = cast(pd.DataFrame, candidate_frame)
    review_frame = frame[frame["candidate_action"].isin(["review_buy", "review_add"])]
    manual_frame = frame[frame["candidate_action"] == "manual_price_required"]
    lines = [
        "# KIS Paper Review Candidates",
        "",
        f"- Generated candidates: {len(frame)}",
        f"- Review buy/add candidates: {len(review_frame)}",
        f"- Manual price checks: {len(manual_frame)}",
        "- Orders sent: 0",
        "",
        "## Candidate Table",
        "",
        "| Ticker | Action | Qty | Est. Amount | Target % | Confidence | Reason |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]

    if frame.empty:
        lines.append("| N/A | N/A | 0 | 0 | 0.00 | 0.00 | no_candidates |")
    else:
        for _, row in frame.head(20).iterrows():
            lines.append(
                "| "
                f"{row['ticker']} | "
                f"{row['candidate_action']} | "
                f"{int(row['estimated_quantity'])} | "
                f"{float(row['estimated_amount']):,.0f} | "
                f"{float(row['target_position_pct']):.2f} | "
                f"{float(row['confidence_score']):.2f} | "
                f"{row['reason']} |"
            )

    lines.extend(
        [
            "",
            "## Safety Notes",
            "",
            "- This artifact is a human-review queue, not an order instruction.",
            "- The system never calls a KIS order endpoint in this step.",
            "- Review disclosures, liquidity, market regime, and news before any manual action.",
            "",
        ]
    )
    return "\n".join(lines)


def _sort_screening_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    result["_priority_order"] = (
        result["review_priority"].map(CANDIDATE_PRIORITY_ORDER).fillna(99)
        if "review_priority" in result.columns
        else 99
    )
    result["_action_order"] = (
        result["final_action"].map({"buy_candidate": 0, "watch": 1}).fillna(2)
        if "final_action" in result.columns
        else 2
    )
    result["_passed_order"] = (
        result["passed"].map(lambda value: 0 if _safe_bool(value) else 1)
        if "passed" in result.columns
        else 1
    )
    for column in ("screen_score", "confidence_score"):
        if column not in result.columns:
            result[column] = 0.0

    return (
        result.sort_values(
            [
                "_passed_order",
                "_priority_order",
                "_action_order",
                "screen_score",
                "confidence_score",
            ],
            ascending=[True, True, True, False, False],
        )
        .drop(columns=["_passed_order", "_priority_order", "_action_order"])
        .reset_index(drop=True)
    )


def _classify_candidate(
    row: pd.Series,
    holding: KISPaperHolding | None,
    gap_value: float,
) -> tuple[str, str, str]:
    passed = _safe_bool(_series_value(row, "passed", False))
    risk_blocked = _safe_bool(_series_value(row, "risk_blocked", False))
    final_action = str(_series_value(row, "final_action", ""))
    status_reason = str(_series_value(row, "screen_status_reason", ""))

    if risk_blocked or not passed:
        return "screen_blocked_or_rejected", "skip", status_reason or "screen_not_passed"
    if final_action != "buy_candidate":
        return "watch_only", "hold_review", f"final_action_{final_action}"
    if gap_value <= 0:
        return "target_already_met", "hold_review", "target_position_already_met"
    if holding is not None and holding.quantity > 0:
        return "held_add_candidate", "review_add", "held_position_below_target"
    return "new_buy_candidate", "review_buy", "passed_buy_candidate_signal"


def _feature_close_price(row: pd.Series, project_root: Path) -> tuple[float, str]:
    signal_path_value = str(_series_value(row, "signal_path", ""))
    if not signal_path_value:
        return 0.0, ""

    signal_path = Path(signal_path_value)
    feature_path = project_root / "data" / "features" / "prices_daily" / signal_path.name
    if not feature_path.exists():
        return 0.0, ""

    feature_frame = pd.read_parquet(feature_path)
    if feature_frame.empty or "close" not in feature_frame.columns:
        return 0.0, ""

    current = feature_frame.copy()
    if "date" in current.columns:
        current["date"] = pd.to_datetime(current["date"], errors="coerce")
        current = current.sort_values("date")
    return _safe_float(current.iloc[-1]["close"]), "feature_close"


def _total_equity(balance: KISPaperBalance) -> float:
    if balance.total_evaluation_amount > 0:
        return balance.total_evaluation_amount
    return balance.cash_amount + balance.stock_evaluation_amount


def _cash_available(cash_amount: float, cash_buffer_pct: float) -> float:
    bounded_buffer = min(max(cash_buffer_pct, 0.0), 100.0)
    return max(cash_amount * (1.0 - bounded_buffer / 100.0), 0.0)


def _reference_price(row: pd.Series, holding: KISPaperHolding | None) -> float:
    for column in ("reference_price", "close", "current_price", "latest_price"):
        price = _safe_float(_series_value(row, column, 0.0))
        if price > 0:
            return price
    if holding and holding.current_price > 0:
        return holding.current_price
    return 0.0


def _reference_price_source(row: pd.Series, holding: KISPaperHolding | None) -> str:
    source = str(_series_value(row, "reference_price_source", ""))
    if source:
        return source
    for column in ("close", "current_price", "latest_price"):
        if _safe_float(_series_value(row, column, 0.0)) > 0:
            return column
    if holding and holding.current_price > 0:
        return "kis_holding_current_price"
    return ""


def _series_value(row: pd.Series, column: str, default: object) -> object:
    if column not in row.index:
        return default
    value = row[column]
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    return cast(object, value)


def _safe_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, float) and pd.isna(value):
        return 0.0
    try:
        return float(str(value).replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return 0.0


def _join_reason(current: str, addition: str) -> str:
    if not current:
        return addition
    if addition in current.split(", "):
        return current
    return f"{current}, {addition}"
