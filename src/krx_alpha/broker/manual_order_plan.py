from typing import Any, cast

import pandas as pd

MANUAL_ORDER_PLAN_COLUMNS = [
    "generated_at",
    "ticker",
    "manual_plan_action",
    "source_candidate_action",
    "estimated_quantity",
    "estimated_amount",
    "reference_price",
    "target_position_pct",
    "confidence_score",
    "screen_score",
    "pre_order_checklist",
    "risk_guardrail",
    "manual_status",
    "reason",
    "orders_sent",
]

REVIEW_ACTIONS = {"review_buy", "review_add"}


def build_manual_order_plan(candidate_frame: Any) -> pd.DataFrame:
    """Build a human-only manual order plan from KIS paper candidates."""

    frame = cast(pd.DataFrame, candidate_frame).copy()
    if frame.empty:
        return pd.DataFrame(columns=MANUAL_ORDER_PLAN_COLUMNS)

    rows: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        candidate_action = str(_value(row, "candidate_action", ""))
        estimated_quantity = int(_safe_float(_value(row, "estimated_quantity", 0)))
        reference_price = _safe_float(_value(row, "reference_price", 0.0))
        rows.append(
            {
                "generated_at": pd.Timestamp.now(tz="UTC"),
                "ticker": str(_value(row, "ticker", "")).zfill(6),
                "manual_plan_action": _manual_plan_action(
                    candidate_action,
                    estimated_quantity,
                    reference_price,
                ),
                "source_candidate_action": candidate_action,
                "estimated_quantity": estimated_quantity,
                "estimated_amount": _safe_float(_value(row, "estimated_amount", 0.0)),
                "reference_price": reference_price,
                "target_position_pct": _safe_float(_value(row, "target_position_pct", 0.0)),
                "confidence_score": _safe_float(_value(row, "confidence_score", 0.0)),
                "screen_score": _safe_float(_value(row, "screen_score", 0.0)),
                "pre_order_checklist": _pre_order_checklist(candidate_action),
                "risk_guardrail": _risk_guardrail(row),
                "manual_status": "not_reviewed",
                "reason": str(_value(row, "reason", "")),
                "orders_sent": 0,
            }
        )

    return pd.DataFrame(rows, columns=MANUAL_ORDER_PLAN_COLUMNS)


def format_manual_order_plan_report(plan_frame: Any) -> str:
    frame = cast(pd.DataFrame, plan_frame)
    review_frame = frame[
        frame["manual_plan_action"].isin(["manual_buy_review", "manual_add_review"])
    ]
    lines = [
        "# Manual Order Plan",
        "",
        "> Review-only artifact. This report is not an order instruction and never sends orders.",
        "",
        f"- Plan rows: {len(frame)}",
        f"- Manual buy/add review rows: {len(review_frame)}",
        "- Orders sent: 0",
        "",
        "## Review Table",
        "",
        "| Ticker | Plan | Qty | Est. Amount | Ref Price | Confidence | Status |",
        "| --- | --- | ---: | ---: | ---: | ---: | --- |",
    ]
    if frame.empty:
        lines.append("| N/A | no_manual_order | 0 | 0 | 0 | 0.00 | not_reviewed |")
    else:
        for _, row in frame.head(20).iterrows():
            lines.append(
                "| "
                f"{row['ticker']} | "
                f"{row['manual_plan_action']} | "
                f"{int(row['estimated_quantity'])} | "
                f"{float(row['estimated_amount']):,.0f} | "
                f"{float(row['reference_price']):,.0f} | "
                f"{float(row['confidence_score']):.2f} | "
                f"{row['manual_status']} |"
            )

    lines.extend(
        [
            "",
            "## Required Manual Checks",
            "",
            "Before acting outside this program, confirm every item below:",
            "",
            "1. Read the latest DART disclosures.",
            "2. Check same-day news and unusual price movement.",
            "3. Confirm liquidity and bid/ask spread in the broker app.",
            "4. Confirm market regime is not bearish or highly volatile.",
            "5. Confirm position size and maximum loss you can tolerate.",
            "6. Enter any order manually in the broker app only after your own decision.",
            "",
            "## Safety Notes",
            "",
            "- The program does not call real order endpoints.",
            "- `orders_sent` must remain 0.",
            "- This file is a checklist, not investment advice.",
            "",
        ]
    )
    return "\n".join(lines)


def _manual_plan_action(
    candidate_action: str,
    estimated_quantity: int,
    reference_price: float,
) -> str:
    if candidate_action == "manual_price_required" or reference_price <= 0:
        return "check_price_first"
    if candidate_action == "review_buy" and estimated_quantity > 0:
        return "manual_buy_review"
    if candidate_action == "review_add" and estimated_quantity > 0:
        return "manual_add_review"
    return "no_manual_order"


def _pre_order_checklist(candidate_action: str) -> str:
    if candidate_action in REVIEW_ACTIONS:
        return (
            "dart_disclosure_check; latest_news_check; liquidity_check; "
            "market_regime_check; position_size_check"
        )
    if candidate_action == "manual_price_required":
        return "reference_price_check; rerun_pipeline_or_check_broker_price"
    return "no_order_review_only"


def _risk_guardrail(row: pd.Series) -> str:
    risk_flags = str(_value(row, "risk_flags", "")).strip()
    caution = str(_value(row, "caution_summary", "")).strip()
    guardrails = [
        "manual_decision_required",
        "orders_sent_must_be_zero",
        "use_limit_order_if_acting_manually",
    ]
    if risk_flags:
        guardrails.append(f"risk_flags={risk_flags}")
    if caution:
        guardrails.append(f"caution={caution}")
    return "; ".join(guardrails)


def _value(row: pd.Series, column: str, default: object) -> object:
    if column not in row.index:
        return default
    value = row[column]
    if value is None:
        return default
    if isinstance(value, float) and pd.isna(value):
        return default
    return cast(object, value)


def _safe_float(value: object) -> float:
    try:
        return float(str(value).replace(",", "").replace("%", ""))
    except (TypeError, ValueError):
        return 0.0
