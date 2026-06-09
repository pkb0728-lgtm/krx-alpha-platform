import pandas as pd

from krx_alpha.broker.manual_order_plan import (
    build_manual_order_plan,
    format_manual_order_plan_report,
)


def test_manual_order_plan_creates_review_checklist_without_orders() -> None:
    candidate_frame = pd.DataFrame(
        {
            "ticker": ["005930", "000660", "005380"],
            "candidate_action": ["review_buy", "manual_price_required", "skip"],
            "estimated_quantity": [3, 0, 0],
            "estimated_amount": [210_000.0, 0.0, 0.0],
            "reference_price": [70_000.0, 0.0, 180_000.0],
            "target_position_pct": [1.5, 0.0, 0.0],
            "confidence_score": [72.0, 68.0, 55.0],
            "screen_score": [75.0, 66.0, 50.0],
            "risk_flags": ["", "", "weak_risk_score"],
            "caution_summary": ["check news", "check price", "risk weak"],
            "reason": ["passed_buy_candidate_signal", "reference_price_missing", "skip"],
        }
    )

    plan = build_manual_order_plan(candidate_frame)

    assert list(plan["manual_plan_action"]) == [
        "manual_buy_review",
        "check_price_first",
        "no_manual_order",
    ]
    assert int(plan["orders_sent"].sum()) == 0
    assert plan.loc[0, "manual_status"] == "not_reviewed"
    assert "dart_disclosure_check" in plan.loc[0, "pre_order_checklist"]
    assert "orders_sent_must_be_zero" in plan.loc[0, "risk_guardrail"]


def test_manual_order_plan_report_states_no_order_endpoint() -> None:
    plan = build_manual_order_plan(
        pd.DataFrame(
            {
                "ticker": ["005930"],
                "candidate_action": ["review_buy"],
                "estimated_quantity": [1],
                "estimated_amount": [70_000.0],
                "reference_price": [70_000.0],
                "target_position_pct": [1.0],
                "confidence_score": [72.0],
                "screen_score": [75.0],
                "reason": ["passed"],
            }
        )
    )

    report = format_manual_order_plan_report(plan)

    assert "Review-only artifact" in report
    assert "Orders sent: 0" in report
    assert "does not call real order endpoints" in report
