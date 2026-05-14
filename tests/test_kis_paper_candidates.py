from pathlib import Path

import pandas as pd

from krx_alpha.broker.kis_candidates import (
    KISPaperCandidateBuilder,
    KISPaperCandidateConfig,
    enrich_screening_reference_prices,
    format_kis_paper_candidate_report,
)
from krx_alpha.broker.kis_paper import KISPaperBalance, KISPaperHolding


def test_kis_paper_candidate_builder_creates_review_rows_without_orders() -> None:
    screening = pd.DataFrame(
        [
            {
                "ticker": "005380",
                "passed": True,
                "risk_blocked": False,
                "final_action": "buy_candidate",
                "review_priority": "high",
                "screen_status_reason": "passed",
                "screen_score": 75.0,
                "confidence_score": 72.0,
                "suggested_position_pct": 10.0,
                "reference_price": 50_000.0,
                "reference_price_source": "feature_close",
                "risk_flags": "",
                "evidence_summary": "strong signal",
                "caution_summary": "review liquidity",
            },
            {
                "ticker": "005930",
                "passed": True,
                "risk_blocked": False,
                "final_action": "buy_candidate",
                "review_priority": "medium",
                "screen_status_reason": "passed",
                "screen_score": 70.0,
                "confidence_score": 68.0,
                "suggested_position_pct": 20.0,
                "reference_price": 65_000.0,
                "reference_price_source": "feature_close",
                "risk_flags": "",
                "evidence_summary": "add signal",
                "caution_summary": "review disclosure",
            },
            {
                "ticker": "000660",
                "passed": False,
                "risk_blocked": True,
                "final_action": "watch",
                "review_priority": "blocked",
                "screen_status_reason": "risk_blocked",
                "screen_score": 45.0,
                "confidence_score": 55.0,
                "suggested_position_pct": 0.0,
                "reference_price": 100_000.0,
                "reference_price_source": "feature_close",
                "risk_flags": "high_volatility",
                "evidence_summary": "weak",
                "caution_summary": "blocked",
            },
        ]
    )
    balance = _sample_balance()

    result = KISPaperCandidateBuilder(
        KISPaperCandidateConfig(max_candidates=10, cash_buffer_pct=0.0)
    ).build(screening, balance)

    new_buy = result[result["ticker"] == "005380"].iloc[0]
    held_add = result[result["ticker"] == "005930"].iloc[0]
    blocked = result[result["ticker"] == "000660"].iloc[0]

    assert new_buy["candidate_action"] == "review_buy"
    assert new_buy["estimated_quantity"] == 2
    assert new_buy["estimated_amount"] == 100_000.0
    assert held_add["candidate_action"] == "review_add"
    assert held_add["estimated_quantity"] == 1
    assert blocked["candidate_action"] == "skip"
    assert int(result["orders_sent"].sum()) == 0


def test_kis_paper_candidate_builder_requires_manual_price_when_price_missing() -> None:
    screening = pd.DataFrame(
        [
            {
                "ticker": "005380",
                "passed": True,
                "risk_blocked": False,
                "final_action": "buy_candidate",
                "review_priority": "high",
                "screen_status_reason": "passed",
                "screen_score": 75.0,
                "confidence_score": 72.0,
                "suggested_position_pct": 10.0,
            }
        ]
    )

    result = KISPaperCandidateBuilder(
        KISPaperCandidateConfig(max_candidates=10, cash_buffer_pct=0.0)
    ).build(screening, _sample_balance())

    row = result.iloc[0]
    assert row["candidate_action"] == "manual_price_required"
    assert row["estimated_quantity"] == 0
    assert "reference_price_missing" in str(row["reason"])


def test_enrich_screening_reference_prices_reads_latest_feature_close(tmp_path: Path) -> None:
    signal_path = tmp_path / "data" / "signals" / "final_signals_daily" / "005930_demo.parquet"
    feature_path = tmp_path / "data" / "features" / "prices_daily" / signal_path.name
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"date": "2024-01-02", "close": 70_000.0},
            {"date": "2024-01-03", "close": 72_000.0},
        ]
    ).to_parquet(feature_path, index=False)
    screening = pd.DataFrame([{"ticker": "005930", "signal_path": str(signal_path)}])

    result = enrich_screening_reference_prices(screening, tmp_path)

    assert result.loc[0, "reference_price"] == 72_000.0
    assert result.loc[0, "reference_price_source"] == "feature_close"


def test_kis_paper_candidate_report_states_no_orders_sent() -> None:
    result = KISPaperCandidateBuilder(
        KISPaperCandidateConfig(max_candidates=10, cash_buffer_pct=0.0)
    ).build(
        pd.DataFrame(
            [
                {
                    "ticker": "005380",
                    "passed": True,
                    "risk_blocked": False,
                    "final_action": "buy_candidate",
                    "review_priority": "high",
                    "screen_status_reason": "passed",
                    "screen_score": 75.0,
                    "confidence_score": 72.0,
                    "suggested_position_pct": 10.0,
                    "reference_price": 50_000.0,
                }
            ]
        ),
        _sample_balance(),
    )

    report = format_kis_paper_candidate_report(result)

    assert "Orders sent: 0" in report
    assert "never calls a KIS order endpoint" in report


def _sample_balance() -> KISPaperBalance:
    return KISPaperBalance(
        account="12345678-01",
        cash_amount=500_000.0,
        total_evaluation_amount=1_000_000.0,
        stock_evaluation_amount=130_000.0,
        purchase_amount=120_000.0,
        profit_loss_amount=10_000.0,
        profit_loss_rate=8.3,
        holdings=[
            KISPaperHolding(
                ticker="005930",
                name="Samsung Electronics",
                quantity=2,
                orderable_quantity=2,
                average_price=60_000.0,
                current_price=65_000.0,
                evaluation_amount=130_000.0,
                profit_loss_amount=10_000.0,
                profit_loss_rate=8.3,
            )
        ],
    )
