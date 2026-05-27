from pathlib import Path

import pandas as pd

from krx_alpha.database.storage import (
    final_signal_file_path,
    processed_price_file_path,
    write_parquet,
)
from krx_alpha.journal.decision_journal import (
    append_decision_journal,
    build_decision_journal_frame,
    evaluate_decision_journal,
)


def test_decision_journal_builds_and_appends_unique_rows(tmp_path: Path) -> None:
    signal_path = final_signal_file_path(tmp_path, "005930", "20240101", "20240131")
    write_parquet(
        pd.DataFrame(
            {
                "date": ["2024-01-30", "2024-01-31"],
                "ticker": ["005930", "005930"],
            }
        ),
        signal_path,
    )
    summary = pd.DataFrame(
        {
            "ticker": ["005930"],
            "status": ["success"],
            "latest_action": ["watch"],
            "latest_confidence_score": [63.0],
            "latest_financial_score": [55.0],
            "latest_event_score": [50.0],
            "latest_flow_score": [60.0],
            "latest_news_score": [52.0],
            "latest_macro_score": [50.0],
            "latest_market_regime": ["neutral"],
            "signal_path": [str(signal_path)],
            "report_path": ["report.md"],
            "error": [""],
        }
    )
    screening = pd.DataFrame(
        {
            "ticker": ["005930"],
            "passed": [False],
            "screen_status_reason": ["confidence_below_threshold"],
            "review_priority": ["watchlist"],
            "screen_score": [58.0],
            "risk_blocked": [False],
            "risk_flags": [""],
            "suggested_position_pct": [1.2],
        }
    )
    kis_candidates = pd.DataFrame(
        {
            "ticker": ["005930"],
            "candidate_action": ["hold_review"],
            "estimated_quantity": [0],
            "estimated_amount": [0.0],
            "reason": ["final_action_watch"],
        }
    )

    journal_rows = build_decision_journal_frame(
        universe="demo",
        start_date="2024-01-01",
        end_date="2024-01-31",
        summary_frame=summary,
        screening_frame=screening,
        kis_candidate_frame=kis_candidates,
        generated_at=pd.Timestamp("2026-05-27T00:00:00Z"),
    )
    result = append_decision_journal(tmp_path, journal_rows)
    second_result = append_decision_journal(tmp_path, journal_rows)

    assert journal_rows.loc[0, "decision_date"] == "2024-01-31"
    assert journal_rows.loc[0, "screen_status_reason"] == "confidence_below_threshold"
    assert journal_rows.loc[0, "kis_candidate_action"] == "hold_review"
    assert result.appended_count == 1
    assert second_result.total_count == 1
    assert second_result.parquet_path.exists()
    assert second_result.csv_path.exists()


def test_decision_journal_evaluates_forward_returns(tmp_path: Path) -> None:
    _write_price_history(
        tmp_path,
        ticker="005930",
        closes=[100.0, 101.0, 104.0, 103.0],
    )
    _write_price_history(
        tmp_path,
        ticker="000660",
        closes=[100.0, 98.0, 95.0, 96.0],
    )
    journal = pd.DataFrame(
        {
            "journal_id": ["a", "b"],
            "universe": ["demo", "demo"],
            "ticker": ["005930", "000660"],
            "decision_date": ["2024-01-02", "2024-01-02"],
            "latest_action": ["buy_candidate", "blocked"],
            "latest_confidence_score": [75.0, 30.0],
            "latest_market_regime": ["bull", "high_volatility"],
        }
    )
    result = evaluate_decision_journal(
        tmp_path,
        holding_days=2,
        journal_frame=journal,
    )

    evaluated = result.frame.sort_values("ticker").reset_index(drop=True)
    blocked = evaluated[evaluated["ticker"] == "000660"].iloc[0]
    buy = evaluated[evaluated["ticker"] == "005930"].iloc[0]

    assert result.evaluated_count == 2
    assert result.pending_count == 0
    assert round(float(buy["forward_return"]), 4) == 0.04
    assert bool(buy["favorable_outcome"]) is True
    assert bool(blocked["favorable_outcome"]) is True
    assert result.parquet_path.exists()
    assert result.csv_path.exists()
    assert result.report_path.exists()
    assert set(result.summary["latest_action"]) == {"buy_candidate", "blocked"}


def _write_price_history(tmp_path: Path, ticker: str, closes: list[float]) -> None:
    dates = pd.date_range("2024-01-02", periods=len(closes), freq="D")
    price_path = processed_price_file_path(tmp_path, ticker, "20240102", "20240105")
    write_parquet(
        pd.DataFrame(
            {
                "date": dates,
                "ticker": [ticker] * len(closes),
                "close": closes,
            }
        ),
        price_path,
    )
