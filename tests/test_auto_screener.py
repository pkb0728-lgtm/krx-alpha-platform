from pathlib import Path

import pandas as pd

from krx_alpha.screening.auto_screener import AutoScreener, format_screening_report


def test_auto_screener_builds_human_review_shortlist(tmp_path: Path) -> None:
    signal_dir = tmp_path / "data" / "signals" / "final_signals_daily"
    feature_dir = tmp_path / "data" / "features" / "prices_daily"
    signal_dir.mkdir(parents=True)
    feature_dir.mkdir(parents=True)
    signal_path = signal_dir / "005930_20240101_20240131.parquet"
    feature_path = feature_dir / signal_path.name

    _final_signal_frame("005930", action="buy_candidate", confidence=75.0).to_parquet(
        signal_path,
        index=False,
    )
    _feature_frame("005930").to_parquet(feature_path, index=False)
    universe_summary = pd.DataFrame(
        {
            "ticker": ["005930"],
            "status": ["success"],
            "latest_action": ["buy_candidate"],
            "latest_confidence_score": [75.0],
            "latest_market_regime": ["bull"],
            "signal_path": [str(signal_path)],
        }
    )

    result = AutoScreener(tmp_path).screen(universe_summary)

    assert len(result) == 1
    assert bool(result.loc[0, "passed"]) is True
    assert result.loc[0, "screen_status_reason"] == "passed"
    assert result.loc[0, "review_priority"] == "high"
    assert result.loc[0, "risk_flags"] == ""
    assert result.loc[0, "screen_score"] >= 65.0
    assert "buy_candidate_signal" in result.loc[0, "reasons"]
    assert "trading_value_surge" in result.loc[0, "reasons"]
    assert "risk filter passed" in result.loc[0, "evidence_summary"]
    assert "confirm_recent_news" in result.loc[0, "review_checklist"]

    report = format_screening_report(result)
    assert "Auto Screener Report" in report
    assert "Priority summary: high 1" in report
    assert "Status summary: passed 1" in report
    assert "Candidate Review Cards" in report
    assert "Review Queue" in report
    assert "no_blocked_or_watchlist_rows" in report
    assert "Status reason: passed" in report
    assert "Priority: high" in report
    assert "Risk flags: none" in report
    assert "Evidence:" in report
    assert "Caution:" in report
    assert "005930" in report


def test_auto_screener_flags_missing_signal_file(tmp_path: Path) -> None:
    universe_summary = pd.DataFrame(
        {
            "ticker": ["000660"],
            "status": ["success"],
            "latest_action": ["watch"],
            "latest_confidence_score": [62.0],
            "latest_market_regime": ["neutral"],
            "signal_path": [str(tmp_path / "missing.parquet")],
        }
    )

    result = AutoScreener(tmp_path).screen(universe_summary)

    assert bool(result.loc[0, "passed"]) is False
    assert result.loc[0, "screen_status_reason"] == "signal_file_missing_or_empty"
    assert result.loc[0, "review_priority"] == "blocked"
    assert result.loc[0, "risk_flags"] == "signal_file_missing_or_empty"
    assert result.loc[0, "screen_score"] == 0.0
    assert result.loc[0, "reasons"] == "signal_file_missing_or_empty"
    assert result.loc[0, "review_checklist"] == "rerun_pipeline, confirm_signal_artifact"

    report = format_screening_report(result)
    assert "Review Queue" in report
    assert "signal_file_missing_or_empty" in report
    assert "Do not review this ticker" in report


def _final_signal_frame(ticker: str, action: str, confidence: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-01-30", "2024-01-31"],
            "as_of_date": ["2024-01-30", "2024-01-31"],
            "ticker": [ticker, ticker],
            "source_signal_label": ["watch", action],
            "financial_score": [50.0, 70.0],
            "financial_reason": ["neutral", "financial_score_supportive"],
            "event_score": [50.0, 50.0],
            "event_risk_flag": [False, False],
            "event_reason": ["neutral", "neutral"],
            "flow_score": [50.0, 65.0],
            "flow_reason": ["neutral", "flow_score_supportive"],
            "news_score": [50.0, 68.0],
            "news_reason": ["neutral", "news_score_supportive"],
            "macro_score": [50.0, 50.0],
            "macro_reason": ["neutral", "neutral"],
            "market_regime": ["neutral", "bull"],
            "market_regime_score": [50.0, 70.0],
            "market_regime_risk_level": ["medium", "low"],
            "final_action": ["watch", action],
            "confidence_score": [55.0, confidence],
            "risk_blocked": [False, False],
            "risk_flags": ["", ""],
            "suggested_position_pct": [1.0, 3.5],
            "signal_reason": ["watch", "buy"],
            "generated_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 2,
        }
    )


def _feature_frame(ticker: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-01-30", "2024-01-31"],
            "as_of_date": ["2024-01-30", "2024-01-31"],
            "ticker": [ticker, ticker],
            "close": [70000.0, 72000.0],
            "volume": [1000, 1200],
            "trading_value": [70_000_000.0, 86_400_000.0],
            "return_1d": [0.01, 0.02],
            "ma_5": [69000.0, 70000.0],
            "ma_20": [68000.0, 69000.0],
            "close_to_ma_5": [0.01, 0.03],
            "close_to_ma_20": [0.02, 0.04],
            "volume_change_5d": [0.05, 0.12],
            "trading_value_change_5d": [0.05, 0.15],
            "range_pct": [0.02, 0.02],
            "volatility_5d": [0.01, 0.015],
            "volatility_20d": [0.02, 0.02],
            "rsi_14": [48.0, 55.0],
            "feature_created_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 2,
        }
    )
