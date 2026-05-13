import pandas as pd

from krx_alpha.backtest.walk_forward import WalkForwardBacktester, WalkForwardConfig


def test_walk_forward_backtester_creates_fold_metrics() -> None:
    dates = pd.date_range("2024-01-02", periods=45, freq="D").date
    price_frame = pd.DataFrame(
        {
            "date": dates,
            "as_of_date": dates,
            "ticker": ["005930"] * len(dates),
            "open": [100 + index for index in range(len(dates))],
            "high": [101 + index for index in range(len(dates))],
            "low": [99 + index for index in range(len(dates))],
            "close": [101 + index for index in range(len(dates))],
            "volume": [1000] * len(dates),
            "trading_value": [100000] * len(dates),
            "trading_value_is_estimated": [False] * len(dates),
            "return_1d": [float("nan")] + [0.01] * (len(dates) - 1),
            "log_return_1d": [float("nan")] + [0.00995] * (len(dates) - 1),
            "range_pct": [0.02] * len(dates),
            "change_rate": [0.0] * len(dates),
            "source": ["test"] * len(dates),
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * len(dates),
            "processed_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * len(dates),
        }
    )
    signal_dates = dates[:30]
    signal_frame = _signal_frame(signal_dates)

    folds, summary = WalkForwardBacktester(
        WalkForwardConfig(train_size=10, test_size=5, step_size=5, holding_days=3)
    ).run(price_frame, signal_frame)

    assert len(folds) == 4
    assert summary.loc[0, "fold_count"] == 4
    assert summary.loc[0, "total_trade_count"] > 0
    assert folds["test_start"].min() > folds["train_start"].min()


def test_walk_forward_backtester_handles_insufficient_dates() -> None:
    dates = pd.date_range("2024-01-02", periods=8, freq="D").date
    price_frame = pd.DataFrame(
        {
            "date": dates,
            "as_of_date": dates,
            "ticker": ["005930"] * len(dates),
            "open": [100] * len(dates),
            "high": [101] * len(dates),
            "low": [99] * len(dates),
            "close": [100] * len(dates),
            "volume": [1000] * len(dates),
            "trading_value": [100000] * len(dates),
            "trading_value_is_estimated": [False] * len(dates),
            "return_1d": [float("nan")] + [0.0] * (len(dates) - 1),
            "log_return_1d": [float("nan")] + [0.0] * (len(dates) - 1),
            "range_pct": [0.02] * len(dates),
            "change_rate": [0.0] * len(dates),
            "source": ["test"] * len(dates),
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * len(dates),
            "processed_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * len(dates),
        }
    )

    folds, summary = WalkForwardBacktester(
        WalkForwardConfig(train_size=10, test_size=5, step_size=5)
    ).run(price_frame, _signal_frame(dates))

    assert folds.empty
    assert summary.loc[0, "fold_count"] == 0
    assert summary.loc[0, "total_trade_count"] == 0


def _signal_frame(dates: object) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": dates,
            "as_of_date": dates,
            "ticker": ["005930"] * len(dates),  # type: ignore[arg-type]
            "source_signal_label": ["watch_buy"] * len(dates),  # type: ignore[arg-type]
            "financial_score": [50.0] * len(dates),  # type: ignore[arg-type]
            "financial_reason": ["no_financial_feature_available"] * len(dates),  # type: ignore[arg-type]
            "event_score": [50.0] * len(dates),  # type: ignore[arg-type]
            "event_risk_flag": [False] * len(dates),  # type: ignore[arg-type]
            "event_reason": ["no_disclosure_event_available"] * len(dates),  # type: ignore[arg-type]
            "flow_score": [50.0] * len(dates),  # type: ignore[arg-type]
            "flow_reason": ["no_investor_flow_available"] * len(dates),  # type: ignore[arg-type]
            "final_action": ["buy_candidate"] * len(dates),  # type: ignore[arg-type]
            "confidence_score": [75.0] * len(dates),  # type: ignore[arg-type]
            "risk_blocked": [False] * len(dates),  # type: ignore[arg-type]
            "risk_flags": [""] * len(dates),  # type: ignore[arg-type]
            "suggested_position_pct": [3.0] * len(dates),  # type: ignore[arg-type]
            "signal_reason": ["test"] * len(dates),  # type: ignore[arg-type]
            "generated_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * len(dates),  # type: ignore[arg-type]
        }
    )
