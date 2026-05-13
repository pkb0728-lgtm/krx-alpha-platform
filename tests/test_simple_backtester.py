import pandas as pd

from krx_alpha.backtest.simple_backtester import BacktestConfig, SimpleBacktester


def test_simple_backtester_uses_next_day_entry() -> None:
    price_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=10, freq="D").date,
            "as_of_date": pd.date_range("2024-01-02", periods=10, freq="D").date,
            "ticker": ["005930"] * 10,
            "open": [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            "high": [101, 102, 103, 104, 105, 106, 107, 108, 109, 110],
            "low": [99, 100, 101, 102, 103, 104, 105, 106, 107, 108],
            "close": [100, 102, 104, 106, 108, 110, 112, 114, 116, 118],
            "volume": [1000] * 10,
            "trading_value": [100000] * 10,
            "trading_value_is_estimated": [False] * 10,
            "return_1d": [float("nan")] + [0.01] * 9,
            "log_return_1d": [float("nan")] + [0.00995] * 9,
            "range_pct": [0.02] * 10,
            "change_rate": [0.0] * 10,
            "source": ["test"] * 10,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 10,
            "processed_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 10,
        }
    )
    signal_frame = pd.DataFrame(
        {
            "date": ["2024-01-03"],
            "as_of_date": ["2024-01-03"],
            "ticker": ["005930"],
            "source_signal_label": ["watch_buy"],
            "financial_score": [50.0],
            "financial_reason": ["no_financial_feature_available"],
            "event_score": [50.0],
            "event_risk_flag": [False],
            "event_reason": ["no_disclosure_event_available"],
            "final_action": ["buy_candidate"],
            "confidence_score": [75.0],
            "risk_blocked": [False],
            "risk_flags": [""],
            "suggested_position_pct": [3.0],
            "signal_reason": ["test"],
            "generated_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    trades, metrics = SimpleBacktester(BacktestConfig(holding_days=3)).run(
        price_frame,
        signal_frame,
    )

    assert len(trades) == 1
    assert trades.loc[0, "entry_date"] == pd.Timestamp("2024-01-04").date()
    assert trades.loc[0, "exit_date"] == pd.Timestamp("2024-01-07").date()
    assert metrics.loc[0, "trade_count"] == 1
    assert metrics.loc[0, "cumulative_return"] > 0


def test_simple_backtester_handles_no_trades() -> None:
    price_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=5, freq="D").date,
            "as_of_date": pd.date_range("2024-01-02", periods=5, freq="D").date,
            "ticker": ["005930"] * 5,
            "open": [100] * 5,
            "high": [101] * 5,
            "low": [99] * 5,
            "close": [100] * 5,
            "volume": [1000] * 5,
            "trading_value": [100000] * 5,
            "trading_value_is_estimated": [False] * 5,
            "return_1d": [float("nan")] + [0.0] * 4,
            "log_return_1d": [float("nan")] + [0.0] * 4,
            "range_pct": [0.02] * 5,
            "change_rate": [0.0] * 5,
            "source": ["test"] * 5,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 5,
            "processed_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 5,
        }
    )
    signal_frame = pd.DataFrame(
        {
            "date": ["2024-01-03"],
            "as_of_date": ["2024-01-03"],
            "ticker": ["005930"],
            "source_signal_label": ["watch"],
            "financial_score": [50.0],
            "financial_reason": ["no_financial_feature_available"],
            "event_score": [50.0],
            "event_risk_flag": [False],
            "event_reason": ["no_disclosure_event_available"],
            "final_action": ["watch"],
            "confidence_score": [60.0],
            "risk_blocked": [False],
            "risk_flags": [""],
            "suggested_position_pct": [1.0],
            "signal_reason": ["test"],
            "generated_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    trades, metrics = SimpleBacktester().run(price_frame, signal_frame)

    assert trades.empty
    assert metrics.loc[0, "trade_count"] == 0
    assert metrics.loc[0, "exposure_count"] == 0
