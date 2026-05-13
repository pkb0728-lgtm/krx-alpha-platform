import pandas as pd

from krx_alpha.signals.signal_engine import SignalEngine


def test_signal_engine_generates_final_actions() -> None:
    score_frame = pd.DataFrame(
        {
            "date": ["2024-01-30", "2024-01-31"],
            "as_of_date": ["2024-01-30", "2024-01-31"],
            "ticker": ["005930", "005930"],
            "technical_score": [72.0, 62.0],
            "risk_score": [70.0, 70.0],
            "total_score": [71.4, 64.4],
            "signal_label": ["watch_buy", "watch"],
            "score_reason": ["close_above_ma20", "rsi_recovery_zone"],
            "scored_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 2,
        }
    )
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-30", "2024-01-31"],
            "as_of_date": ["2024-01-30", "2024-01-31"],
            "ticker": ["005930", "005930"],
            "close": [74300, 72700],
            "volume": [1000, 1200],
            "trading_value": [74300000000, 87240000000],
            "return_1d": [0.01, -0.02],
            "ma_5": [74000, 73780],
            "ma_20": [74285, 74070],
            "close_to_ma_5": [0.004, -0.015],
            "close_to_ma_20": [0.001, -0.018],
            "volume_change_5d": [0.2, 0.1],
            "trading_value_change_5d": [0.35, 0.05],
            "range_pct": [0.02, 0.03],
            "volatility_5d": [0.01, 0.02],
            "volatility_20d": [0.015, 0.018],
            "rsi_14": [53, 48],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 2,
        }
    )

    signal_frame = SignalEngine().generate(score_frame, feature_frame)

    assert list(signal_frame["final_action"]) == ["buy_candidate", "watch"]
    assert signal_frame["confidence_score"].between(0, 100).all()
    assert signal_frame.loc[0, "suggested_position_pct"] > 0


def test_signal_engine_blocks_insufficient_history() -> None:
    score_frame = pd.DataFrame(
        {
            "date": ["2024-01-05"],
            "as_of_date": ["2024-01-05"],
            "ticker": ["005930"],
            "technical_score": [72.0],
            "risk_score": [80.0],
            "total_score": [74.0],
            "signal_label": ["watch_buy"],
            "score_reason": ["close_above_ma5"],
            "scored_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-05"],
            "as_of_date": ["2024-01-05"],
            "ticker": ["005930"],
            "close": [76600],
            "volume": [1000],
            "trading_value": [76600000000],
            "return_1d": [0.0],
            "ma_5": [76000],
            "ma_20": [float("nan")],
            "close_to_ma_5": [0.01],
            "close_to_ma_20": [float("nan")],
            "volume_change_5d": [float("nan")],
            "trading_value_change_5d": [float("nan")],
            "range_pct": [0.01],
            "volatility_5d": [float("nan")],
            "volatility_20d": [float("nan")],
            "rsi_14": [float("nan")],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    signal_frame = SignalEngine().generate(score_frame, feature_frame)

    assert signal_frame.loc[0, "final_action"] == "blocked"
    assert "insufficient_history" in signal_frame.loc[0, "risk_flags"]


def test_signal_engine_blocks_unfavorable_market_regime() -> None:
    score_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "technical_score": [76.0],
            "risk_score": [80.0],
            "total_score": [77.0],
            "signal_label": ["watch_buy"],
            "score_reason": ["close_above_ma20"],
            "scored_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    feature_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "close": [72700],
            "volume": [1200],
            "trading_value": [87240000000],
            "return_1d": [0.01],
            "ma_5": [72000],
            "ma_20": [71000],
            "close_to_ma_5": [0.01],
            "close_to_ma_20": [0.02],
            "volume_change_5d": [0.2],
            "trading_value_change_5d": [0.3],
            "range_pct": [0.02],
            "volatility_5d": [0.01],
            "volatility_20d": [0.018],
            "rsi_14": [55],
            "feature_created_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    regime_frame = pd.DataFrame(
        {
            "date": ["2024-01-31"],
            "as_of_date": ["2024-01-31"],
            "ticker": ["005930"],
            "close": [72700],
            "ma_20": [71000],
            "ma_60": [76000],
            "return_20d": [-0.08],
            "volatility_20d": [0.05],
            "close_to_ma_20": [0.02],
            "close_to_ma_60": [-0.04],
            "rsi_14": [55],
            "regime": ["high_volatility"],
            "regime_score": [30.0],
            "risk_level": ["high"],
            "regime_reason": ["volatility_20d_above_threshold"],
            "generated_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    signal_frame = SignalEngine().generate(score_frame, feature_frame, regime_frame)

    assert signal_frame.loc[0, "final_action"] == "blocked"
    assert signal_frame.loc[0, "market_regime"] == "high_volatility"
    assert "market_regime_high_volatility" in signal_frame.loc[0, "risk_flags"]
