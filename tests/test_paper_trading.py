import pandas as pd

from krx_alpha.paper_trading.simulator import PaperTradingConfig, PaperTradingSimulator


def _price_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=8, freq="D").date,
            "as_of_date": pd.date_range("2024-01-02", periods=8, freq="D").date,
            "ticker": ["005930"] * 8,
            "open": [100, 101, 102, 103, 104, 105, 106, 107],
            "high": [101, 102, 103, 104, 105, 106, 107, 108],
            "low": [99, 100, 101, 102, 103, 104, 105, 106],
            "close": [100, 102, 104, 106, 108, 110, 112, 114],
            "volume": [1000] * 8,
            "trading_value": [100000] * 8,
            "trading_value_is_estimated": [False] * 8,
            "return_1d": [float("nan")] + [0.01] * 7,
            "log_return_1d": [float("nan")] + [0.00995] * 7,
            "range_pct": [0.02] * 8,
            "change_rate": [0.0] * 8,
            "source": ["test"] * 8,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 8,
            "processed_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 8,
        }
    )


def _signal_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-01-03", "2024-01-06"],
            "as_of_date": ["2024-01-03", "2024-01-06"],
            "ticker": ["005930", "005930"],
            "source_signal_label": ["watch_buy", "avoid"],
            "financial_score": [50.0, 50.0],
            "financial_reason": ["neutral", "neutral"],
            "event_score": [50.0, 50.0],
            "event_risk_flag": [False, False],
            "event_reason": ["neutral", "neutral"],
            "flow_score": [50.0, 50.0],
            "flow_reason": ["neutral", "neutral"],
            "news_score": [50.0, 50.0],
            "news_reason": ["neutral", "neutral"],
            "macro_score": [50.0, 50.0],
            "macro_reason": ["neutral", "neutral"],
            "final_action": ["buy_candidate", "avoid"],
            "confidence_score": [75.0, 35.0],
            "risk_blocked": [False, False],
            "risk_flags": ["", ""],
            "suggested_position_pct": [10.0, 0.0],
            "signal_reason": ["paper buy", "paper exit"],
            "generated_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 2,
        }
    )


def test_paper_trading_simulator_buys_and_sells_without_real_orders() -> None:
    config = PaperTradingConfig(
        initial_cash=10_000.0,
        max_position_pct=50.0,
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
    )

    trades, positions, summary = PaperTradingSimulator(config).run(_price_frame(), _signal_frame())

    assert trades["side"].tolist() == ["buy", "sell"]
    assert trades["mode"].eq("paper").all()
    assert trades.loc[0, "execution_date"] == pd.Timestamp("2024-01-04").date()
    assert trades.loc[1, "execution_date"] == pd.Timestamp("2024-01-07").date()
    assert positions.empty
    assert summary.loc[0, "trade_count"] == 2
    assert summary.loc[0, "sell_count"] == 1
    assert summary.loc[0, "realized_pnl"] > 0
    assert summary.loc[0, "cumulative_return"] > 0


def test_paper_trading_simulator_keeps_open_position_when_no_exit() -> None:
    signal_frame = _signal_frame().head(1)
    config = PaperTradingConfig(
        initial_cash=10_000.0,
        max_position_pct=50.0,
        transaction_cost_bps=0.0,
        slippage_bps=0.0,
    )

    trades, positions, summary = PaperTradingSimulator(config).run(_price_frame(), signal_frame)

    assert len(trades) == 1
    assert trades.loc[0, "side"] == "buy"
    assert len(positions) == 1
    assert positions.loc[0, "shares"] > 0
    assert summary.loc[0, "unrealized_pnl"] > 0
