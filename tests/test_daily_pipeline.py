from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from krx_alpha.collectors.price_collector import PriceRequest
from krx_alpha.pipelines import daily_pipeline
from krx_alpha.pipelines.daily_pipeline import DailyPipeline


def test_daily_pipeline_runs_with_mock_collector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=25, freq="D").date,
            "ticker": ["005930"] * 25,
            "open": range(100, 125),
            "high": range(102, 127),
            "low": range(99, 124),
            "close": range(101, 126),
            "volume": range(1000, 1025),
            "trading_value": range(101000000000, 126000000000, 1000000000),
            "trading_value_is_estimated": [False] * 25,
            "change_rate": [1.0] * 25,
            "source": ["test"] * 25,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 25,
        }
    )

    class FakeCollector:
        def collect(self, request: PriceRequest) -> pd.DataFrame:
            return raw_frame

    monkeypatch.setattr(daily_pipeline, "PykrxPriceCollector", lambda: FakeCollector())

    request = PriceRequest(ticker="005930", start_date=date(2024, 1, 1), end_date=date(2024, 1, 31))
    result = DailyPipeline(tmp_path).run(request)

    assert result.raw_path.exists()
    assert result.processed_path.exists()
    assert result.feature_path.exists()
    assert result.regime_path.exists()
    assert result.regime_report_path.exists()
    assert result.score_path.exists()
    assert result.signal_path.exists()
    assert result.report_path.exists()
    assert result.latest_action in {"buy_candidate", "watch", "hold", "avoid", "blocked"}
    assert result.latest_market_regime in {
        "bull",
        "bear",
        "sideways",
        "high_volatility",
        "rebound",
        "neutral",
        "insufficient_data",
    }
