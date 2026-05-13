import pandas as pd

from krx_alpha.processors.price_processor import PriceProcessor


def test_price_processor_creates_processed_columns() -> None:
    raw_frame = pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03"],
            "ticker": ["5930", "005930"],
            "open": [70000, 71000],
            "high": [72000, 73000],
            "low": [69000, 70500],
            "close": [71500, 72500],
            "volume": [1000000, 1100000],
            "trading_value": [71500000000, 79750000000],
            "trading_value_is_estimated": [False, False],
            "change_rate": [1.2, 1.4],
            "source": ["test", "test"],
            "collected_at": pd.Timestamp("2026-05-13T00:00:00Z"),
        }
    )

    processed_frame = PriceProcessor().process(raw_frame)

    assert list(processed_frame["ticker"]) == ["005930", "005930"]
    assert "return_1d" in processed_frame.columns
    assert "log_return_1d" in processed_frame.columns
    assert "range_pct" in processed_frame.columns
    assert processed_frame.loc[0, "return_1d"] != processed_frame.loc[0, "return_1d"]
    assert round(float(processed_frame.loc[1, "return_1d"]), 6) == round(72500 / 71500 - 1, 6)
