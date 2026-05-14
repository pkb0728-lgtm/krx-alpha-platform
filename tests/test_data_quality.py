import pandas as pd

from krx_alpha.monitoring.data_quality import (
    QUALITY_STATUS_FAIL,
    QUALITY_STATUS_PASS,
    QUALITY_STATUS_WARN,
    PriceDataQualityChecker,
    format_data_quality_report,
    summarize_quality,
)


def test_price_data_quality_checker_passes_clean_price_frame() -> None:
    result = PriceDataQualityChecker().check(_clean_price_frame(), dataset="prices_daily")

    assert set(result["status"]) == {QUALITY_STATUS_PASS}
    summary = summarize_quality(result)
    assert summary["fail"] == 0
    assert summary["warn"] == 0

    report = format_data_quality_report(result)
    assert "Data Quality Report" in report
    assert "required_columns" in report


def test_price_data_quality_checker_flags_practical_price_issues() -> None:
    result = PriceDataQualityChecker().check(_dirty_price_frame(), dataset="prices_daily")
    statuses = dict(zip(result["check_name"], result["status"], strict=True))

    assert statuses["duplicate_date_ticker"] == QUALITY_STATUS_FAIL
    assert statuses["non_negative_market_values"] == QUALITY_STATUS_FAIL
    assert statuses["ohlc_integrity"] == QUALITY_STATUS_FAIL
    assert statuses["calendar_gap"] == QUALITY_STATUS_WARN
    assert statuses["suspicious_return"] == QUALITY_STATUS_WARN

    summary = summarize_quality(result)
    assert summary["fail"] == 3
    assert summary["warn"] == 2

    report = format_data_quality_report(result)
    assert "fix negative price" in report
    assert "corporate action" in report


def test_price_data_quality_checker_flags_empty_dataset() -> None:
    result = PriceDataQualityChecker().check(_clean_price_frame().head(0), dataset="prices_daily")
    statuses = dict(zip(result["check_name"], result["status"], strict=True))

    assert statuses["required_columns"] == QUALITY_STATUS_PASS
    assert statuses["non_empty"] == QUALITY_STATUS_FAIL


def _clean_price_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "ticker": ["005930", "005930", "005930"],
            "open": [100.0, 101.0, 102.0],
            "high": [105.0, 106.0, 107.0],
            "low": [99.0, 100.0, 101.0],
            "close": [102.0, 103.0, 104.0],
            "volume": [1000, 1100, 1200],
            "trading_value": [102000.0, 113300.0, 124800.0],
            "trading_value_is_estimated": [False, False, False],
            "source": ["demo", "demo", "demo"],
            "collected_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 3,
        }
    )


def _dirty_price_frame() -> pd.DataFrame:
    frame = _clean_price_frame().iloc[[0]].copy()
    return pd.concat(
        [
            frame,
            pd.DataFrame(
                {
                    "date": ["2024-01-15", "2024-01-15"],
                    "ticker": ["005930", "005930"],
                    "open": [200.0, 500.0],
                    "high": [210.0, 400.0],
                    "low": [190.0, 450.0],
                    "close": [200.0, 500.0],
                    "volume": [1000, -1],
                    "trading_value": [200000.0, -500.0],
                    "trading_value_is_estimated": [False, False],
                    "source": ["demo", "demo"],
                    "collected_at": [pd.Timestamp("2026-05-14T00:00:00Z")] * 2,
                }
            ),
        ],
        ignore_index=True,
    )
