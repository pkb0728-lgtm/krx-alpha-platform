import pandas as pd
from krx_alpha.collectors.price_collector import PriceRequest, PykrxPriceCollector

OPEN = "\uc2dc\uac00"
HIGH = "\uace0\uac00"
LOW = "\uc800\uac00"
CLOSE = "\uc885\uac00"
VOLUME = "\uac70\ub798\ub7c9"
TRADING_VALUE = "\uac70\ub798\ub300\uae08"
CHANGE_RATE = "\ub4f1\ub77d\ub960"


def test_collect_price_normalizes_pykrx_columns() -> None:
    raw_frame = pd.DataFrame(
        {
            OPEN: [70000, 71000],
            HIGH: [72000, 73000],
            LOW: [69000, 70500],
            CLOSE: [71500, 72500],
            VOLUME: [1000000, 1100000],
            TRADING_VALUE: [71500000000, 79750000000],
            CHANGE_RATE: [1.2, 1.4],
        },
        index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
    )

    def provider(start_date: str, end_date: str, ticker: str, adjusted: bool) -> pd.DataFrame:
        assert start_date == "20240101"
        assert end_date == "20240131"
        assert ticker == "005930"
        assert adjusted is True
        return raw_frame

    request = PriceRequest.from_strings("5930", "2024-01-01", "2024-01-31")
    frame = PykrxPriceCollector(provider=provider).collect(request)

    assert list(frame["ticker"].unique()) == ["005930"]
    assert list(frame["close"]) == [71500, 72500]
    assert list(frame["trading_value_is_estimated"]) == [False, False]
    assert "collected_at" in frame.columns


def test_collect_price_estimates_trading_value_when_missing() -> None:
    raw_frame = pd.DataFrame(
        {
            OPEN: [70000],
            HIGH: [72000],
            LOW: [69000],
            CLOSE: [71500],
            VOLUME: [1000000],
        },
        index=pd.to_datetime(["2024-01-02"]),
    )

    def provider(start_date: str, end_date: str, ticker: str, adjusted: bool) -> pd.DataFrame:
        return raw_frame

    request = PriceRequest.from_strings("005930", "2024-01-01", "2024-01-31")
    frame = PykrxPriceCollector(provider=provider).collect(request)

    assert frame.loc[0, "trading_value"] == 71500000000
    assert bool(frame.loc[0, "trading_value_is_estimated"]) is True
