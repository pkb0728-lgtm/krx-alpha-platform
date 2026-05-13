import pandas as pd

from krx_alpha.collectors.investor_flow_collector import (
    InvestorFlowRequest,
    PykrxInvestorFlowCollector,
)


def test_investor_flow_collector_normalizes_provider_frames() -> None:
    value_frame = pd.DataFrame(
        {
            "개인": [-1000.0],
            "외국인합계": [700.0],
            "기관합계": [300.0],
            "기타법인": [0.0],
            "전체": [0.0],
        },
        index=pd.to_datetime(["2024-01-02"]),
    )
    volume_frame = pd.DataFrame(
        {
            "개인": [-10.0],
            "외국인합계": [7.0],
            "기관합계": [3.0],
            "기타법인": [0.0],
            "전체": [0.0],
        },
        index=pd.to_datetime(["2024-01-02"]),
    )

    def provider(start_date: str, end_date: str, ticker: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        assert start_date == "20240102"
        assert end_date == "20240102"
        assert ticker == "005930"
        return value_frame, volume_frame

    request = InvestorFlowRequest.from_strings(
        ticker="005930",
        start_date="2024-01-02",
        end_date="2024-01-02",
        demo=False,
    )
    frame = PykrxInvestorFlowCollector(provider=provider).collect(request)

    assert len(frame) == 1
    assert frame.loc[0, "foreign_net_buy_value"] == 700.0
    assert frame.loc[0, "institution_net_buy_volume"] == 3.0
    assert frame.loc[0, "ticker"] == "005930"


def test_investor_flow_collector_demo_mode_creates_rows() -> None:
    request = InvestorFlowRequest.from_strings(
        ticker="005930",
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    frame = PykrxInvestorFlowCollector().collect(request)

    assert not frame.empty
    assert frame["ticker"].eq("005930").all()
    assert frame["source"].eq("demo_investor_flow").all()
