import pandas as pd

from krx_alpha.features.investor_flow_features import InvestorFlowFeatureBuilder


def test_investor_flow_feature_builder_scores_smart_money_inflow() -> None:
    flow_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=5, freq="D").date,
            "ticker": ["005930"] * 5,
            "individual_net_buy_value": [-8_000_000_000.0] * 5,
            "foreign_net_buy_value": [5_000_000_000.0] * 5,
            "institution_net_buy_value": [3_000_000_000.0] * 5,
            "other_net_buy_value": [0.0] * 5,
            "total_net_buy_value": [0.0] * 5,
            "individual_net_buy_volume": [-100_000.0] * 5,
            "foreign_net_buy_volume": [70_000.0] * 5,
            "institution_net_buy_volume": [30_000.0] * 5,
            "other_net_buy_volume": [0.0] * 5,
            "total_net_buy_volume": [0.0] * 5,
            "source": ["test"] * 5,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 5,
        }
    )

    feature_frame = InvestorFlowFeatureBuilder().build(flow_frame)

    latest = feature_frame.sort_values("date").iloc[-1]
    assert latest["smart_money_net_buy_value_5d"] == 40_000_000_000.0
    assert latest["flow_score"] > 80
    assert "strong_smart_money_inflow" in latest["flow_reason"]


def test_investor_flow_feature_builder_scores_outflow() -> None:
    flow_frame = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-02", periods=5, freq="D").date,
            "ticker": ["005930"] * 5,
            "individual_net_buy_value": [8_000_000_000.0] * 5,
            "foreign_net_buy_value": [-5_000_000_000.0] * 5,
            "institution_net_buy_value": [-3_000_000_000.0] * 5,
            "other_net_buy_value": [0.0] * 5,
            "total_net_buy_value": [0.0] * 5,
            "individual_net_buy_volume": [100_000.0] * 5,
            "foreign_net_buy_volume": [-70_000.0] * 5,
            "institution_net_buy_volume": [-30_000.0] * 5,
            "other_net_buy_volume": [0.0] * 5,
            "total_net_buy_volume": [0.0] * 5,
            "source": ["test"] * 5,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 5,
        }
    )

    feature_frame = InvestorFlowFeatureBuilder().build(flow_frame)

    latest = feature_frame.sort_values("date").iloc[-1]
    assert latest["flow_score"] < 40
    assert "strong_smart_money_outflow" in latest["flow_reason"]
