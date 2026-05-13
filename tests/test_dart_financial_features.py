import pandas as pd

from krx_alpha.features.dart_financial_features import DartFinancialFeatureBuilder


def test_dart_financial_feature_builder_creates_ratios_and_score() -> None:
    financial_frame = pd.DataFrame(
        {
            "corp_code": ["00126380"] * 6,
            "ticker": ["005930"] * 6,
            "bsns_year": ["2023"] * 6,
            "reprt_code": ["11011"] * 6,
            "fs_div": ["CFS"] * 6,
            "sj_div": ["IS", "IS", "IS", "BS", "BS", "BS"],
            "account_id": [
                "ifrs-full_Revenue",
                "dart_OperatingIncomeLoss",
                "ifrs-full_ProfitLoss",
                "ifrs-full_Assets",
                "ifrs-full_Liabilities",
                "ifrs-full_Equity",
            ],
            "account_nm": [
                "Revenue",
                "Operating income",
                "Net income",
                "Assets",
                "Liabilities",
                "Equity",
            ],
            "thstrm_amount": ["1,200", "240", "180", "3,000", "900", "2,100"],
            "thstrm_amount_value": [1200.0, 240.0, 180.0, 3000.0, 900.0, 2100.0],
            "frmtrm_amount": ["1,000", "100", "80", "2,800", "1,000", "1,800"],
            "frmtrm_amount_value": [1000.0, 100.0, 80.0, 2800.0, 1000.0, 1800.0],
            "currency": ["KRW"] * 6,
            "source": ["opendart_demo"] * 6,
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")] * 6,
        }
    )

    feature_frame = DartFinancialFeatureBuilder().build(financial_frame)

    assert len(feature_frame) == 1
    assert round(float(feature_frame.loc[0, "revenue_growth"]), 4) == 0.2
    assert round(float(feature_frame.loc[0, "operating_margin"]), 4) == 0.2
    assert round(float(feature_frame.loc[0, "debt_ratio"]), 4) == 0.4286
    assert feature_frame.loc[0, "financial_score"] > 80
    assert "revenue_growth_positive" in feature_frame.loc[0, "financial_reason"]


def test_dart_financial_feature_builder_handles_missing_optional_accounts() -> None:
    financial_frame = pd.DataFrame(
        {
            "corp_code": ["00126380"],
            "ticker": ["005930"],
            "bsns_year": ["2023"],
            "reprt_code": ["11011"],
            "fs_div": ["CFS"],
            "sj_div": ["IS"],
            "account_id": ["ifrs-full_Revenue"],
            "account_nm": ["Revenue"],
            "thstrm_amount": ["1,200"],
            "thstrm_amount_value": [1200.0],
            "frmtrm_amount": ["1,000"],
            "frmtrm_amount_value": [1000.0],
            "currency": ["KRW"],
            "source": ["opendart_demo"],
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    feature_frame = DartFinancialFeatureBuilder().build(financial_frame)

    assert len(feature_frame) == 1
    assert feature_frame.loc[0, "revenue"] == 1200.0
    assert pd.isna(feature_frame.loc[0, "operating_margin"])
    assert feature_frame.loc[0, "financial_score"] >= 50
