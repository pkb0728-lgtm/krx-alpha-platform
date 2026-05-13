from typing import Any

REQUIRED_FINANCIAL_FEATURE_COLUMNS = {
    "corp_code",
    "ticker",
    "bsns_year",
    "reprt_code",
    "fs_div",
    "revenue",
    "operating_income",
    "net_income",
    "total_assets",
    "total_liabilities",
    "total_equity",
    "revenue_growth",
    "operating_margin",
    "net_margin",
    "debt_ratio",
    "roe",
    "financial_score",
    "financial_reason",
    "source",
    "feature_created_at",
}


def validate_financial_feature_frame(frame: Any) -> None:
    missing_columns = REQUIRED_FINANCIAL_FEATURE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required financial feature columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Financial feature frame is empty.")

    if not frame["corp_code"].astype(str).str.fullmatch(r"\d{8}").all():
        raise ValueError("Financial feature corp_code must be an eight-digit code.")

    if not frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all():
        raise ValueError("Financial feature ticker must be a six-digit code.")

    if frame["financial_score"].dropna().between(0, 100).all() is False:
        raise ValueError("financial_score values must be between 0 and 100.")
