from typing import Any

REQUIRED_INVESTOR_FLOW_COLUMNS = {
    "date",
    "ticker",
    "individual_net_buy_value",
    "foreign_net_buy_value",
    "institution_net_buy_value",
    "other_net_buy_value",
    "total_net_buy_value",
    "individual_net_buy_volume",
    "foreign_net_buy_volume",
    "institution_net_buy_volume",
    "other_net_buy_volume",
    "total_net_buy_volume",
    "source",
    "collected_at",
}

REQUIRED_INVESTOR_FLOW_FEATURE_COLUMNS = {
    "date",
    "as_of_date",
    "ticker",
    "individual_net_buy_value",
    "foreign_net_buy_value",
    "institution_net_buy_value",
    "smart_money_net_buy_value",
    "foreign_net_buy_value_5d",
    "institution_net_buy_value_5d",
    "smart_money_net_buy_value_5d",
    "flow_score",
    "flow_reason",
    "feature_created_at",
}


def validate_investor_flow_frame(frame: Any) -> None:
    missing_columns = REQUIRED_INVESTOR_FLOW_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required investor flow columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Investor flow frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Investor flow frame contains null dates.")

    if not frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all():
        raise ValueError("Investor flow ticker must be a six-digit code.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Investor flow frame contains duplicated date/ticker rows.")


def validate_investor_flow_feature_frame(frame: Any) -> None:
    missing_columns = REQUIRED_INVESTOR_FLOW_FEATURE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(
            f"Missing required investor flow feature columns: {sorted(missing_columns)}"
        )

    if frame.empty:
        raise ValueError("Investor flow feature frame is empty.")

    if frame["date"].isna().any():
        raise ValueError("Investor flow feature frame contains null dates.")

    if not frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all():
        raise ValueError("Investor flow feature ticker must be a six-digit code.")

    if frame.duplicated(subset=["date", "ticker"]).any():
        raise ValueError("Investor flow feature frame contains duplicated date/ticker rows.")

    if frame["flow_score"].dropna().between(0, 100).all() is False:
        raise ValueError("flow_score values must be between 0 and 100.")
