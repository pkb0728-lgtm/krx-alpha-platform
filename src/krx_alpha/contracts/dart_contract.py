from typing import Any

REQUIRED_DART_COMPANY_COLUMNS = {
    "corp_code",
    "stock_code",
    "stock_name",
    "corp_name",
    "corp_name_eng",
    "corp_cls",
    "ceo_nm",
    "induty_code",
    "est_dt",
    "acc_mt",
    "source",
    "collected_at",
}

REQUIRED_DART_FINANCIAL_COLUMNS = {
    "corp_code",
    "ticker",
    "bsns_year",
    "reprt_code",
    "fs_div",
    "sj_div",
    "account_id",
    "account_nm",
    "thstrm_amount",
    "thstrm_amount_value",
    "frmtrm_amount",
    "frmtrm_amount_value",
    "currency",
    "source",
    "collected_at",
}

REQUIRED_DART_DISCLOSURE_COLUMNS = {
    "corp_code",
    "corp_name",
    "stock_code",
    "report_nm",
    "rcept_no",
    "rcept_dt",
    "flr_nm",
    "rm",
    "source",
    "collected_at",
}


def validate_dart_company_frame(frame: Any) -> None:
    _validate_required_columns(frame, REQUIRED_DART_COMPANY_COLUMNS, "DART company")
    if frame.empty:
        raise ValueError("DART company frame is empty.")
    if not frame["corp_code"].astype(str).str.fullmatch(r"\d{8}").all():
        raise ValueError("DART company corp_code must be an eight-digit code.")
    if not frame["stock_code"].astype(str).str.fullmatch(r"\d{6}").all():
        raise ValueError("DART company stock_code must be a six-digit ticker.")


def validate_dart_financial_frame(frame: Any) -> None:
    _validate_required_columns(frame, REQUIRED_DART_FINANCIAL_COLUMNS, "DART financial")
    if frame.empty:
        raise ValueError("DART financial frame is empty.")
    if not frame["corp_code"].astype(str).str.fullmatch(r"\d{8}").all():
        raise ValueError("DART financial corp_code must be an eight-digit code.")
    if not frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all():
        raise ValueError("DART financial ticker must be a six-digit code.")
    if frame[["bsns_year", "reprt_code", "account_nm"]].isna().any().any():
        raise ValueError("DART financial frame contains null business fields.")


def validate_dart_disclosure_frame(frame: Any) -> None:
    _validate_required_columns(frame, REQUIRED_DART_DISCLOSURE_COLUMNS, "DART disclosure")
    if frame.empty:
        raise ValueError("DART disclosure frame is empty.")
    if not frame["corp_code"].astype(str).str.fullmatch(r"\d{8}").all():
        raise ValueError("DART disclosure corp_code must be an eight-digit code.")
    if frame["rcept_no"].isna().any():
        raise ValueError("DART disclosure frame contains null receipt numbers.")


def _validate_required_columns(frame: Any, required_columns: set[str], name: str) -> None:
    missing_columns = required_columns - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required {name} columns: {sorted(missing_columns)}")
