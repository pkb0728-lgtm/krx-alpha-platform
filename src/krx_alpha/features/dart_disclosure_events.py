from typing import Any

import pandas as pd

from krx_alpha.contracts.dart_contract import validate_dart_disclosure_frame
from krx_alpha.contracts.disclosure_event_contract import validate_disclosure_event_frame

DISCLOSURE_EVENT_COLUMNS = [
    "date",
    "as_of_date",
    "corp_code",
    "ticker",
    "report_nm",
    "rcept_no",
    "event_category",
    "event_score",
    "event_risk_flag",
    "event_reason",
    "source",
    "feature_created_at",
]

RiskRule = tuple[tuple[str, ...], str, str, float]

RISK_RULES: tuple[RiskRule, ...] = (
    (
        ("유상증자", "capital increase", "rights offering"),
        "capital_raise_risk",
        "disclosure_risk_capital_increase",
        20.0,
    ),
    (
        ("전환사채", "convertible bond", "cb"),
        "convertible_bond_risk",
        "disclosure_risk_convertible_bond",
        25.0,
    ),
    (
        ("신주인수권", "bond with warrant", "bw"),
        "warrant_risk",
        "disclosure_risk_warrant",
        25.0,
    ),
    (
        ("소송", "lawsuit", "litigation"),
        "litigation_risk",
        "disclosure_risk_litigation",
        25.0,
    ),
    (
        ("횡령", "배임", "embezzlement", "breach of trust"),
        "governance_risk",
        "disclosure_risk_governance",
        10.0,
    ),
    (
        ("상장폐지", "거래정지", "delisting", "trading suspension"),
        "listing_risk",
        "disclosure_risk_listing",
        5.0,
    ),
    (
        ("감사의견", "audit opinion", "관리종목", "불성실"),
        "audit_or_designation_risk",
        "disclosure_risk_audit_or_designation",
        20.0,
    ),
)

POSITIVE_RULES: tuple[RiskRule, ...] = (
    (
        ("자기주식취득", "share repurchase", "treasury stock acquisition"),
        "shareholder_return",
        "disclosure_positive_shareholder_return",
        65.0,
    ),
    (
        ("현금배당", "dividend"),
        "shareholder_return",
        "disclosure_positive_dividend",
        60.0,
    ),
    (
        ("공급계약", "supply contract", "contract award"),
        "business_momentum",
        "disclosure_positive_contract",
        60.0,
    ),
)

ROUTINE_KEYWORDS = (
    "사업보고서",
    "분기보고서",
    "반기보고서",
    "annual report",
    "quarterly report",
    "semi-annual report",
    "business report",
)


class DartDisclosureEventBuilder:
    """Convert normalized OpenDART disclosure rows into event features."""

    def build(self, disclosure_frame: Any) -> Any:
        validate_dart_disclosure_frame(disclosure_frame)
        frame = disclosure_frame.copy()
        frame["corp_code"] = frame["corp_code"].astype(str).str.zfill(8)
        frame["ticker"] = frame["stock_code"].astype(str).str.zfill(6)
        frame["date"] = pd.to_datetime(frame["rcept_dt"], format="%Y%m%d", errors="coerce")

        rows: list[dict[str, object]] = []
        for _, row in frame.iterrows():
            event_category, event_score, event_risk_flag, event_reason = _classify_report(
                str(row["report_nm"])
            )
            rows.append(
                {
                    "date": row["date"].date() if pd.notna(row["date"]) else pd.NaT,
                    "as_of_date": row["date"].date() if pd.notna(row["date"]) else pd.NaT,
                    "corp_code": str(row["corp_code"]),
                    "ticker": str(row["ticker"]),
                    "report_nm": str(row["report_nm"]),
                    "rcept_no": str(row["rcept_no"]),
                    "event_category": event_category,
                    "event_score": event_score,
                    "event_risk_flag": event_risk_flag,
                    "event_reason": event_reason,
                    "source": str(row["source"]),
                    "feature_created_at": pd.Timestamp.now(tz="UTC"),
                }
            )

        event_frame = pd.DataFrame(rows, columns=DISCLOSURE_EVENT_COLUMNS)
        validate_disclosure_event_frame(event_frame)
        return event_frame


def _classify_report(report_name: str) -> tuple[str, float, bool, str]:
    normalized = report_name.lower()

    for keywords, category, reason, score in RISK_RULES:
        if any(keyword.lower() in normalized for keyword in keywords):
            return category, score, True, reason

    for keywords, category, reason, score in POSITIVE_RULES:
        if any(keyword.lower() in normalized for keyword in keywords):
            return category, score, False, reason

    if any(keyword.lower() in normalized for keyword in ROUTINE_KEYWORDS):
        return "routine_report", 50.0, False, "disclosure_routine_report"

    return "neutral_disclosure", 50.0, False, "disclosure_event_neutral"
