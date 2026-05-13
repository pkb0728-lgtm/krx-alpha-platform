from typing import Any

import pandas as pd

from krx_alpha.contracts.dart_contract import validate_dart_financial_frame
from krx_alpha.contracts.financial_feature_contract import validate_financial_feature_frame

FINANCIAL_FEATURE_COLUMNS = [
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
]

AccountRule = dict[str, tuple[str, ...]]

ACCOUNT_RULES: dict[str, AccountRule] = {
    "revenue": {
        "ids": ("Revenue",),
        "names": ("매출액", "수익", "영업수익"),
    },
    "operating_income": {
        "ids": ("OperatingIncomeLoss",),
        "names": ("영업이익", "영업손실"),
    },
    "net_income": {
        "ids": ("ProfitLoss",),
        "names": ("당기순이익", "당기순손실", "분기순이익"),
    },
    "total_assets": {
        "ids": ("Assets",),
        "names": ("자산총계",),
    },
    "total_liabilities": {
        "ids": ("Liabilities",),
        "names": ("부채총계",),
    },
    "total_equity": {
        "ids": ("Equity",),
        "names": ("자본총계",),
    },
}


class DartFinancialFeatureBuilder:
    """Build reusable financial features from normalized OpenDART statements."""

    def build(self, financial_frame: Any) -> Any:
        validate_dart_financial_frame(financial_frame)
        frame = financial_frame.copy()
        frame["corp_code"] = frame["corp_code"].astype(str).str.zfill(8)
        frame["ticker"] = frame["ticker"].astype(str).str.zfill(6)
        frame["bsns_year"] = frame["bsns_year"].astype(str)
        frame["reprt_code"] = frame["reprt_code"].astype(str)
        frame["fs_div"] = frame["fs_div"].astype(str)

        rows: list[dict[str, object]] = []
        group_columns = ["corp_code", "ticker", "bsns_year", "reprt_code", "fs_div"]
        for keys, group in frame.groupby(group_columns, dropna=False):
            corp_code, ticker, bsns_year, reprt_code, fs_div = keys
            metrics = {
                feature_name: _find_account_value(group, rule)
                for feature_name, rule in ACCOUNT_RULES.items()
            }
            revenue = metrics["revenue"]
            operating_income = metrics["operating_income"]
            net_income = metrics["net_income"]
            total_assets = metrics["total_assets"]
            total_liabilities = metrics["total_liabilities"]
            total_equity = metrics["total_equity"]
            previous_revenue = _find_previous_account_value(group, ACCOUNT_RULES["revenue"])

            revenue_growth = _safe_ratio_delta(revenue, previous_revenue)
            operating_margin = _safe_divide(operating_income, revenue)
            net_margin = _safe_divide(net_income, revenue)
            debt_ratio = _safe_divide(total_liabilities, total_equity)
            roe = _safe_divide(net_income, total_equity)
            financial_score, financial_reason = _score_financials(
                revenue_growth=revenue_growth,
                operating_margin=operating_margin,
                net_margin=net_margin,
                debt_ratio=debt_ratio,
                roe=roe,
            )

            rows.append(
                {
                    "corp_code": str(corp_code),
                    "ticker": str(ticker),
                    "bsns_year": str(bsns_year),
                    "reprt_code": str(reprt_code),
                    "fs_div": str(fs_div),
                    "revenue": revenue,
                    "operating_income": operating_income,
                    "net_income": net_income,
                    "total_assets": total_assets,
                    "total_liabilities": total_liabilities,
                    "total_equity": total_equity,
                    "revenue_growth": revenue_growth,
                    "operating_margin": operating_margin,
                    "net_margin": net_margin,
                    "debt_ratio": debt_ratio,
                    "roe": roe,
                    "financial_score": financial_score,
                    "financial_reason": financial_reason,
                    "source": str(group["source"].iloc[0]),
                    "feature_created_at": pd.Timestamp.now(tz="UTC"),
                }
            )

        feature_frame = pd.DataFrame(rows, columns=FINANCIAL_FEATURE_COLUMNS)
        validate_financial_feature_frame(feature_frame)
        return feature_frame


def _find_account_value(group: pd.DataFrame, rule: AccountRule) -> float | None:
    row = _find_account_row(group, rule)
    if row is None:
        return None
    value = row.get("thstrm_amount_value")
    return None if pd.isna(value) else float(value)


def _find_previous_account_value(
    group: pd.DataFrame,
    rule: AccountRule,
) -> float | None:
    row = _find_account_row(group, rule)
    if row is None:
        return None
    value = row.get("frmtrm_amount_value")
    return None if pd.isna(value) else float(value)


def _find_account_row(group: pd.DataFrame, rule: AccountRule) -> pd.Series | None:
    account_ids = group.get("account_id", pd.Series([""] * len(group))).astype(str)
    account_names = group["account_nm"].astype(str)

    for account_id_keyword in rule["ids"]:
        matched = group[account_ids.str.contains(account_id_keyword, case=False, regex=False)]
        if not matched.empty:
            return matched.iloc[0]

    for account_name_keyword in rule["names"]:
        matched = group[account_names.str.contains(account_name_keyword, case=False, regex=False)]
        if not matched.empty:
            return matched.iloc[0]

    return None


def _safe_ratio_delta(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None or previous == 0:
        return None
    return current / previous - 1


def _safe_divide(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _score_financials(
    revenue_growth: float | None,
    operating_margin: float | None,
    net_margin: float | None,
    debt_ratio: float | None,
    roe: float | None,
) -> tuple[float, str]:
    score = 50.0
    reasons: list[str] = []

    if revenue_growth is not None and revenue_growth > 0.1:
        score += 15
        reasons.append("revenue_growth_positive")
    elif revenue_growth is not None and revenue_growth < -0.05:
        score -= 10
        reasons.append("revenue_growth_negative")

    if operating_margin is not None and operating_margin > 0.1:
        score += 15
        reasons.append("operating_margin_healthy")
    elif operating_margin is not None and operating_margin < 0:
        score -= 15
        reasons.append("operating_loss")

    if net_margin is not None and net_margin > 0.05:
        score += 10
        reasons.append("net_margin_positive")

    if debt_ratio is not None and debt_ratio < 1.0:
        score += 10
        reasons.append("debt_ratio_conservative")
    elif debt_ratio is not None and debt_ratio > 2.0:
        score -= 15
        reasons.append("debt_ratio_high")

    if roe is not None and roe > 0.08:
        score += 10
        reasons.append("roe_positive")

    if not reasons:
        reasons.append("financial_evidence_neutral")

    return float(min(100, max(0, score))), ", ".join(reasons)
