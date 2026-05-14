from dataclasses import dataclass
from typing import Any

import pandas as pd

from krx_alpha.contracts.price_contract import REQUIRED_PRICE_COLUMNS

QUALITY_STATUS_PASS = "PASS"
QUALITY_STATUS_WARN = "WARN"
QUALITY_STATUS_FAIL = "FAIL"

QUALITY_CHECK_COLUMNS = [
    "dataset",
    "check_name",
    "status",
    "severity",
    "affected_rows",
    "affected_pct",
    "detail",
    "action",
]


@dataclass(frozen=True)
class PriceQualityConfig:
    max_abs_return: float = 0.30
    max_calendar_gap_days: int = 7


class PriceDataQualityChecker:
    """Run practical quality checks for daily OHLCV price datasets."""

    def __init__(self, config: PriceQualityConfig | None = None) -> None:
        self.config = config or PriceQualityConfig()

    def check(self, frame: Any, dataset: str = "price") -> pd.DataFrame:
        checks = [
            self._required_columns_check(frame, dataset),
            self._non_empty_check(frame, dataset),
        ]
        if frame.empty:
            return _quality_frame(checks)

        checks.extend(
            [
                self._null_required_values_check(frame, dataset),
                self._duplicate_date_ticker_check(frame, dataset),
                self._non_negative_market_values_check(frame, dataset),
                self._ohlc_integrity_check(frame, dataset),
                self._calendar_gap_check(frame, dataset),
                self._suspicious_return_check(frame, dataset),
            ]
        )
        return _quality_frame(checks)

    def _required_columns_check(self, frame: pd.DataFrame, dataset: str) -> dict[str, object]:
        missing_columns = sorted(REQUIRED_PRICE_COLUMNS - set(frame.columns))
        if not missing_columns:
            return _quality_row(
                dataset=dataset,
                check_name="required_columns",
                status=QUALITY_STATUS_PASS,
                affected_rows=0,
                total_rows=len(frame),
                detail="all required price columns are present",
                action="none",
            )
        return _quality_row(
            dataset=dataset,
            check_name="required_columns",
            status=QUALITY_STATUS_FAIL,
            affected_rows=len(missing_columns),
            total_rows=max(len(REQUIRED_PRICE_COLUMNS), 1),
            detail=f"missing columns: {', '.join(missing_columns)}",
            action="fix collector or processor schema before using this dataset",
        )

    def _non_empty_check(self, frame: pd.DataFrame, dataset: str) -> dict[str, object]:
        if not frame.empty:
            return _quality_row(
                dataset=dataset,
                check_name="non_empty",
                status=QUALITY_STATUS_PASS,
                affected_rows=0,
                total_rows=len(frame),
                detail=f"dataset has {len(frame)} row(s)",
                action="none",
            )
        return _quality_row(
            dataset=dataset,
            check_name="non_empty",
            status=QUALITY_STATUS_FAIL,
            affected_rows=1,
            total_rows=1,
            detail="dataset is empty",
            action="rerun upstream collection or processing step",
        )

    def _null_required_values_check(self, frame: pd.DataFrame, dataset: str) -> dict[str, object]:
        columns = [column for column in REQUIRED_PRICE_COLUMNS if column in frame.columns]
        if not columns:
            return _skipped_row(dataset, "null_required_values", "required columns are missing")

        affected = int(frame[columns].isna().any(axis=1).sum())
        return _quality_row(
            dataset=dataset,
            check_name="null_required_values",
            status=QUALITY_STATUS_FAIL if affected else QUALITY_STATUS_PASS,
            affected_rows=affected,
            total_rows=len(frame),
            detail=f"{affected} row(s) contain null required price values",
            action="inspect source rows and rebuild processed data" if affected else "none",
        )

    def _duplicate_date_ticker_check(self, frame: pd.DataFrame, dataset: str) -> dict[str, object]:
        if not {"date", "ticker"}.issubset(frame.columns):
            return _skipped_row(
                dataset,
                "duplicate_date_ticker",
                "date or ticker column is missing",
            )

        affected = int(frame.duplicated(subset=["date", "ticker"], keep=False).sum())
        return _quality_row(
            dataset=dataset,
            check_name="duplicate_date_ticker",
            status=QUALITY_STATUS_FAIL if affected else QUALITY_STATUS_PASS,
            affected_rows=affected,
            total_rows=len(frame),
            detail=f"{affected} duplicate date/ticker row(s)",
            action=(
                "deduplicate by date and ticker before feature generation" if affected else "none"
            ),
        )

    def _non_negative_market_values_check(
        self,
        frame: pd.DataFrame,
        dataset: str,
    ) -> dict[str, object]:
        columns = [
            column
            for column in ["open", "high", "low", "close", "volume", "trading_value"]
            if column in frame.columns
        ]
        if not columns:
            return _skipped_row(dataset, "non_negative_market_values", "market columns are missing")

        numeric = frame[columns].apply(pd.to_numeric, errors="coerce")
        affected = int((numeric < 0).any(axis=1).sum())
        return _quality_row(
            dataset=dataset,
            check_name="non_negative_market_values",
            status=QUALITY_STATUS_FAIL if affected else QUALITY_STATUS_PASS,
            affected_rows=affected,
            total_rows=len(frame),
            detail=f"{affected} row(s) contain negative market values",
            action="fix negative price, volume, or trading value rows" if affected else "none",
        )

    def _ohlc_integrity_check(self, frame: pd.DataFrame, dataset: str) -> dict[str, object]:
        required = {"open", "high", "low", "close"}
        if not required.issubset(frame.columns):
            return _skipped_row(dataset, "ohlc_integrity", "one or more OHLC columns are missing")

        ohlc = frame[list(required)].apply(pd.to_numeric, errors="coerce")
        invalid = (
            (ohlc["high"] < ohlc["low"])
            | (ohlc["open"] > ohlc["high"])
            | (ohlc["open"] < ohlc["low"])
            | (ohlc["close"] > ohlc["high"])
            | (ohlc["close"] < ohlc["low"])
        )
        affected = int(invalid.sum())
        return _quality_row(
            dataset=dataset,
            check_name="ohlc_integrity",
            status=QUALITY_STATUS_FAIL if affected else QUALITY_STATUS_PASS,
            affected_rows=affected,
            total_rows=len(frame),
            detail=f"{affected} row(s) violate OHLC high/low bounds",
            action="verify split-adjustment and raw OHLC source rows" if affected else "none",
        )

    def _calendar_gap_check(self, frame: pd.DataFrame, dataset: str) -> dict[str, object]:
        if not {"date", "ticker"}.issubset(frame.columns):
            return _skipped_row(dataset, "calendar_gap", "date or ticker column is missing")

        current = frame[["date", "ticker"]].copy()
        current["date"] = pd.to_datetime(current["date"], errors="coerce")
        current = current.dropna(subset=["date"]).sort_values(["ticker", "date"])
        gaps = current.groupby("ticker")["date"].diff().dt.days
        affected = int((gaps > self.config.max_calendar_gap_days).sum())
        status = QUALITY_STATUS_WARN if affected else QUALITY_STATUS_PASS
        return _quality_row(
            dataset=dataset,
            check_name="calendar_gap",
            status=status,
            affected_rows=affected,
            total_rows=len(frame),
            detail=(f"{affected} gap(s) exceed {self.config.max_calendar_gap_days} calendar days"),
            action=(
                "review holidays, suspension, or missing collection dates" if affected else "none"
            ),
        )

    def _suspicious_return_check(self, frame: pd.DataFrame, dataset: str) -> dict[str, object]:
        if not {"date", "ticker", "close"}.issubset(frame.columns):
            return _skipped_row(
                dataset,
                "suspicious_return",
                "date, ticker, or close column is missing",
            )

        current = frame[["date", "ticker", "close"]].copy()
        current["date"] = pd.to_datetime(current["date"], errors="coerce")
        current["close"] = pd.to_numeric(current["close"], errors="coerce")
        current = current.dropna(subset=["date", "close"]).sort_values(["ticker", "date"])
        returns = current.groupby("ticker")["close"].pct_change()
        affected = int((returns.abs() > self.config.max_abs_return).sum())
        status = QUALITY_STATUS_WARN if affected else QUALITY_STATUS_PASS
        return _quality_row(
            dataset=dataset,
            check_name="suspicious_return",
            status=status,
            affected_rows=affected,
            total_rows=len(frame),
            detail=f"{affected} return(s) exceed {self.config.max_abs_return:.0%}",
            action=(
                "verify corporate action adjustment or abnormal event rows" if affected else "none"
            ),
        )


def summarize_quality(result_frame: Any) -> dict[str, int]:
    if result_frame.empty:
        return {"total": 0, "pass": 0, "warn": 0, "fail": 0}
    statuses = result_frame["status"].astype(str)
    return {
        "total": int(len(result_frame)),
        "pass": int((statuses == QUALITY_STATUS_PASS).sum()),
        "warn": int((statuses == QUALITY_STATUS_WARN).sum()),
        "fail": int((statuses == QUALITY_STATUS_FAIL).sum()),
    }


def format_data_quality_report(result_frame: Any, title: str = "Data Quality Report") -> str:
    summary = summarize_quality(result_frame)
    lines = [
        f"# {title}",
        "",
        f"- Total checks: {summary['total']}",
        f"- Passed: {summary['pass']}",
        f"- Warnings: {summary['warn']}",
        f"- Failed: {summary['fail']}",
        "",
        "## Checks",
        "",
        "| Dataset | Check | Status | Affected Rows | Affected % | Detail | Action |",
        "| --- | --- | --- | ---: | ---: | --- | --- |",
    ]
    for _, row in result_frame.iterrows():
        lines.append(
            "| "
            f"{row['dataset']} | "
            f"{row['check_name']} | "
            f"{row['status']} | "
            f"{int(row['affected_rows'])} | "
            f"{float(row['affected_pct']):.2%} | "
            f"{_escape_table(row['detail'])} | "
            f"{_escape_table(row['action'])} |"
        )

    lines.extend(
        [
            "",
            "## Reading Guide",
            "",
            "- `FAIL` should be fixed before using the dataset for features or backtests.",
            "- `WARN` should be reviewed because it may be a market event or collection gap.",
            "- `PASS` means the specific check did not find an issue.",
            "",
        ]
    )
    return "\n".join(lines)


def _quality_frame(rows: list[dict[str, object]]) -> pd.DataFrame:
    frame = pd.DataFrame(rows, columns=QUALITY_CHECK_COLUMNS)
    if frame.empty:
        return frame
    return frame.sort_values(["severity", "check_name"], ascending=[False, True]).reset_index(
        drop=True
    )


def _quality_row(
    *,
    dataset: str,
    check_name: str,
    status: str,
    affected_rows: int,
    total_rows: int,
    detail: str,
    action: str,
) -> dict[str, object]:
    denominator = max(total_rows, 1)
    return {
        "dataset": dataset,
        "check_name": check_name,
        "status": status,
        "severity": _quality_severity(status),
        "affected_rows": int(affected_rows),
        "affected_pct": float(affected_rows / denominator),
        "detail": detail,
        "action": action,
    }


def _skipped_row(dataset: str, check_name: str, detail: str) -> dict[str, object]:
    return _quality_row(
        dataset=dataset,
        check_name=check_name,
        status=QUALITY_STATUS_WARN,
        affected_rows=0,
        total_rows=1,
        detail=f"check skipped: {detail}",
        action="fix required columns first",
    )


def _quality_severity(status: str) -> int:
    if status == QUALITY_STATUS_FAIL:
        return 2
    if status == QUALITY_STATUS_WARN:
        return 1
    return 0


def _escape_table(value: object) -> str:
    return str(value).replace("|", "/")
