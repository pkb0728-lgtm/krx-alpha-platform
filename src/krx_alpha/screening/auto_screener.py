from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from krx_alpha.contracts.screening_contract import validate_screening_frame

SCREENING_COLUMNS = [
    "screen_date",
    "ticker",
    "passed",
    "screen_score",
    "final_action",
    "confidence_score",
    "market_regime",
    "risk_blocked",
    "suggested_position_pct",
    "trading_value",
    "trading_value_change_5d",
    "rsi_14",
    "volatility_5d",
    "financial_score",
    "event_score",
    "flow_score",
    "news_score",
    "macro_score",
    "reasons",
    "evidence_summary",
    "caution_summary",
    "review_checklist",
    "signal_path",
    "screened_at",
]


@dataclass(frozen=True)
class AutoScreenerConfig:
    min_confidence: float = 60.0
    min_screen_score: float = 60.0
    allowed_actions: tuple[str, ...] = ("buy_candidate", "watch")


class AutoScreener:
    """Build a human-review shortlist from latest universe signal artifacts."""

    def __init__(
        self,
        project_root: Path,
        config: AutoScreenerConfig | None = None,
    ) -> None:
        self.project_root = project_root
        self.config = config or AutoScreenerConfig()

    def screen(self, universe_summary: Any) -> Any:
        rows = []
        for _, summary_row in universe_summary.iterrows():
            if str(summary_row.get("status", "")) != "success":
                continue

            signal_path = Path(str(summary_row.get("signal_path", "")))
            if not signal_path.is_absolute():
                signal_path = self.project_root / signal_path
            if not signal_path.exists():
                rows.append(self._missing_signal_row(summary_row, signal_path))
                continue

            signal_frame = pd.read_parquet(signal_path)
            if signal_frame.empty:
                rows.append(self._missing_signal_row(summary_row, signal_path))
                continue

            latest_signal = _latest_by_date(signal_frame)
            latest_feature = self._load_latest_feature(signal_path)
            rows.append(self._screen_row(latest_signal, latest_feature, signal_path))

        result = pd.DataFrame(rows, columns=SCREENING_COLUMNS)
        if not result.empty:
            result = result.sort_values(
                ["passed", "screen_score", "confidence_score"],
                ascending=[False, False, False],
            ).reset_index(drop=True)
        validate_screening_frame(result)
        return result

    def _load_latest_feature(self, signal_path: Path) -> pd.Series | None:
        feature_path = self.project_root / "data" / "features" / "prices_daily" / signal_path.name
        if not feature_path.exists():
            return None

        feature_frame = pd.read_parquet(feature_path)
        if feature_frame.empty:
            return None
        return _latest_by_date(feature_frame)

    def _screen_row(
        self,
        signal_row: pd.Series,
        feature_row: pd.Series | None,
        signal_path: Path,
    ) -> dict[str, object]:
        reasons, raw_score = _score_screen_candidate(signal_row, feature_row)
        screen_score = float(np.clip(raw_score, 0, 100))
        final_action = str(signal_row["final_action"])
        confidence_score = float(signal_row["confidence_score"])
        risk_blocked = bool(signal_row["risk_blocked"])
        passed = (
            final_action in self.config.allowed_actions
            and confidence_score >= self.config.min_confidence
            and screen_score >= self.config.min_screen_score
            and not risk_blocked
        )

        return {
            "screen_date": _as_date_string(signal_row["date"]),
            "ticker": str(signal_row["ticker"]).zfill(6),
            "passed": passed,
            "screen_score": screen_score,
            "final_action": final_action,
            "confidence_score": confidence_score,
            "market_regime": str(signal_row.get("market_regime", "unknown")),
            "risk_blocked": risk_blocked,
            "suggested_position_pct": float(signal_row["suggested_position_pct"]),
            "trading_value": _feature_value(feature_row, "trading_value"),
            "trading_value_change_5d": _feature_value(feature_row, "trading_value_change_5d"),
            "rsi_14": _feature_value(feature_row, "rsi_14"),
            "volatility_5d": _feature_value(feature_row, "volatility_5d"),
            "financial_score": _optional_float(signal_row.get("financial_score")),
            "event_score": _optional_float(signal_row.get("event_score")),
            "flow_score": _optional_float(signal_row.get("flow_score")),
            "news_score": _optional_float(signal_row.get("news_score")),
            "macro_score": _optional_float(signal_row.get("macro_score")),
            "reasons": ", ".join(reasons),
            "evidence_summary": _evidence_summary(
                signal_row,
                feature_row,
                reasons,
                screen_score,
            ),
            "caution_summary": _caution_summary(signal_row, feature_row, reasons, passed),
            "review_checklist": _review_checklist(signal_row, feature_row, passed),
            "signal_path": str(signal_path),
            "screened_at": pd.Timestamp.now(tz="UTC"),
        }

    def _missing_signal_row(self, summary_row: pd.Series, signal_path: Path) -> dict[str, object]:
        return {
            "screen_date": "",
            "ticker": str(summary_row.get("ticker", "")).zfill(6),
            "passed": False,
            "screen_score": 0.0,
            "final_action": str(summary_row.get("latest_action", "")),
            "confidence_score": _optional_float(summary_row.get("latest_confidence_score")),
            "market_regime": str(summary_row.get("latest_market_regime", "unknown")),
            "risk_blocked": True,
            "suggested_position_pct": 0.0,
            "trading_value": np.nan,
            "trading_value_change_5d": np.nan,
            "rsi_14": np.nan,
            "volatility_5d": np.nan,
            "financial_score": _optional_float(summary_row.get("latest_financial_score")),
            "event_score": _optional_float(summary_row.get("latest_event_score")),
            "flow_score": _optional_float(summary_row.get("latest_flow_score")),
            "news_score": _optional_float(summary_row.get("latest_news_score")),
            "macro_score": _optional_float(summary_row.get("latest_macro_score")),
            "reasons": "signal_file_missing_or_empty",
            "evidence_summary": "No signal file was available for review.",
            "caution_summary": "Do not review this ticker until the final signal artifact exists.",
            "review_checklist": "rerun_pipeline, confirm_signal_artifact",
            "signal_path": str(signal_path),
            "screened_at": pd.Timestamp.now(tz="UTC"),
        }


def format_screening_report(result_frame: Any, title: str = "Auto Screener Report") -> str:
    passed = result_frame[result_frame["passed"]] if not result_frame.empty else result_frame
    lines = [
        f"# {title}",
        "",
        f"- Checked tickers: {len(result_frame)}",
        f"- Passed: {len(passed)}",
        f"- Review cards: {min(len(passed), 10)}",
        "",
        "## Candidate Review Cards",
        "",
    ]
    if passed.empty:
        lines.append("- No candidates passed the screen.")
    else:
        for rank, (_, row) in enumerate(passed.head(10).iterrows(), start=1):
            lines.extend(_candidate_card_lines(rank, row))

    lines.extend(
        [
            "",
            "## Candidates",
            "",
            "| Ticker | Action | Score | Confidence | Position | Reasons |",
            "| --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    if passed.empty:
        lines.append("| N/A | N/A | 0.00 | 0.00 | 0.00% | no_candidates_passed |")
    else:
        for _, row in passed.head(20).iterrows():
            lines.append(
                "| "
                f"{row['ticker']} | "
                f"{row['final_action']} | "
                f"{float(row['screen_score']):.2f} | "
                f"{float(row['confidence_score']):.2f} | "
                f"{float(row['suggested_position_pct']):.2f}% | "
                f"{row['reasons']} |"
            )

    lines.extend(
        [
            "",
            "## Reading Guide",
            "",
            "- This screener is a shortlist for human review, not an order instruction.",
            "- Candidates must pass action, confidence, score, and risk-block filters.",
            "- Review liquidity, disclosure events, news, and market regime before action.",
            "",
        ]
    )
    return "\n".join(lines)


def _candidate_card_lines(rank: int, row: pd.Series) -> list[str]:
    return [
        f"### {rank}. {row['ticker']} - {row['final_action']}",
        "",
        f"- Screen score: {float(row['screen_score']):.2f}",
        f"- Confidence: {float(row['confidence_score']):.2f}",
        f"- Suggested position: {float(row['suggested_position_pct']):.2f}%",
        f"- Evidence: {row['evidence_summary']}",
        f"- Caution: {row['caution_summary']}",
        f"- Review checklist: {row['review_checklist']}",
        "",
    ]


def _score_screen_candidate(
    signal_row: pd.Series,
    feature_row: pd.Series | None,
) -> tuple[list[str], float]:
    reasons: list[str] = []
    score = 0.0

    final_action = str(signal_row["final_action"])
    if final_action == "buy_candidate":
        score += 30
        reasons.append("buy_candidate_signal")
    elif final_action == "watch":
        score += 15
        reasons.append("watch_signal")
    else:
        reasons.append(f"action_{final_action}")

    confidence = float(signal_row["confidence_score"])
    score += min(confidence * 0.35, 35)
    if confidence >= 70:
        reasons.append("high_confidence")
    elif confidence >= 60:
        reasons.append("moderate_confidence")

    if bool(signal_row["risk_blocked"]):
        score -= 50
        reasons.append("risk_blocked")
    else:
        score += 10
        reasons.append("risk_filter_passed")

    _add_score_reason(
        reasons,
        "financial_score_supportive",
        _optional_float(signal_row.get("financial_score")) >= 60,
    )
    _add_score_reason(
        reasons,
        "flow_score_supportive",
        _optional_float(signal_row.get("flow_score")) >= 60,
    )
    _add_score_reason(
        reasons,
        "news_score_supportive",
        _optional_float(signal_row.get("news_score")) >= 60,
    )

    regime = str(signal_row.get("market_regime", "unknown"))
    if regime in {"bull", "uptrend", "bullish"}:
        score += 5
        reasons.append("market_regime_supportive")
    elif regime in {"bear", "downtrend", "high_volatility"}:
        score -= 8
        reasons.append("market_regime_caution")

    trading_value_change = _feature_value(feature_row, "trading_value_change_5d")
    if pd.notna(trading_value_change):
        if float(trading_value_change) >= 0.10:
            score += 8
            reasons.append("trading_value_surge")
        elif float(trading_value_change) >= 0.03:
            score += 4
            reasons.append("trading_value_increase")

    rsi_14 = _feature_value(feature_row, "rsi_14")
    if pd.notna(rsi_14):
        rsi_value = float(rsi_14)
        if 35 <= rsi_value <= 60:
            score += 7
            reasons.append("rsi_recovery_zone")
        elif rsi_value > 75:
            score -= 7
            reasons.append("rsi_overheated")
        elif rsi_value < 30:
            score -= 5
            reasons.append("rsi_weak")

    return reasons, score


def _latest_by_date(frame: pd.DataFrame) -> pd.Series:
    current = frame.copy()
    current["date"] = pd.to_datetime(current["date"], errors="coerce")
    return current.sort_values("date").iloc[-1]


def _feature_value(feature_row: pd.Series | None, column: str) -> float:
    if feature_row is None or column not in feature_row.index:
        return float("nan")
    return _optional_float(feature_row[column])


def _optional_float(value: object) -> float:
    if value is None or pd.isna(value):
        return float("nan")
    return float(cast(Any, value))


def _as_date_string(value: object) -> str:
    return cast(str, pd.Timestamp(value).date().isoformat())


def _add_score_reason(reasons: list[str], reason: str, condition: bool) -> None:
    if condition:
        reasons.append(reason)


def _evidence_summary(
    signal_row: pd.Series,
    feature_row: pd.Series | None,
    reasons: list[str],
    screen_score: float,
) -> str:
    evidence = [
        f"{signal_row['final_action']} action with screen score {screen_score:.2f}",
        f"confidence {float(signal_row['confidence_score']):.2f}",
    ]
    if "risk_filter_passed" in reasons:
        evidence.append("risk filter passed")
    if "trading_value_surge" in reasons:
        evidence.append("trading value surged over the 5-day baseline")
    elif "trading_value_increase" in reasons:
        evidence.append("trading value increased over the 5-day baseline")
    if "rsi_recovery_zone" in reasons:
        evidence.append("RSI is in a recovery-friendly range")
    if _optional_float(signal_row.get("financial_score")) >= 60:
        evidence.append("financial score is supportive")
    if _optional_float(signal_row.get("flow_score")) >= 60:
        evidence.append("investor flow score is supportive")
    if _optional_float(signal_row.get("news_score")) >= 60:
        evidence.append("news sentiment score is supportive")

    trading_value = _feature_value(feature_row, "trading_value")
    if pd.notna(trading_value):
        evidence.append(f"latest trading value {_format_large_number(trading_value)}")
    return "; ".join(evidence)


def _caution_summary(
    signal_row: pd.Series,
    feature_row: pd.Series | None,
    reasons: list[str],
    passed: bool,
) -> str:
    cautions: list[str] = []
    if not passed:
        cautions.append("candidate did not pass all screen thresholds")
    if bool(signal_row["risk_blocked"]):
        cautions.append("risk filter blocked the signal")
    if "market_regime_caution" in reasons:
        cautions.append("market regime is not supportive")
    if str(signal_row.get("market_regime", "")) in {"unknown", "insufficient_data"}:
        cautions.append("market regime needs more data")
    if _optional_float(signal_row.get("financial_score")) < 50:
        cautions.append("financial score is weak")
    if _optional_float(signal_row.get("event_score")) < 50:
        cautions.append("disclosure event score is weak")
    if _optional_float(signal_row.get("news_score")) < 50:
        cautions.append("news sentiment is weak or missing")
    if _feature_value(feature_row, "volatility_5d") > 0.05:
        cautions.append("short-term volatility is elevated")
    if "rsi_overheated" in reasons:
        cautions.append("RSI looks overheated")
    if not cautions:
        cautions.append("no hard block, but confirm latest news and disclosures")
    return "; ".join(cautions)


def _review_checklist(
    signal_row: pd.Series,
    feature_row: pd.Series | None,
    passed: bool,
) -> str:
    checklist = [
        "confirm_recent_news",
        "check_dart_disclosures",
        "verify_liquidity",
        "review_position_size",
    ]
    if not passed:
        checklist.insert(0, "identify_failed_threshold")
    if str(signal_row.get("market_regime", "")) in {"unknown", "insufficient_data"}:
        checklist.append("extend_price_history_for_regime")
    if pd.isna(_feature_value(feature_row, "trading_value")):
        checklist.append("rebuild_price_features")
    return ", ".join(checklist)


def _format_large_number(value: float) -> str:
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    return f"{value:.2f}"
