import ssl
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from time import sleep
from typing import Any
from urllib import parse, request

import pandas as pd

TELEGRAM_MESSAGE_LIMIT = 4096

TelegramTransport = Callable[[request.Request, float], Any]
TelegramSleeper = Callable[[float], None]


@dataclass(frozen=True)
class TelegramSendResult:
    sent: bool
    dry_run: bool
    status_code: int | None
    message: str
    response_text: str = ""


class TelegramNotifier:
    """Send plain-text alerts through the Telegram Bot API."""

    def __init__(
        self,
        bot_token: str | None,
        chat_id: str | None,
        timeout_seconds: float = 10.0,
        max_retries: int = 2,
        retry_sleep_seconds: float = 1.0,
        transport: TelegramTransport | None = None,
        sleeper: TelegramSleeper | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be zero or positive.")
        if retry_sleep_seconds < 0:
            raise ValueError("retry_sleep_seconds must be zero or positive.")

        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_sleep_seconds = retry_sleep_seconds
        self._transport = transport or _default_transport
        self._sleeper = sleeper or sleep

    def send_message(self, message: str, dry_run: bool = False) -> TelegramSendResult:
        text = _truncate_message(message)
        if dry_run:
            return TelegramSendResult(
                sent=False,
                dry_run=True,
                status_code=None,
                message=text,
            )

        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set.")

        last_error: Exception | None = None
        for attempt_index in range(self.max_retries + 1):
            try:
                result = self._send_once(text)
            except Exception as exc:
                last_error = exc
                if not self._can_retry(attempt_index):
                    raise RuntimeError(
                        f"Telegram message send failed after {attempt_index + 1} attempt(s)."
                    ) from exc
                self._sleep_before_retry()
                continue

            if result.sent:
                return result

            status_code = result.status_code or 0
            last_error = RuntimeError(f"Telegram API returned status {status_code}.")
            if not _is_retryable_status(status_code) or not self._can_retry(attempt_index):
                raise last_error
            self._sleep_before_retry()

        raise RuntimeError("Telegram message send failed.") from last_error

    def _send_once(self, text: str) -> TelegramSendResult:
        if not self.bot_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set.")

        payload = parse.urlencode(
            {
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": "true",
            }
        ).encode("utf-8")
        telegram_request = request.Request(
            _telegram_send_message_url(self.bot_token),
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )

        with self._transport(telegram_request, self.timeout_seconds) as response:
            response_text = response.read().decode("utf-8", errors="replace")
            status_code = _response_status_code(response)

        return TelegramSendResult(
            sent=200 <= status_code < 300,
            dry_run=False,
            status_code=status_code,
            message=text,
            response_text=response_text,
        )

    def _can_retry(self, attempt_index: int) -> bool:
        return attempt_index < self.max_retries

    def _sleep_before_retry(self) -> None:
        if self.retry_sleep_seconds > 0:
            self._sleeper(self.retry_sleep_seconds)


def build_daily_telegram_message(
    universe_summary: Any,
    screening_result: Any | None = None,
    paper_portfolio_summary: Any | None = None,
    backtest_metrics: Any | None = None,
    walk_forward_summary: Any | None = None,
    drift_result: Any | None = None,
    operations_health: Any | None = None,
    generated_at: datetime | None = None,
    top_n: int = 5,
) -> str:
    """Build a compact daily operations brief for Telegram."""
    if universe_summary.empty:
        raise ValueError("Universe summary frame is empty.")

    generated = generated_at or datetime.now()
    frame = universe_summary.copy()
    success_frame = frame[frame["status"] == "success"].sort_values(
        "latest_confidence_score",
        ascending=False,
    )
    failed_frame = frame[frame["status"] == "failed"]

    lines = [
        "KRX Alpha Daily Brief",
        f"Generated: {generated.strftime('%Y-%m-%d %H:%M')}",
        "",
        "Universe",
        f"- Tickers: {len(frame)}",
        f"- Success: {len(success_frame)}",
        f"- Failed: {len(failed_frame)}",
        "",
        "Top candidates",
    ]
    lines.extend(_format_candidate_lines(success_frame, top_n))
    lines.extend(_format_screening_lines(screening_result, top_n))
    lines.extend(_format_paper_portfolio_lines(paper_portfolio_summary))
    lines.extend(_format_backtest_lines(backtest_metrics))
    lines.extend(_format_walk_forward_lines(walk_forward_summary))
    lines.extend(_format_drift_lines(drift_result))
    lines.extend(_format_operations_health_lines(operations_health))
    lines.extend(
        [
            "",
            "Risk note",
            "- Screening aid only. Not investment advice.",
            "- Review liquidity, disclosures, news, and market regime before action.",
        ]
    )
    return _truncate_message("\n".join(lines))


def _format_candidate_lines(frame: pd.DataFrame, top_n: int) -> list[str]:
    if frame.empty:
        return ["- No successful ticker results."]

    lines: list[str] = []
    limited_frame = frame.head(max(top_n, 1))
    for rank, (_, row) in enumerate(limited_frame.iterrows(), start=1):
        lines.append(
            f"{rank}. {row['ticker']} | {row['latest_action']} | "
            f"score {_format_number(row['latest_confidence_score'])} | "
            f"regime {_format_optional(row, 'latest_market_regime')} | "
            f"F {_format_number(_row_value(row, 'latest_financial_score'))} / "
            f"E {_format_number(_row_value(row, 'latest_event_score'))} / "
            f"Flow {_format_number(_row_value(row, 'latest_flow_score'))} / "
            f"News {_format_number(_row_value(row, 'latest_news_score'))} / "
            f"Macro {_format_number(_row_value(row, 'latest_macro_score'))}"
        )
    return lines


def _format_screening_lines(result: Any | None, top_n: int) -> list[str]:
    if result is None or result.empty:
        return ["", "Auto screener", "- No latest screening result."]

    frame = result.copy()
    passed_frame = frame[frame["passed"]] if "passed" in frame.columns else frame.iloc[0:0]
    lines = [
        "",
        "Auto screener",
        f"- Checked {len(frame)} | passed {len(passed_frame)}",
    ]
    status_summary = _screen_status_summary(frame)
    if status_summary:
        lines.append(f"- Status reasons: {status_summary}")
    if passed_frame.empty:
        lines.append("- No candidates passed the screen.")
        return lines

    ranked = passed_frame.sort_values(
        ["screen_score", "confidence_score"],
        ascending=[False, False],
    ).head(max(top_n, 1))
    for rank, (_, row) in enumerate(ranked.iterrows(), start=1):
        lines.append(
            f"{rank}. {row['ticker']} | {row['final_action']} | "
            f"priority {_format_optional(row, 'review_priority')} | "
            f"screen {_format_number(row['screen_score'])} | "
            f"confidence {_format_number(row['confidence_score'])} | "
            f"position {_format_plain_percent(row['suggested_position_pct'])}"
        )
        evidence = _row_value(row, "evidence_summary")
        caution = _row_value(row, "caution_summary")
        if not _is_missing(evidence) and str(evidence):
            lines.append(f"   evidence: {_truncate_line(str(evidence), limit=120)}")
        if not _is_missing(caution) and str(caution):
            lines.append(f"   caution: {_truncate_line(str(caution), limit=120)}")
    return lines


def _screen_status_summary(frame: pd.DataFrame) -> str:
    if "screen_status_reason" not in frame.columns:
        return ""

    counts = frame["screen_status_reason"].fillna("unknown").astype(str).value_counts()
    return ", ".join(f"{reason} {count}" for reason, count in counts.head(3).items())


def _format_paper_portfolio_lines(summary: Any | None) -> list[str]:
    if summary is None or summary.empty:
        return ["", "Paper portfolio", "- No latest paper portfolio summary."]

    metric = summary.sort_values("generated_at", ascending=False).iloc[0]
    lines = [
        "",
        "Paper portfolio",
        (
            f"- {metric['universe']} | tickers "
            f"{int(metric['loaded_ticker_count'])}/{int(metric['requested_ticker_count'])} | "
            f"trades {int(metric['trade_count'])} | "
            f"return {_format_percent(metric['cumulative_return'])} | "
            f"exposure {_format_plain_percent(metric['gross_exposure_pct'])} | "
            f"cash {_format_plain_percent(metric['cash_pct'])}"
        ),
    ]
    skipped_tickers = _row_value(metric, "skipped_tickers")
    if not _is_missing(skipped_tickers) and str(skipped_tickers):
        lines.append(f"- skipped: {skipped_tickers}")
    return lines


def _format_backtest_lines(metrics: Any | None) -> list[str]:
    if metrics is None or metrics.empty:
        return ["", "Backtest", "- No latest backtest metrics."]

    metric = metrics.sort_values("cumulative_return", ascending=False).iloc[0]
    return [
        "",
        "Backtest",
        (
            f"- {metric['ticker']} | trades {int(metric['trade_count'])} | "
            f"win {_format_percent(metric['win_rate'])} | "
            f"return {_format_percent(metric['cumulative_return'])} | "
            f"MDD {_format_percent(metric['max_drawdown'])} | "
            f"Sharpe {float(metric['sharpe_ratio']):.2f}"
        ),
    ]


def _format_walk_forward_lines(summary: Any | None) -> list[str]:
    if summary is None or summary.empty:
        return ["", "Walk-forward", "- No latest walk-forward summary."]

    metric = summary.sort_values("compounded_return", ascending=False).iloc[0]
    return [
        "",
        "Walk-forward",
        (
            f"- {metric['ticker']} | folds {int(metric['fold_count'])} | "
            f"trades {int(metric['total_trade_count'])} | "
            f"compounded {_format_percent(metric['compounded_return'])} | "
            f"worst MDD {_format_percent(metric['worst_max_drawdown'])} | "
            f"positive folds {_format_percent(metric['positive_fold_ratio'])}"
        ),
    ]


def _format_drift_lines(result: Any | None) -> list[str]:
    if result is None or result.empty or "drift_detected" not in result.columns:
        return ["", "Drift", "- No latest drift result."]

    drift_count = int(result["drift_detected"].sum())
    if "feature" in result.columns:
        drifted = result[result["drift_detected"]].head(3)
        lines = [
            "",
            "Drift",
            f"- Data drift: {drift_count}/{len(result)} features flagged.",
        ]
        if drifted.empty:
            lines.append("- Status: stable")
        else:
            for _, row in drifted.iterrows():
                lines.append(f"- {row['feature']}: {row['drift_reason']}")
        return lines

    row = result.iloc[0]
    return [
        "",
        "Drift",
        (
            f"- Performance drift: {row['metric']} | "
            f"detected {bool(row['drift_detected'])} | "
            f"reason {row['drift_reason']}"
        ),
    ]


def _format_operations_health_lines(result: Any | None) -> list[str]:
    if result is None or result.empty or "status" not in result.columns:
        return ["", "Operations health", "- No latest operations health result."]

    status_values = result["status"].astype(str)
    ok_count = int((status_values == "OK").sum())
    warning_count = int(status_values.isin(["WARN", "STALE"]).sum())
    problem_count = int(status_values.isin(["MISSING", "EMPTY", "FAILED"]).sum())
    lines = [
        "",
        "Operations health",
        (f"- OK {ok_count}/{len(result)} | warnings {warning_count} | problems {problem_count}"),
    ]

    non_ok = result[status_values != "OK"].copy()
    if non_ok.empty:
        lines.append("- Status: all checked artifacts are healthy")
        return lines

    if "severity" in non_ok.columns:
        non_ok = non_ok.sort_values("severity", ascending=False)
    for _, row in non_ok.head(3).iterrows():
        check_name = _row_value(row, "check_name")
        status = _row_value(row, "status")
        detail = _truncate_line(str(_row_value(row, "detail")), limit=80)
        lines.append(f"- {check_name}: {status} ({detail})")
    return lines


def _default_transport(telegram_request: request.Request, timeout: float) -> Any:
    context = _default_ssl_context()
    if context is None:
        return request.urlopen(telegram_request, timeout=timeout)
    return request.urlopen(telegram_request, timeout=timeout, context=context)


@lru_cache(maxsize=1)
def _default_ssl_context() -> ssl.SSLContext | None:
    try:
        import certifi
    except ImportError:
        return None

    return ssl.create_default_context(cafile=certifi.where())


def _telegram_send_message_url(bot_token: str) -> str:
    return f"https://api.telegram.org/bot{bot_token}/sendMessage"


def _response_status_code(response: Any) -> int:
    status_value: Any = getattr(response, "status", None)
    if status_value is None:
        status_value = getattr(response, "code", 0)
    return int(status_value)


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def _truncate_message(message: str) -> str:
    if len(message) <= TELEGRAM_MESSAGE_LIMIT:
        return message

    suffix = "\n\n[truncated]"
    return message[: TELEGRAM_MESSAGE_LIMIT - len(suffix)] + suffix


def _truncate_line(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(limit - 3, 0)] + "..."


def _format_number(value: Any) -> str:
    if _is_missing(value):
        return "N/A"
    return f"{float(value):.2f}"


def _format_percent(value: Any) -> str:
    if _is_missing(value):
        return "N/A"
    return f"{float(value) * 100:.2f}%"


def _format_plain_percent(value: Any) -> str:
    if _is_missing(value):
        return "N/A"
    return f"{float(value):.2f}%"


def _format_optional(row: pd.Series, column: str) -> str:
    value = _row_value(row, column)
    return "N/A" if _is_missing(value) else str(value)


def _row_value(row: pd.Series, column: str) -> Any:
    return row[column] if column in row.index else None


def _is_missing(value: Any) -> bool:
    return bool(value is None or pd.isna(value))
