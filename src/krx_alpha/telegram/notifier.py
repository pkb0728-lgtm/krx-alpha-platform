from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib import parse, request

import pandas as pd

TELEGRAM_MESSAGE_LIMIT = 4096

TelegramTransport = Callable[[request.Request, float], Any]


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
        transport: TelegramTransport | None = None,
    ) -> None:
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.timeout_seconds = timeout_seconds
        self._transport = transport or _default_transport

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

        try:
            with self._transport(telegram_request, self.timeout_seconds) as response:
                response_text = response.read().decode("utf-8", errors="replace")
                status_code = _response_status_code(response)
        except Exception as exc:
            raise RuntimeError("Telegram message send failed.") from exc

        if status_code < 200 or status_code >= 300:
            raise RuntimeError(f"Telegram API returned status {status_code}.")

        return TelegramSendResult(
            sent=True,
            dry_run=False,
            status_code=status_code,
            message=text,
            response_text=response_text,
        )


def build_daily_telegram_message(
    universe_summary: Any,
    backtest_metrics: Any | None = None,
    walk_forward_summary: Any | None = None,
    drift_result: Any | None = None,
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
    lines.extend(_format_backtest_lines(backtest_metrics))
    lines.extend(_format_walk_forward_lines(walk_forward_summary))
    lines.extend(_format_drift_lines(drift_result))
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
            f"Flow {_format_number(_row_value(row, 'latest_flow_score'))}"
        )
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


def _default_transport(telegram_request: request.Request, timeout: float) -> Any:
    return request.urlopen(telegram_request, timeout=timeout)


def _telegram_send_message_url(bot_token: str) -> str:
    return f"https://api.telegram.org/bot{bot_token}/sendMessage"


def _response_status_code(response: Any) -> int:
    status_value: Any = getattr(response, "status", None)
    if status_value is None:
        status_value = getattr(response, "code", 0)
    return int(status_value)


def _truncate_message(message: str) -> str:
    if len(message) <= TELEGRAM_MESSAGE_LIMIT:
        return message

    suffix = "\n\n[truncated]"
    return message[: TELEGRAM_MESSAGE_LIMIT - len(suffix)] + suffix


def _format_number(value: Any) -> str:
    if _is_missing(value):
        return "N/A"
    return f"{float(value):.2f}"


def _format_percent(value: Any) -> str:
    if _is_missing(value):
        return "N/A"
    return f"{float(value) * 100:.2f}%"


def _format_optional(row: pd.Series, column: str) -> str:
    value = _row_value(row, column)
    return "N/A" if _is_missing(value) else str(value)


def _row_value(row: pd.Series, column: str) -> Any:
    return row[column] if column in row.index else None


def _is_missing(value: Any) -> bool:
    return bool(value is None or pd.isna(value))
