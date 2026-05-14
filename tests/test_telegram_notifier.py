from datetime import datetime
from typing import Any
from urllib import request

import pandas as pd
import pytest

from krx_alpha.telegram.notifier import TelegramNotifier, build_daily_telegram_message


class FakeTelegramResponse:
    def __init__(self, status: int = 200, body: bytes = b'{"ok": true}') -> None:
        self.status = status
        self.body = body

    def __enter__(self) -> "FakeTelegramResponse":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        return None

    def read(self) -> bytes:
        return self.body


def test_build_daily_telegram_message_includes_core_sections() -> None:
    summary = pd.DataFrame(
        {
            "ticker": ["005930", "005380"],
            "status": ["success", "success"],
            "latest_action": ["watch", "buy_candidate"],
            "latest_confidence_score": [63.78, 72.83],
            "latest_financial_score": [50.0, 80.0],
            "latest_event_score": [50.0, 55.0],
            "latest_flow_score": [85.0, 70.0],
            "latest_news_score": [65.0, 45.0],
            "latest_macro_score": [50.0, 42.0],
            "latest_market_regime": ["neutral", "bull"],
        }
    )
    backtest = pd.DataFrame(
        {
            "ticker": ["005380"],
            "trade_count": [7],
            "win_rate": [0.5714],
            "cumulative_return": [0.7867],
            "max_drawdown": [-0.1035],
            "sharpe_ratio": [4.33],
        }
    )
    walk_forward = pd.DataFrame(
        {
            "ticker": ["005380"],
            "fold_count": [3],
            "total_trade_count": [2],
            "compounded_return": [0.0364],
            "worst_max_drawdown": [-0.052],
            "positive_fold_ratio": [0.6667],
        }
    )
    drift = pd.DataFrame(
        {
            "feature": ["rsi_14", "volatility_5d"],
            "drift_detected": [True, False],
            "drift_reason": ["mean_shift", "stable"],
        }
    )
    paper_portfolio = pd.DataFrame(
        {
            "universe": ["demo"],
            "requested_ticker_count": [3],
            "loaded_ticker_count": [3],
            "trade_count": [2],
            "cumulative_return": [0.0123],
            "gross_exposure_pct": [12.5],
            "cash_pct": [87.5],
            "skipped_tickers": [""],
            "generated_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )
    screening = pd.DataFrame(
        {
            "ticker": ["005380", "005930"],
            "passed": [True, False],
            "review_priority": ["high", "low"],
            "screen_score": [72.5, 55.0],
            "final_action": ["buy_candidate", "watch"],
            "confidence_score": [72.83, 63.78],
            "suggested_position_pct": [3.5, 0.0],
            "evidence_summary": ["risk filter passed; trading value surged", ""],
            "caution_summary": ["confirm latest disclosure before action", ""],
        }
    )
    operations_health = pd.DataFrame(
        {
            "check_name": ["Universe summary", "Optional ML metrics"],
            "category": ["signals", "models"],
            "status": ["OK", "WARN"],
            "severity": [0, 1],
            "row_count": [3, None],
            "age_hours": [1.0, None],
            "detail": ["artifact is present and readable", "optional artifact not found"],
        }
    )

    message = build_daily_telegram_message(
        summary,
        screening_result=screening,
        paper_portfolio_summary=paper_portfolio,
        backtest_metrics=backtest,
        walk_forward_summary=walk_forward,
        drift_result=drift,
        operations_health=operations_health,
        generated_at=datetime(2026, 5, 13, 9, 0),
        top_n=1,
    )

    assert "KRX Alpha Daily Brief" in message
    assert "1. 005380 | buy_candidate" in message
    assert "News 45.00" in message
    assert "Macro 42.00" in message
    assert "Auto screener" in message
    assert "Checked 2 | passed 1" in message
    assert "priority high" in message
    assert "screen 72.50" in message
    assert "evidence: risk filter passed" in message
    assert "caution: confirm latest disclosure" in message
    assert "Paper portfolio" in message
    assert "demo | tickers 3/3 | trades 2" in message
    assert "Backtest" in message
    assert "Walk-forward" in message
    assert "Data drift: 1/2 features flagged" in message
    assert "rsi_14: mean_shift" in message
    assert "Operations health" in message
    assert "OK 1/2 | warnings 1 | problems 0" in message
    assert "Optional ML metrics: WARN" in message
    assert "Screening aid only" in message


def test_telegram_notifier_dry_run_does_not_require_credentials() -> None:
    result = TelegramNotifier(bot_token=None, chat_id=None).send_message("hello", dry_run=True)

    assert result.sent is False
    assert result.dry_run is True
    assert result.message == "hello"


def test_telegram_notifier_uses_transport() -> None:
    captured: dict[str, Any] = {}

    def fake_transport(telegram_request: request.Request, timeout: float) -> FakeTelegramResponse:
        captured["url"] = telegram_request.full_url
        captured["data"] = telegram_request.data
        captured["timeout"] = timeout
        return FakeTelegramResponse()

    result = TelegramNotifier(
        bot_token="token",
        chat_id="123",
        timeout_seconds=3.0,
        transport=fake_transport,
    ).send_message("hello")

    assert result.sent is True
    assert result.status_code == 200
    assert "bottoken/sendMessage" in captured["url"]
    assert b"chat_id=123" in captured["data"]
    assert captured["timeout"] == 3.0


def test_telegram_notifier_retries_transient_transport_failure() -> None:
    calls: list[int] = []

    def fake_transport(telegram_request: request.Request, timeout: float) -> FakeTelegramResponse:
        calls.append(1)
        if len(calls) == 1:
            raise TimeoutError("temporary network failure")
        return FakeTelegramResponse()

    result = TelegramNotifier(
        bot_token="token",
        chat_id="123",
        max_retries=1,
        retry_sleep_seconds=0,
        transport=fake_transport,
    ).send_message("hello")

    assert result.sent is True
    assert len(calls) == 2


def test_telegram_notifier_retries_retryable_api_status() -> None:
    statuses = [500, 200]

    def fake_transport(telegram_request: request.Request, timeout: float) -> FakeTelegramResponse:
        return FakeTelegramResponse(status=statuses.pop(0))

    result = TelegramNotifier(
        bot_token="token",
        chat_id="123",
        max_retries=1,
        retry_sleep_seconds=0,
        transport=fake_transport,
    ).send_message("hello")

    assert result.sent is True
    assert result.status_code == 200
    assert statuses == []


def test_telegram_notifier_does_not_retry_non_retryable_api_status() -> None:
    calls: list[int] = []

    def fake_transport(telegram_request: request.Request, timeout: float) -> FakeTelegramResponse:
        calls.append(1)
        return FakeTelegramResponse(status=400)

    with pytest.raises(RuntimeError, match="status 400"):
        TelegramNotifier(
            bot_token="token",
            chat_id="123",
            max_retries=2,
            retry_sleep_seconds=0,
            transport=fake_transport,
        ).send_message("hello")

    assert len(calls) == 1


def test_telegram_notifier_requires_credentials_when_sending() -> None:
    with pytest.raises(ValueError, match="TELEGRAM_BOT_TOKEN"):
        TelegramNotifier(bot_token=None, chat_id=None).send_message("hello")
