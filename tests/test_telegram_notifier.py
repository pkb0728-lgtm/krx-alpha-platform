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
    kis_candidates = pd.DataFrame(
        {
            "ticker": ["005380", "005930"],
            "candidate_action": ["review_buy", "skip"],
            "estimated_quantity": [3, 0],
            "estimated_amount": [300_000.0, 0.0],
            "target_position_pct": [10.0, 0.0],
            "confidence_score": [72.0, 58.0],
            "screen_score": [75.0, 50.0],
        }
    )
    screening = pd.DataFrame(
        {
            "ticker": ["005380", "005930"],
            "passed": [True, False],
            "screen_status_reason": ["passed", "confidence_below_threshold"],
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
            "action": ["", "generate ML baseline when model monitoring is needed"],
        }
    )

    message = build_daily_telegram_message(
        summary,
        screening_result=screening,
        paper_portfolio_summary=paper_portfolio,
        kis_paper_candidates=kis_candidates,
        backtest_metrics=backtest,
        walk_forward_summary=walk_forward,
        drift_result=drift,
        operations_health=operations_health,
        generated_at=datetime(2026, 5, 13, 9, 0),
        top_n=1,
    )

    assert "KRX Alpha 일일 요약" in message
    assert "1. 005380 현대차 | 판단: 매수 검토" in message
    assert "뉴스 45.00" in message
    assert "매크로 42.00" in message
    assert "자동 스크리너" in message
    assert "검사 종목: 2개 / 통과: 1개" in message
    assert "상태 요약: 조건 통과 1개, 신뢰도 부족 1개" in message
    assert "우선순위: 높음" in message
    assert "점수 72.50" in message
    assert "근거: 리스크 필터 통과" in message
    assert "주의: confirm latest disclosure before action" in message
    assert "확인 필요 종목" in message
    assert "005930 삼성전자 | 우선순위: 낮음 | 사유: 신뢰도 부족 | 점수 55.00" in message
    assert "모의 포트폴리오" in message
    assert "유니버스: demo | 로드 3/3개 | 거래 2회" in message
    assert "KIS 모의투자 후보" in message
    assert "매수·추가매수 검토: 1개" in message
    assert "백테스트" in message
    assert "워크포워드 검증" in message
    assert "데이터 드리프트: 2개 피처 중 1개 경고" in message
    assert "rsi_14: mean_shift" in message
    assert "운영 상태" in message
    assert "정상 1/2개 | 주의 1개 | 문제 0개" in message
    assert "Optional ML metrics: 주의" in message
    assert "generate ML baseline" in message
    assert "투자 판단 보조용" in message


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
