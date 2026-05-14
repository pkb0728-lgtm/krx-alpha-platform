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

STOCK_NAME_KO = {
    "000270": "기아",
    "000660": "SK하이닉스",
    "005380": "현대차",
    "005930": "삼성전자",
    "035420": "NAVER",
    "035720": "카카오",
    "042700": "한미반도체",
    "051910": "LG화학",
    "055550": "신한지주",
    "068270": "셀트리온",
    "105560": "KB금융",
}

ACTION_KO = {
    "buy_candidate": "매수 검토",
    "watch": "관망",
    "hold": "보유/중립",
    "avoid": "회피",
    "blocked": "리스크 차단",
}

ACTION_GUIDE_KO = {
    "buy_candidate": "조건은 좋지만 매수 전 뉴스, 공시, 유동성을 다시 확인하세요.",
    "watch": "관심 종목으로 지켜보되 아직 적극 매수 단계는 아닙니다.",
    "hold": "뚜렷한 방향성이 약해 중립적으로 보는 상태입니다.",
    "avoid": "현재 조건에서는 신규 매수 검토를 피하는 편이 안전합니다.",
    "blocked": "리스크 필터가 먼저 작동했으므로 매수 후보에서 제외합니다.",
}

REGIME_KO = {
    "bull": "상승장",
    "bullish": "상승 우위",
    "uptrend": "상승 추세",
    "bear": "하락장",
    "downtrend": "하락 추세",
    "sideways": "횡보장",
    "neutral": "중립",
    "rebound": "반등",
    "high_volatility": "고변동성장",
    "insufficient_data": "데이터 부족",
    "unknown": "알 수 없음",
}

PRIORITY_KO = {
    "high": "높음",
    "medium": "중간",
    "watchlist": "관찰",
    "low": "낮음",
    "blocked": "차단",
}

SCREEN_STATUS_KO = {
    "passed": "조건 통과",
    "confidence_below_threshold": "신뢰도 부족",
    "confidence_and_score_below_threshold": "신뢰도와 점수 부족",
    "score_below_threshold": "점수 부족",
    "screen_score_below_threshold": "점수 부족",
    "action_not_allowed": "허용되지 않은 판단",
    "risk_blocked": "리스크 차단",
    "signal_file_missing_or_empty": "신호 파일 없음",
}

CANDIDATE_ACTION_KO = {
    "review_buy": "매수 검토",
    "review_add": "추가매수 검토",
    "manual_price_required": "가격 확인 필요",
    "hold_review": "관망",
    "skip": "제외",
}

CANDIDATE_GUIDE_KO = {
    "review_buy": (
        "모의계좌 기준 매수 검토 후보입니다. 실제 매매 전에는 사람이 다시 확인해야 합니다."
    ),
    "review_add": "보유 중인 종목의 추가매수 검토 후보입니다. 현재 비중을 먼저 확인하세요.",
    "manual_price_required": "현재가 기준이 부족해 사람이 가격을 직접 확인해야 합니다.",
    "hold_review": "지켜볼 수는 있지만 지금 바로 매수 후보로 보기는 어렵습니다.",
    "skip": "조건 미달 또는 리스크 차단으로 후보에서 제외되었습니다.",
}

RISK_FLAG_KO = {
    "insufficient_history": "과거 데이터 부족",
    "low_liquidity": "유동성 부족",
    "wide_daily_range": "하루 변동폭 큼",
    "high_short_term_volatility": "단기 변동성 높음",
    "weak_risk_score": "위험 점수 약함",
    "disclosure_event_risk": "공시 이벤트 확인 필요",
    "weak_investor_flow": "외국인/기관 수급 약함",
    "weak_macro_environment": "거시 환경 비우호적",
    "market_regime_bear": "시장 하락장",
    "market_regime_high_volatility": "시장 고변동성",
    "signal_file_missing_or_empty": "신호 파일 없음",
}

CAUTION_KO = {
    "candidate did not pass all screen thresholds": "스크리너 기준을 모두 통과하지 못했습니다.",
    "risk filter blocked the signal": "리스크 필터가 신호를 차단했습니다.",
    "market regime is not supportive": "시장 국면이 우호적이지 않습니다.",
    "market regime needs more data": "시장 국면 판단에 데이터가 더 필요합니다.",
    "financial score is weak": "재무 점수가 약합니다.",
    "disclosure event score is weak": "공시 이벤트 점수가 약합니다.",
    "news sentiment is weak or missing": "뉴스 감성 점수가 약하거나 없습니다.",
    "short-term volatility is elevated": "단기 변동성이 높습니다.",
    "RSI looks overheated": "RSI가 과열 구간에 가깝습니다.",
    "no hard block, but confirm latest news and disclosures": (
        "강한 차단 사유는 없지만 최신 뉴스와 공시는 확인해야 합니다."
    ),
}

CHECKLIST_KO = {
    "identify_failed_threshold": "미달 기준 확인",
    "confirm_recent_news": "최근 뉴스 확인",
    "check_dart_disclosures": "DART 공시 확인",
    "verify_liquidity": "거래대금/유동성 확인",
    "review_position_size": "투자 비중 확인",
    "extend_price_history_for_regime": "더 긴 기간으로 시장 국면 재확인",
    "rebuild_price_features": "가격 피처 재생성",
    "rerun_pipeline": "파이프라인 재실행",
    "confirm_signal_artifact": "최종 신호 파일 확인",
}

EVIDENCE_KO = {
    "risk filter passed": "리스크 필터 통과",
    "trading value surged over the 5-day baseline": "최근 5일 대비 거래대금 급증",
    "trading value increased over the 5-day baseline": "최근 5일 대비 거래대금 증가",
    "RSI is in a recovery-friendly range": "RSI가 반등 기대 구간",
    "financial score is supportive": "재무 점수 우호적",
    "investor flow score is supportive": "수급 점수 우호적",
    "news sentiment score is supportive": "뉴스 감성 점수 우호적",
}

HEALTH_STATUS_KO = {
    "OK": "정상",
    "WARN": "주의",
    "STALE": "오래됨",
    "MISSING": "누락",
    "EMPTY": "비어 있음",
    "FAILED": "실패",
}


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
    kis_paper_candidates: Any | None = None,
    backtest_metrics: Any | None = None,
    walk_forward_summary: Any | None = None,
    drift_result: Any | None = None,
    operations_health: Any | None = None,
    generated_at: datetime | None = None,
    top_n: int = 5,
) -> str:
    """Build a compact Korean daily operations brief for Telegram."""
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
        "KRX Alpha 일일 요약",
        f"생성 시각: {generated.strftime('%Y-%m-%d %H:%M')}",
        "",
        "전체 분석",
        f"- 분석 종목: {len(frame)}개",
        f"- 정상 처리: {len(success_frame)}개",
        f"- 실패: {len(failed_frame)}개",
        "",
        "상위 종목",
    ]
    lines.extend(_format_candidate_lines(success_frame, top_n))
    lines.extend(_format_screening_lines(screening_result, top_n))
    lines.extend(_format_paper_portfolio_lines(paper_portfolio_summary))
    lines.extend(_format_kis_candidate_lines(kis_paper_candidates, top_n))
    lines.extend(_format_backtest_lines(backtest_metrics))
    lines.extend(_format_walk_forward_lines(walk_forward_summary))
    lines.extend(_format_drift_lines(drift_result))
    lines.extend(_format_operations_health_lines(operations_health))
    lines.extend(
        [
            "",
            "주의",
            "- 이 메시지는 투자 판단 보조용이며 투자 조언이 아닙니다.",
            "- 실제 매매 전에는 유동성, 공시, 뉴스, 시장 국면을 직접 확인하세요.",
            "- KIS 모의투자 후보는 검토 목록일 뿐 실제 주문을 보내지 않습니다.",
        ]
    )
    return _truncate_message("\n".join(lines))


def _format_candidate_lines(frame: pd.DataFrame, top_n: int) -> list[str]:
    if frame.empty:
        return ["- 정상 처리된 종목이 없습니다."]

    lines: list[str] = []
    limited_frame = frame.head(max(top_n, 1))
    for rank, (_, row) in enumerate(limited_frame.iterrows(), start=1):
        action = _format_action(_row_value(row, "latest_action"))
        regime = _format_regime(_row_value(row, "latest_market_regime"))
        lines.append(
            f"{rank}. {_stock_label(row)} | 판단: {action} | "
            f"신뢰도 {_format_number(row['latest_confidence_score'])} | 시장: {regime}"
        )
        lines.append(
            "   점수: "
            f"재무 {_format_number(_row_value(row, 'latest_financial_score'))} / "
            f"공시 {_format_number(_row_value(row, 'latest_event_score'))} / "
            f"수급 {_format_number(_row_value(row, 'latest_flow_score'))} / "
            f"뉴스 {_format_number(_row_value(row, 'latest_news_score'))} / "
            f"매크로 {_format_number(_row_value(row, 'latest_macro_score'))}"
        )
        lines.append(f"   해석: {_action_guide(_row_value(row, 'latest_action'))}")
    return lines


def _format_screening_lines(result: Any | None, top_n: int) -> list[str]:
    if result is None or result.empty:
        return ["", "자동 스크리너", "- 최신 스크리너 결과가 없습니다."]

    frame = result.copy()
    passed_frame = frame[frame["passed"]] if "passed" in frame.columns else frame.iloc[0:0]
    lines = [
        "",
        "자동 스크리너",
        f"- 검사 종목: {len(frame)}개 / 통과: {len(passed_frame)}개",
    ]
    status_summary = _screen_status_summary(frame)
    if status_summary:
        lines.append(f"- 상태 요약: {status_summary}")
    if passed_frame.empty:
        lines.append("- 통과 종목 없음: 지금은 억지로 고를 종목이 없습니다.")
    else:
        ranked = passed_frame.sort_values(
            ["screen_score", "confidence_score"],
            ascending=[False, False],
        ).head(max(top_n, 1))
        for rank, (_, row) in enumerate(ranked.iterrows(), start=1):
            lines.append(
                f"{rank}. {_stock_label(row)} | 판단: {_format_action(row['final_action'])} | "
                f"우선순위: {_format_priority(_row_value(row, 'review_priority'))} | "
                f"점수 {_format_number(row['screen_score'])} | "
                f"신뢰도 {_format_number(row['confidence_score'])} | "
                f"제안 비중 {_format_plain_percent(row['suggested_position_pct'])}"
            )
            evidence = _row_value(row, "evidence_summary")
            caution = _row_value(row, "caution_summary")
            if not _is_missing(evidence) and str(evidence):
                lines.append(f"   근거: {_truncate_line(_format_evidence(evidence), limit=120)}")
            if not _is_missing(caution) and str(caution):
                lines.append(f"   주의: {_truncate_line(_format_caution(caution), limit=120)}")

    lines.extend(_format_screening_review_queue(frame, top_n))
    return lines


def _format_screening_review_queue(frame: pd.DataFrame, top_n: int) -> list[str]:
    if "passed" not in frame.columns:
        return []

    review_frame = frame[~_passed_mask(frame["passed"])]
    if review_frame.empty:
        return []

    if "review_priority" in review_frame.columns:
        priority_order = {"high": 0, "medium": 1, "watchlist": 2, "low": 3, "blocked": 4}
        review_frame = review_frame.copy()
        review_frame["_priority_order"] = (
            review_frame["review_priority"].map(priority_order).fillna(99)
        )
        review_frame = review_frame.sort_values(
            ["_priority_order", "screen_score", "confidence_score"],
            ascending=[True, False, False],
        ).drop(columns=["_priority_order"])

    lines = ["- 확인 필요 종목:"]
    for _, row in review_frame.head(max(top_n, 1)).iterrows():
        risk_flags = _format_risk_flags(_row_value(row, "risk_flags"))
        risk_text = f" | 리스크: {risk_flags}" if risk_flags != "없음" else ""
        priority = _format_priority(_row_value(row, "review_priority"))
        status = _format_screen_status(_row_value(row, "screen_status_reason"))
        lines.append(
            f"  - {_stock_label(row)} | 우선순위: {priority} | "
            f"사유: {status} | 점수 {_format_number(row['screen_score'])}{risk_text}"
        )
        lines.append(f"    해석: {_screening_beginner_summary(row)}")
    return lines


def _passed_mask(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    return series.fillna(False).astype(str).str.lower().isin({"true", "1", "yes"})


def _screen_status_summary(frame: pd.DataFrame) -> str:
    if "screen_status_reason" not in frame.columns:
        return ""

    counts = frame["screen_status_reason"].fillna("unknown").astype(str).value_counts()
    return ", ".join(
        f"{_format_screen_status(reason)} {count}개" for reason, count in counts.head(3).items()
    )


def _format_paper_portfolio_lines(summary: Any | None) -> list[str]:
    if summary is None or summary.empty:
        return ["", "모의 포트폴리오", "- 최신 모의 포트폴리오 결과가 없습니다."]

    metric = summary.sort_values("generated_at", ascending=False).iloc[0]
    lines = [
        "",
        "모의 포트폴리오",
        (
            f"- 유니버스: {metric['universe']} | 로드 "
            f"{int(metric['loaded_ticker_count'])}/{int(metric['requested_ticker_count'])}개 | "
            f"거래 {int(metric['trade_count'])}회 | "
            f"수익률 {_format_percent(metric['cumulative_return'])} | "
            f"노출 {_format_plain_percent(metric['gross_exposure_pct'])} | "
            f"현금 {_format_plain_percent(metric['cash_pct'])}"
        ),
    ]
    skipped_tickers = _row_value(metric, "skipped_tickers")
    if not _is_missing(skipped_tickers) and str(skipped_tickers):
        lines.append(f"- 제외 종목: {skipped_tickers}")
    return lines


def _format_kis_candidate_lines(candidates: Any | None, top_n: int) -> list[str]:
    if candidates is None:
        return ["", "KIS 모의투자 후보", "- 이번 실행에는 KIS 후보 계산이 포함되지 않았습니다."]
    if candidates.empty:
        return ["", "KIS 모의투자 후보", "- KIS 후보 결과가 비어 있습니다."]

    frame = candidates.copy()
    review_mask = frame["candidate_action"].isin(["review_buy", "review_add"])
    manual_mask = frame["candidate_action"] == "manual_price_required"
    lines = [
        "",
        "KIS 모의투자 후보",
        (
            f"- 전체 후보 행: {len(frame)}개 / 매수·추가매수 검토: "
            f"{int(review_mask.sum())}개 / 가격 확인 필요: {int(manual_mask.sum())}개"
        ),
        "- 실제 주문은 보내지 않고, 모의계좌 기준 검토 목록만 만듭니다.",
    ]

    action_order = {
        "review_buy": 0,
        "review_add": 1,
        "manual_price_required": 2,
        "hold_review": 3,
        "skip": 4,
    }
    ranked = frame.copy()
    ranked["_action_order"] = ranked["candidate_action"].map(action_order).fillna(99)
    ranked = ranked.sort_values(
        ["_action_order", "confidence_score", "screen_score"],
        ascending=[True, False, False],
    ).drop(columns=["_action_order"])

    for rank, (_, row) in enumerate(ranked.head(max(top_n, 1)).iterrows(), start=1):
        action = _format_candidate_action(_row_value(row, "candidate_action"))
        lines.append(
            f"{rank}. {_stock_label(row)} | 상태: {action} | "
            f"예상 {int(_numeric_value(row, 'estimated_quantity'))}주 / "
            f"{_format_money(_numeric_value(row, 'estimated_amount'))}원 | "
            f"목표 비중 {_format_plain_percent(_row_value(row, 'target_position_pct'))}"
        )
        lines.append(f"   해석: {_candidate_guide(_row_value(row, 'candidate_action'))}")

    return lines


def _format_backtest_lines(metrics: Any | None) -> list[str]:
    if metrics is None or metrics.empty:
        return ["", "백테스트", "- 최신 백테스트 결과가 없습니다."]

    metric = metrics.sort_values("cumulative_return", ascending=False).iloc[0]
    return [
        "",
        "백테스트",
        (
            f"- {_stock_label(metric)} | 거래 {int(metric['trade_count'])}회 | "
            f"승률 {_format_percent(metric['win_rate'])} | "
            f"수익률 {_format_percent(metric['cumulative_return'])} | "
            f"MDD {_format_percent(metric['max_drawdown'])} | "
            f"샤프 {float(metric['sharpe_ratio']):.2f}"
        ),
    ]


def _format_walk_forward_lines(summary: Any | None) -> list[str]:
    if summary is None or summary.empty:
        return ["", "워크포워드 검증", "- 최신 워크포워드 결과가 없습니다."]

    metric = summary.sort_values("compounded_return", ascending=False).iloc[0]
    return [
        "",
        "워크포워드 검증",
        (
            f"- {_stock_label(metric)} | 구간 {int(metric['fold_count'])}개 | "
            f"거래 {int(metric['total_trade_count'])}회 | "
            f"복리 수익률 {_format_percent(metric['compounded_return'])} | "
            f"최악 MDD {_format_percent(metric['worst_max_drawdown'])} | "
            f"양수 구간 {_format_percent(metric['positive_fold_ratio'])}"
        ),
    ]


def _format_drift_lines(result: Any | None) -> list[str]:
    if result is None or result.empty or "drift_detected" not in result.columns:
        return ["", "드리프트 점검", "- 최신 드리프트 결과가 없습니다."]

    drift_count = int(result["drift_detected"].sum())
    if "feature" in result.columns:
        drifted = result[result["drift_detected"]].head(3)
        lines = [
            "",
            "드리프트 점검",
            f"- 데이터 드리프트: {len(result)}개 피처 중 {drift_count}개 경고",
        ]
        if drifted.empty:
            lines.append("- 상태: 안정적")
        else:
            for _, row in drifted.iterrows():
                lines.append(f"- {row['feature']}: {row['drift_reason']}")
        return lines

    row = result.iloc[0]
    return [
        "",
        "드리프트 점검",
        (
            f"- 성능 드리프트: {row['metric']} | "
            f"감지 여부 {bool(row['drift_detected'])} | "
            f"사유 {row['drift_reason']}"
        ),
    ]


def _format_operations_health_lines(result: Any | None) -> list[str]:
    if result is None or result.empty or "status" not in result.columns:
        return ["", "운영 상태", "- 최신 운영 상태 점검 결과가 없습니다."]

    status_values = result["status"].astype(str)
    ok_count = int((status_values == "OK").sum())
    warning_count = int(status_values.isin(["WARN", "STALE"]).sum())
    problem_count = int(status_values.isin(["MISSING", "EMPTY", "FAILED"]).sum())
    lines = [
        "",
        "운영 상태",
        (f"- 정상 {ok_count}/{len(result)}개 | 주의 {warning_count}개 | 문제 {problem_count}개"),
    ]

    non_ok = result[status_values != "OK"].copy()
    if non_ok.empty:
        lines.append("- 상태: 점검한 산출물이 모두 정상입니다.")
        return lines

    if "severity" in non_ok.columns:
        non_ok = non_ok.sort_values("severity", ascending=False)
    for _, row in non_ok.head(3).iterrows():
        check_name = _row_value(row, "check_name")
        status = _row_value(row, "status")
        detail = _truncate_line(str(_row_value(row, "detail")), limit=80)
        action = _row_value(row, "action")
        if not _is_missing(action) and str(action).strip():
            detail = f"{detail}; 조치: {_truncate_line(str(action), limit=80)}"
        lines.append(f"- {check_name}: {_format_health_status(status)} ({detail})")
    return lines


def _stock_label(row: pd.Series) -> str:
    ticker = _ticker_text(_row_value(row, "ticker"))
    explicit_name = _row_value(row, "stock_name")
    if _is_missing(explicit_name):
        explicit_name = _row_value(row, "name")
    stock_name = str(explicit_name).strip() if not _is_missing(explicit_name) else ""
    if not stock_name:
        stock_name = STOCK_NAME_KO.get(ticker, "")
    return f"{ticker} {stock_name}".strip()


def _ticker_text(value: Any) -> str:
    if _is_missing(value):
        return "N/A"
    text = str(value).strip()
    return text.zfill(6) if text.isdigit() else text


def _format_action(value: Any) -> str:
    text = "" if _is_missing(value) else str(value)
    return ACTION_KO.get(text, text or "N/A")


def _action_guide(value: Any) -> str:
    text = "" if _is_missing(value) else str(value)
    return ACTION_GUIDE_KO.get(text, "점수와 리스크를 함께 보고 사람이 최종 판단해야 합니다.")


def _format_regime(value: Any) -> str:
    text = "" if _is_missing(value) else str(value)
    return REGIME_KO.get(text, text or "N/A")


def _format_priority(value: Any) -> str:
    text = "" if _is_missing(value) else str(value)
    return PRIORITY_KO.get(text, text or "N/A")


def _format_screen_status(value: Any) -> str:
    text = "" if _is_missing(value) else str(value)
    if ":" in text:
        head, tail = text.split(":", maxsplit=1)
        mapped_tail = _format_risk_flags(tail)
        return f"{SCREEN_STATUS_KO.get(head, head)}({mapped_tail})"
    return SCREEN_STATUS_KO.get(text, text or "N/A")


def _format_candidate_action(value: Any) -> str:
    text = "" if _is_missing(value) else str(value)
    return CANDIDATE_ACTION_KO.get(text, text or "N/A")


def _candidate_guide(value: Any) -> str:
    text = "" if _is_missing(value) else str(value)
    return CANDIDATE_GUIDE_KO.get(text, "후보 상태를 추가로 확인해야 합니다.")


def _format_risk_flags(value: Any) -> str:
    if _is_missing(value):
        return "없음"
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan"}:
        return "없음"
    flags = [flag.strip() for flag in text.split(",") if flag.strip()]
    if not flags:
        return "없음"
    return ", ".join(RISK_FLAG_KO.get(flag, flag) for flag in flags)


def _format_evidence(value: Any) -> str:
    if _is_missing(value):
        return "표시할 근거가 없습니다."
    phrases = [phrase.strip() for phrase in str(value).split(";") if phrase.strip()]
    if not phrases:
        return "표시할 근거가 없습니다."
    return " / ".join(_format_evidence_phrase(phrase) for phrase in phrases)


def _format_evidence_phrase(phrase: str) -> str:
    if " action with screen score " in phrase:
        action, score = phrase.split(" action with screen score ", maxsplit=1)
        return f"판단 {_format_action(action)}, 스크리너 점수 {score}"
    if phrase.startswith("confidence "):
        return f"신뢰도 {phrase.removeprefix('confidence ')}"
    if phrase.startswith("latest trading value "):
        return f"최근 거래대금 {phrase.removeprefix('latest trading value ')}"
    return EVIDENCE_KO.get(phrase, phrase)


def _format_caution(value: Any) -> str:
    if _is_missing(value):
        return "특별한 주의 문구가 없습니다."
    phrases = [phrase.strip() for phrase in str(value).split(";") if phrase.strip()]
    if not phrases:
        return "특별한 주의 문구가 없습니다."
    return " / ".join(CAUTION_KO.get(phrase, phrase) for phrase in phrases)


def _format_checklist(value: Any) -> str:
    if _is_missing(value):
        return "추가 확인 항목이 없습니다."
    items = [item.strip() for item in str(value).split(",") if item.strip()]
    if not items:
        return "추가 확인 항목이 없습니다."
    return ", ".join(CHECKLIST_KO.get(item, item) for item in items)


def _screening_beginner_summary(row: pd.Series) -> str:
    risk_flags = _format_risk_flags(_row_value(row, "risk_flags"))
    if risk_flags != "없음":
        return f"현재는 {risk_flags} 때문에 매수 후보로 보기 어렵습니다."
    action = str(_row_value(row, "final_action"))
    if action in ACTION_GUIDE_KO:
        return ACTION_GUIDE_KO[action]
    checklist = _format_checklist(_row_value(row, "review_checklist"))
    return f"추가 확인이 필요합니다: {checklist}"


def _format_health_status(value: Any) -> str:
    text = "" if _is_missing(value) else str(value)
    return HEALTH_STATUS_KO.get(text, text or "N/A")


def _numeric_value(row: pd.Series, column: str) -> float:
    value = _row_value(row, column)
    if _is_missing(value):
        return 0.0
    return float(value)


def _format_money(value: float) -> str:
    return f"{value:,.0f}"


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
