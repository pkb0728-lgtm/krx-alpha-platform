from dataclasses import dataclass
from typing import Any

import pandas as pd

ACTION_KO = {
    "buy_candidate": "매수 검토 후보",
    "watch": "관찰",
    "hold": "중립/보류",
    "avoid": "회피",
    "blocked": "리스크 차단",
}

REGIME_KO = {
    "bull": "상승장",
    "bear": "하락장",
    "sideways": "횡보장",
    "high_volatility": "고변동성장",
    "rebound": "반등 가능 구간",
    "neutral": "중립",
    "insufficient_data": "데이터 부족",
    "unknown": "알 수 없음",
}

MATCHED_BY_KO = {
    "ticker": "종목코드",
    "name": "회사명",
    "partial_name": "부분 회사명",
}

SOURCE_KO = {
    "static_alias": "내장 종목명",
    "finance_data_reader": "무료 상장 목록",
    "user_input": "직접 입력",
}

REASON_KO = {
    "close_above_ma5": "종가가 5일 이동평균선 위에 있습니다.",
    "close_above_ma20": "종가가 20일 이동평균선 위에 있습니다.",
    "rsi_recovery_zone": "RSI가 과열이 아닌 회복/중립 구간입니다.",
    "trading_value_increase": "최근 5일 기준 거래대금이 증가했습니다.",
    "high_short_term_volatility": "단기 변동성이 높아 주의가 필요합니다.",
    "insufficient_or_neutral_evidence": "현재 규칙 기준으로 뚜렷한 우위가 부족합니다.",
    "no_financial_feature_available": (
        "재무 데이터가 연결되지 않아 중립 점수 50점으로 처리했습니다."
    ),
    "revenue_growth_positive": "매출 성장률이 긍정적입니다.",
    "revenue_growth_negative": "매출 성장률이 약합니다.",
    "operating_margin_healthy": "영업이익률이 양호합니다.",
    "operating_loss": "영업손실 구간입니다.",
    "net_margin_positive": "순이익률이 플러스입니다.",
    "debt_ratio_conservative": "부채비율이 보수적인 편입니다.",
    "debt_ratio_high": "부채비율이 높아 재무 리스크를 확인해야 합니다.",
    "roe_positive": "ROE가 플러스입니다.",
    "financial_evidence_neutral": "재무 근거는 중립입니다.",
    "no_disclosure_event_available": "공시 이벤트 데이터가 연결되지 않아 중립 처리했습니다.",
    "disclosure_routine_report": "정기 보고서 성격의 공시입니다.",
    "disclosure_event_neutral": "공시 이벤트는 현재 규칙 기준 중립입니다.",
    "disclosure_positive_shareholder_return": "주주환원 관련 긍정 공시입니다.",
    "disclosure_positive_dividend": "배당 관련 긍정 공시입니다.",
    "disclosure_positive_contract": "계약/수주 등 사업 모멘텀 가능성이 있는 공시입니다.",
    "disclosure_risk_capital_increase": "유상증자 가능성으로 희석 리스크가 있습니다.",
    "disclosure_risk_convertible_bond": "전환사채 관련 오버행 리스크가 있습니다.",
    "disclosure_risk_warrant": "신주인수권 관련 오버행 리스크가 있습니다.",
    "disclosure_risk_litigation": "소송 리스크가 포함된 공시입니다.",
    "disclosure_risk_governance": "지배구조 리스크가 포함된 공시입니다.",
    "disclosure_risk_listing": "상장/거래정지 관련 리스크가 포함된 공시입니다.",
    "disclosure_risk_audit_or_designation": "감사/지정 관련 리스크가 포함된 공시입니다.",
    "no_investor_flow_available": "수급 데이터가 연결되지 않아 중립 점수 50점으로 처리했습니다.",
    "foreign_net_buy_positive_5d": "최근 외국인 순매수가 긍정적입니다.",
    "foreign_net_sell_5d": "최근 외국인 순매도가 나타났습니다.",
    "institution_net_buy_positive_5d": "최근 기관 순매수가 긍정적입니다.",
    "institution_net_sell_5d": "최근 기관 순매도가 나타났습니다.",
    "strong_smart_money_inflow": "외국인과 기관 수급이 강하게 유입됐습니다.",
    "smart_money_inflow": "외국인/기관 수급이 대체로 긍정적입니다.",
    "strong_smart_money_outflow": "외국인과 기관 수급이 강하게 이탈했습니다.",
    "smart_money_outflow": "외국인/기관 수급이 부정적입니다.",
    "investor_flow_neutral": "수급 근거는 중립입니다.",
    "no_news_sentiment_available": "뉴스 감성 데이터가 연결되지 않아 중립 점수 50점입니다.",
    "news_sentiment_positive": "뉴스 톤이 긍정적입니다.",
    "news_sentiment_negative": "뉴스 톤이 부정적입니다.",
    "news_sentiment_neutral": "뉴스 톤이 중립 또는 혼재 상태입니다.",
    "news_volume_elevated": "뉴스량이 많아 사람이 직접 확인할 필요가 있습니다.",
    "no_macro_feature_available": "거시 데이터가 연결되지 않아 중립 점수 50점입니다.",
    "us_10y_yield_high": "미국 10년물 금리가 높아 주식 밸류에이션에 부담입니다.",
    "us_10y_yield_elevated": "미국 10년물 금리가 다소 높은 편입니다.",
    "us_10y_yield_supportive": "미국 10년물 금리 환경이 비교적 우호적입니다.",
    "fed_funds_rate_restrictive": "미국 기준금리가 긴축적입니다.",
    "us_10y_yield_rising": "미국 10년물 금리가 상승했습니다.",
    "us_10y_yield_falling": "미국 10년물 금리가 하락했습니다.",
    "usdkrw_rising_fx_pressure": "원/달러 환율 상승으로 환율 부담이 있을 수 있습니다.",
    "usdkrw_falling_fx_tailwind": "원/달러 환율 하락은 상대적으로 우호적일 수 있습니다.",
    "macro_environment_neutral": "거시 환경 근거는 중립입니다.",
}

ACTION_GUIDE_KO = {
    "buy_candidate": (
        "조건은 좋지만 자동 매수 신호가 아닙니다. 뉴스, 공시, 유동성을 직접 확인하세요."
    ),
    "watch": "관찰 후보입니다. 바로 매수보다 다음 실행에서 조건이 유지되는지 보는 단계입니다.",
    "hold": "뚜렷한 우위가 약합니다. 새로 진입하기보다 추가 근거를 기다리는 쪽입니다.",
    "avoid": "현재 규칙 기준으로 신규 진입 매력이 낮습니다.",
    "blocked": "리스크 필터가 먼저 작동했습니다. 매수 검토 대상에서 제외하는 쪽입니다.",
}


@dataclass(frozen=True)
class AnalysisItem:
    label: str
    value: str
    interpretation: str


@dataclass(frozen=True)
class SingleStockAnalysis:
    summary: list[AnalysisItem]
    score_breakdown: list[AnalysisItem]
    price_metrics: list[AnalysisItem]
    evidence: list[AnalysisItem]
    beginner_notes: list[str]


def format_single_stock_analysis_message(
    analysis: SingleStockAnalysis,
    *,
    signal_path: Any,
    report_path: Any,
) -> str:
    """Format the full single-stock analysis as Telegram-friendly plain text."""
    lines = ["KRX Alpha 단일 종목 상세 분석", ""]
    lines.extend(_format_message_section("1. 한눈에 보는 결론", analysis.summary))
    lines.extend(_format_message_section("2. 점수 분해", analysis.score_breakdown))
    lines.extend(_format_message_section("3. 가격/기술 지표", analysis.price_metrics))
    lines.extend(_format_message_section("4. 판단 근거", analysis.evidence))
    lines.extend(["초보자용 해석 메모"])
    lines.extend([f"- {note}" for note in analysis.beginner_notes])
    lines.extend(
        [
            "",
            "생성 파일",
            f"- 시그널 파일: {signal_path}",
            f"- 리포트 파일: {report_path}",
            "",
            "모드",
            "- 분석 전용입니다. 실제 주문은 보내지 않았습니다.",
        ]
    )
    return "\n".join(lines)


def _format_message_section(title: str, items: list[AnalysisItem]) -> list[str]:
    lines = [title]
    for item in items:
        lines.append(f"- {item.label}: {item.value}")
        if item.interpretation:
            lines.append(f"  해석: {item.interpretation}")
    lines.append("")
    return lines


def build_single_stock_analysis(
    *,
    stock_label: str,
    query: str,
    matched_by: str,
    source: str,
    start_date: str,
    end_date: str,
    score_frame: Any,
    feature_frame: Any,
    signal_frame: Any,
    data_quality_warning_count: int,
    data_quality_fail_count: int,
) -> SingleStockAnalysis:
    scores = score_frame.copy().sort_values(["ticker", "date"])
    features = feature_frame.copy().sort_values(["ticker", "date"])
    signals = signal_frame.copy().sort_values(["ticker", "date"])

    latest_score = scores.iloc[-1]
    latest_signal = signals.iloc[-1]
    latest_feature = _matching_latest_feature(features, latest_signal)

    action = str(latest_signal.get("final_action", ""))
    regime = str(latest_signal.get("market_regime", "unknown"))
    risk_blocked = bool(latest_signal.get("risk_blocked", False))
    risk_flags = str(latest_signal.get("risk_flags", "") or "").strip()

    summary = [
        AnalysisItem("입력값", query, "사용자가 입력한 회사명 또는 종목코드입니다."),
        AnalysisItem("분석 종목", stock_label, "실제로 분석에 사용한 종목입니다."),
        AnalysisItem(
            "검색 방식",
            f"{label_with_raw(matched_by, MATCHED_BY_KO)}, {label_with_raw(source, SOURCE_KO)}",
            "회사명으로 찾았는지, 종목코드로 찾았는지 보여줍니다.",
        ),
        AnalysisItem(
            "분석 기간", f"{start_date} ~ {end_date}", "이 기간의 가격 데이터로 계산했습니다."
        ),
        AnalysisItem(
            "데이터 기준일",
            _format_date(latest_signal.get("date")),
            "이 날짜까지의 정보만 사용했습니다.",
        ),
        AnalysisItem(
            "최신 판단", label_with_raw(action, ACTION_KO), ACTION_GUIDE_KO.get(action, "")
        ),
        AnalysisItem(
            "신뢰도 점수",
            _format_number(latest_signal.get("confidence_score")),
            _score_interpretation(latest_signal.get("confidence_score")),
        ),
        AnalysisItem(
            "시장 국면",
            label_with_raw(regime, REGIME_KO),
            _regime_interpretation(regime),
        ),
        AnalysisItem(
            "추천 비중",
            _format_percent_from_pct(latest_signal.get("suggested_position_pct")),
            "실제 주문 비중이 아니라 검토용 참고값입니다.",
        ),
        AnalysisItem(
            "리스크 차단",
            "예" if risk_blocked else "아니오",
            risk_flags if risk_flags else "현재 최종 리스크 차단 조건은 없습니다.",
        ),
        AnalysisItem(
            "데이터 품질",
            f"경고 {data_quality_warning_count}개, 실패 {data_quality_fail_count}개",
            "실패가 있으면 해당 분석은 보수적으로 해석해야 합니다.",
        ),
    ]

    score_breakdown = _build_score_breakdown(latest_score)
    price_metrics = _build_price_metrics(latest_feature)
    evidence = _build_evidence(latest_score, latest_signal)
    beginner_notes = _build_beginner_notes(latest_score, latest_feature, latest_signal)

    return SingleStockAnalysis(
        summary=summary,
        score_breakdown=score_breakdown,
        price_metrics=price_metrics,
        evidence=evidence,
        beginner_notes=beginner_notes,
    )


def label_with_raw(value: str, labels: dict[str, str]) -> str:
    label = labels.get(value)
    return f"{label} ({value})" if label is not None else value


def _matching_latest_feature(features: pd.DataFrame, latest_signal: pd.Series) -> pd.Series:
    ticker = str(latest_signal.get("ticker", "")).zfill(6)
    date_value = pd.Timestamp(latest_signal.get("date")).date()
    matched = features[
        (features["ticker"].astype(str).str.zfill(6) == ticker)
        & (pd.to_datetime(features["date"]).dt.date == date_value)
    ]
    return matched.iloc[-1] if not matched.empty else features.iloc[-1]


def _build_score_breakdown(latest_score: pd.Series) -> list[AnalysisItem]:
    score_items = [
        ("종합 점수", "total_score", "모든 근거를 가중 평균한 최종 점수입니다."),
        ("기술적 점수", "technical_score", "이동평균, RSI, 거래대금 변화 중심의 점수입니다."),
        ("리스크 점수", "risk_score", "높을수록 변동성 부담이 낮다는 뜻입니다."),
        ("재무 점수", "financial_score", "실적, 이익률, 부채비율 등 재무 근거입니다."),
        ("공시/이벤트 점수", "event_score", "공시 이벤트가 긍정/부정인지 반영합니다."),
        ("수급 점수", "flow_score", "외국인/기관 수급 근거입니다."),
        ("뉴스 점수", "news_score", "뉴스 감성 및 뉴스량 근거입니다."),
        ("거시환경 점수", "macro_score", "금리, 환율 등 외부 환경 근거입니다."),
    ]
    return [
        AnalysisItem(label, _format_number(latest_score.get(column)), interpretation)
        for label, column, interpretation in score_items
    ]


def _build_price_metrics(latest_feature: pd.Series) -> list[AnalysisItem]:
    return [
        AnalysisItem(
            "종가", _format_currency(latest_feature.get("close")), "마지막 거래일의 종가입니다."
        ),
        AnalysisItem(
            "1일 수익률",
            _format_percent(latest_feature.get("return_1d")),
            "전일 대비 등락률입니다.",
        ),
        AnalysisItem(
            "5일선 대비",
            _format_percent(latest_feature.get("close_to_ma_5")),
            "양수면 단기 평균보다 가격이 위에 있습니다.",
        ),
        AnalysisItem(
            "20일선 대비",
            _format_percent(latest_feature.get("close_to_ma_20")),
            "양수면 중기 평균보다 가격이 위에 있습니다.",
        ),
        AnalysisItem(
            "RSI 14",
            _format_number(latest_feature.get("rsi_14")),
            _rsi_interpretation(latest_feature.get("rsi_14")),
        ),
        AnalysisItem(
            "거래대금",
            _format_currency(latest_feature.get("trading_value")),
            "가격과 거래량을 함께 본 유동성 지표입니다.",
        ),
        AnalysisItem(
            "거래대금 5일 변화",
            _format_percent(latest_feature.get("trading_value_change_5d")),
            "최근 관심도 증가 여부를 보는 지표입니다.",
        ),
        AnalysisItem(
            "5일 변동성",
            _format_percent(latest_feature.get("volatility_5d")),
            "짧은 기간 가격 흔들림입니다.",
        ),
        AnalysisItem(
            "20일 변동성",
            _format_percent(latest_feature.get("volatility_20d")),
            "중기 가격 흔들림입니다.",
        ),
    ]


def _build_evidence(latest_score: pd.Series, latest_signal: pd.Series) -> list[AnalysisItem]:
    return [
        AnalysisItem(
            "기술적 판단 이유",
            _translate_reasons(latest_score.get("score_reason")),
            "가격/거래대금 기반 근거입니다.",
        ),
        AnalysisItem(
            "재무 근거",
            _translate_reasons(latest_score.get("financial_reason")),
            "연결된 DART 재무 데이터 기준입니다.",
        ),
        AnalysisItem(
            "공시/이벤트 근거",
            _translate_reasons(latest_score.get("event_reason")),
            "공시 이벤트의 위험/기회 요약입니다.",
        ),
        AnalysisItem(
            "수급 근거",
            _translate_reasons(latest_score.get("flow_reason")),
            "외국인/기관 수급 데이터 기준입니다.",
        ),
        AnalysisItem(
            "뉴스 근거",
            _translate_reasons(latest_score.get("news_reason")),
            "뉴스 감성 데이터 기준입니다.",
        ),
        AnalysisItem(
            "거시환경 근거",
            _translate_reasons(latest_score.get("macro_reason")),
            "금리/환율 등 매크로 데이터 기준입니다.",
        ),
        AnalysisItem(
            "최종 시그널 이유",
            _translate_reasons(latest_signal.get("signal_reason")),
            "리스크 필터까지 거친 최종 이유입니다.",
        ),
    ]


def _build_beginner_notes(
    latest_score: pd.Series,
    latest_feature: pd.Series,
    latest_signal: pd.Series,
) -> list[str]:
    notes = [
        "이 결과는 투자 판단 보조용입니다. 실제 매수/매도는 사용자가 직접 확인해야 합니다.",
        "한 번의 실행 결과보다 며칠 동안 같은 방향의 신호가 유지되는지가 더 중요합니다.",
    ]

    action = str(latest_signal.get("final_action", ""))
    action_note = ACTION_GUIDE_KO.get(action)
    if action_note:
        notes.append(action_note)

    if str(latest_signal.get("market_regime", "")) == "insufficient_data":
        notes.append(
            "시장 국면이 데이터 부족이면 120~180일 이상의 기간으로 다시 보는 것이 좋습니다."
        )
    if bool(latest_signal.get("risk_blocked", False)):
        notes.append("리스크 차단이 예라면 점수가 높아도 우선 매수 후보에서 제외하는 설계입니다.")
    if _safe_float(latest_feature.get("volatility_5d")) > 0.04:
        notes.append(
            "5일 변동성이 높은 편이라 진입 가격과 손절 기준을 더 보수적으로 잡아야 합니다."
        )
    if _safe_float(latest_feature.get("rsi_14")) > 70:
        notes.append("RSI가 높은 구간은 단기 과열 가능성이 있어 추격 매수를 조심해야 합니다.")
    if _safe_float(latest_feature.get("rsi_14")) < 30:
        notes.append("RSI가 낮은 구간은 낙폭이 크지만 추세 훼손 가능성도 함께 봐야 합니다.")
    if _has_neutral_external_scores(latest_score):
        notes.append(
            "재무/수급/뉴스/거시 점수가 50점이면 "
            "해당 데이터가 연결되지 않은 중립 처리일 수 있습니다."
        )

    return notes


def _has_neutral_external_scores(latest_score: pd.Series) -> bool:
    reason_columns = [
        "financial_reason",
        "event_reason",
        "flow_reason",
        "news_reason",
        "macro_reason",
    ]
    return any(str(latest_score.get(column, "")).startswith("no_") for column in reason_columns)


def _translate_reasons(value: Any) -> str:
    reason_text = str(value or "").strip()
    if not reason_text or reason_text == "nan":
        reason_text = "insufficient_or_neutral_evidence"
    reason_codes = [reason.strip() for reason in reason_text.split(",") if reason.strip()]
    translated = [REASON_KO.get(reason, reason) for reason in reason_codes]
    return " / ".join(translated)


def _score_interpretation(value: Any) -> str:
    score = _safe_float(value)
    if score >= 70:
        return "강한 후보권입니다. 그래도 사람의 최종 확인이 필요합니다."
    if score >= 60:
        return "관찰할 만한 구간입니다."
    if score >= 50:
        return "중립에 가까운 구간입니다."
    return "현재 기준으로는 우위가 약한 구간입니다."


def _regime_interpretation(value: str) -> str:
    if value == "bull":
        return "시장 환경이 비교적 우호적입니다."
    if value == "bear":
        return "시장 환경이 불리하므로 보수적으로 봅니다."
    if value == "high_volatility":
        return "변동성이 높아 포지션을 줄이는 쪽이 안전합니다."
    if value == "insufficient_data":
        return "국면 판단에 필요한 데이터가 부족합니다."
    return "시장 국면은 중립적으로 해석합니다."


def _rsi_interpretation(value: Any) -> str:
    rsi = _safe_float(value)
    if pd.isna(value):
        return "RSI 계산에 필요한 데이터가 부족합니다."
    if rsi >= 70:
        return "과열 가능성이 있는 구간입니다."
    if rsi <= 30:
        return "과매도 가능성이 있지만 하락 추세도 확인해야 합니다."
    if 45 <= rsi <= 60:
        return "중립에서 회복 쪽으로 볼 수 있는 구간입니다."
    return "강한 방향성은 아직 제한적입니다."


def _format_date(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return str(pd.Timestamp(value).strftime("%Y-%m-%d"))


def _format_number(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.2f}"


def _format_percent(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value) * 100:.2f}%"


def _format_percent_from_pct(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):.2f}%"


def _format_currency(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.0f}원"


def _safe_float(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    return float(value)
