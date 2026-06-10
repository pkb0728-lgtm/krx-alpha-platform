from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from krx_alpha.dashboard.data_loader import (
    action_counts,
    beginner_decision_brief,
    beginner_decision_journal_brief,
    filter_screening_result,
    find_latest_api_health,
    find_latest_decision_journal_evaluation,
    find_latest_drift_result,
    find_latest_operations_health,
    find_latest_universe_summary,
    load_api_health,
    load_backtest_metrics,
    load_backtest_trades,
    load_decision_journal_evaluation,
    load_drift_result,
    load_kis_paper_candidates,
    load_macro_features,
    load_markdown,
    load_ml_metrics,
    load_ml_predictions,
    load_news_sentiment,
    load_operations_health,
    load_paper_portfolio_history,
    load_paper_portfolio_summary,
    load_paper_portfolio_trades,
    load_paper_summary,
    load_paper_trades,
    load_screening_result,
    load_universe_summary,
    load_walk_forward_folds,
    load_walk_forward_summary,
    screening_review_queue,
    summarize_decision_journal_evaluation,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def _render_dashboard_styles() -> None:
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 2rem;
            padding-bottom: 3rem;
        }
        div[data-testid="stMetric"] {
            background: #ffffff;
            border: 1px solid #e6e8eb;
            border-radius: 8px;
            padding: 0.75rem 0.85rem;
            box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
        }
        div[data-testid="stMetricLabel"] {
            color: #475569;
            font-size: 0.82rem;
        }
        div[data-testid="stMetricValue"] {
            color: #111827;
            font-size: 1.2rem;
        }
        .krx-snapshot {
            border: 1px solid #d9e2ec;
            border-radius: 8px;
            padding: 1rem 1.1rem;
            background: #f8fafc;
            margin: 0.75rem 0 1rem 0;
        }
        .krx-snapshot-title {
            font-weight: 700;
            color: #111827;
            margin-bottom: 0.35rem;
        }
        .krx-snapshot-body {
            color: #334155;
            line-height: 1.55;
        }
        .krx-section-anchor {
            height: 0.1rem;
            margin-top: 0.25rem;
        }
        h2, h3 {
            letter-spacing: 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_sidebar(summary_path: Path, active_period: tuple[str, str] | None) -> None:
    with st.sidebar:
        st.title("KRX Alpha")
        st.caption("장 마감 후 분석 결과 확인용")
        st.markdown("**현재 실행 기준**")
        st.write(f"기간: {_period_label(active_period)}")
        st.write(f"파일: `{summary_path.name}`")
        st.markdown("**화면 이동**")
        st.markdown(
            """
            - [오늘 결론](#overview)
            - [판단 성과](#performance)
            - [유니버스 순위](#universe)
            - [자동 스크리너](#screening)
            - [KIS 검토 후보](#kis)
            - [뉴스/거시](#market-data)
            - [검증 결과](#validation)
            - [운영 상태](#operations)
            - [리포트](#reports)
            """
        )


def _render_operating_snapshot(
    *,
    summary_path: Path,
    active_period: tuple[str, str] | None,
    summary_frame: pd.DataFrame,
    screening_frame: pd.DataFrame,
    kis_candidate_frame: pd.DataFrame,
    brief: dict[str, Any],
) -> None:
    success_count = int((summary_frame["status"] == "success").sum())
    failed_count = int((summary_frame["status"] == "failed").sum())
    screening_passed = (
        _truthy_count(screening_frame["passed"]) if "passed" in screening_frame.columns else 0
    )
    review_count = (
        int(kis_candidate_frame["candidate_action"].isin(["review_buy", "review_add"]).sum())
        if "candidate_action" in kis_candidate_frame.columns
        else 0
    )
    top_stock = str(brief.get("top_stock", "N/A"))
    snapshot_text = (
        f"분석 기간은 {_period_label(active_period)}이고, "
        f"{len(summary_frame)}개 종목 중 {success_count}개가 정상 처리됐습니다. "
        f"스크리너 통과는 {screening_passed}개, KIS 검토 후보는 {review_count}개입니다. "
        f"현재 상위 종목은 {top_stock}입니다."
    )
    status_text = "정상" if failed_count == 0 else f"실패 {failed_count}개 확인 필요"
    st.markdown(
        f"""
        <div class="krx-snapshot">
            <div class="krx-snapshot-title">오늘의 운영 스냅샷 · {status_text}</div>
            <div class="krx-snapshot-body">
                {snapshot_text}<br/>
                기준 파일: <code>{summary_path.name}</code>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(
        page_title="KRX Alpha Platform",
        page_icon="📈",
        layout="wide",
    )
    _render_dashboard_styles()

    st.title("KRX Alpha Platform")
    st.caption("한국 주식 데이터 기반 설명 가능한 투자 의사결정 보조 대시보드")
    with st.expander("초보자용: 이 화면은 어떻게 보면 되나요?", expanded=False):
        st.markdown(
            """
            - 이 대시보드는 **바로 매수하라는 화면이 아니라**,
              더 조사할 종목을 정리해주는 화면입니다.
            - `점수`는 0~100점에 가까운 참고 지표입니다.
              높을수록 현재 조건이 더 우호적이라는 뜻입니다.
            - `관망`은 아직 매수 후보가 아니라 계속 지켜보자는 뜻입니다.
            - `매수 검토`가 나오더라도 뉴스, 공시, 유동성, 시장 상황을 사람이 다시 확인해야 합니다.
            - `실제 주문 수`가 0이면 실제 주문이 나가지 않았다는 뜻입니다.
            """
        )
    with st.expander("초보자용: 자주 나오는 용어", expanded=False):
        st.markdown(
            """
            - `신뢰도`: 여러 조건을 합쳐서 현재 신호를 얼마나 믿을 만한지 점수화한 값입니다.
            - `스크리너`: 많은 종목 중 다시 볼 만한 후보를 먼저 걸러내는 필터입니다.
            - `시장 국면`: 상승장, 하락장, 횡보장처럼 시장 분위기를 나눈 값입니다.
            - `최대 낙폭`: 투자금이 고점 대비 얼마나 크게 빠졌는지 보는 위험 지표입니다.
            - `드리프트`: 최근 데이터나 모델 성능이 예전과 달라지는 현상입니다.
            """
        )

    summary_path = find_latest_universe_summary(PROJECT_ROOT)
    if summary_path is None:
        st.warning("유니버스 요약 결과가 없습니다. 먼저 일일 작업을 실행하세요.")
        return

    summary_frame = load_universe_summary(summary_path)
    active_period = _period_from_artifact_name(summary_path)
    _render_sidebar(summary_path, active_period)
    screening_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "signals" / "screening_daily",
        active_period,
    )
    screening_frame = (
        load_screening_result(screening_path) if screening_path is not None else pd.DataFrame()
    )
    kis_candidate_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "signals" / "kis_paper_candidates",
        active_period,
    )
    kis_candidate_frame = (
        load_kis_paper_candidates(kis_candidate_path)
        if kis_candidate_path is not None
        else pd.DataFrame()
    )
    success_count = int((summary_frame["status"] == "success").sum())
    failed_count = int((summary_frame["status"] == "failed").sum())
    top_row = summary_frame.iloc[0] if not summary_frame.empty else None

    st.markdown('<div class="krx-section-anchor" id="overview"></div>', unsafe_allow_html=True)
    metric_cols = st.columns(6)
    metric_cols[0].metric("분석 종목 수", len(summary_frame))
    metric_cols[1].metric("성공", success_count)
    metric_cols[2].metric("실패", failed_count)
    metric_cols[3].metric(
        "상위 종목",
        _stock_label(top_row) if top_row is not None else "N/A",
        str(top_row.get("latest_action_ko", top_row["latest_action"]))
        if top_row is not None
        else None,
    )
    top_regime = (
        str(top_row["latest_market_regime"])
        if top_row is not None and "latest_market_regime" in summary_frame.columns
        else "N/A"
    )

    st.caption(f"현재 대시보드 기준 파일: {summary_path.name}")
    st.caption(f"현재 분석 기간: {_period_label(active_period)}")
    st.info(
        "이 기간과 맞지 않는 거시 환경, 백테스트, 워크포워드, ML 결과는 "
        "오늘 실행 결과가 아니라 과거 검증용 산출물로 표시됩니다."
    )
    top_regime_ko = (
        str(top_row["latest_market_regime_ko"])
        if top_row is not None and "latest_market_regime_ko" in summary_frame.columns
        else top_regime
    )
    metric_cols[4].metric("시장 국면", top_regime_ko)
    top_news = (
        _format_score(top_row["latest_news_score"])
        if top_row is not None and "latest_news_score" in summary_frame.columns
        else "N/A"
    )
    metric_cols[5].metric("뉴스 점수", top_news)

    brief = beginner_decision_brief(summary_frame, screening_frame, kis_candidate_frame)
    _render_operating_snapshot(
        summary_path=summary_path,
        active_period=active_period,
        summary_frame=summary_frame,
        screening_frame=screening_frame,
        kis_candidate_frame=kis_candidate_frame,
        brief=brief,
    )
    st.subheader("오늘 결론부터 보기")
    st.info(f"**{brief['headline']}**\n\n{brief['detail']}\n\n다음 확인: {brief['next_step']}")
    brief_cols = st.columns(4)
    brief_cols[0].metric("상위 종목", str(brief["top_stock"]))
    brief_cols[1].metric("스크리너 통과", int(brief["screening_passed_count"]))
    brief_cols[2].metric("KIS 검토 후보", int(brief["review_candidate_count"]))
    brief_cols[3].metric("리스크 차단", int(brief["blocked_count"]))

    st.markdown('<div class="krx-section-anchor" id="performance"></div>', unsafe_allow_html=True)
    st.subheader("판단 성과 추적")
    st.caption(
        "매일 저장된 판단을 며칠 뒤 실제 종가와 비교합니다. "
        "이 영역은 예측이 얼마나 맞았는지 기록으로 확인하는 곳입니다."
    )
    journal_evaluation_path = find_latest_decision_journal_evaluation(PROJECT_ROOT)
    if journal_evaluation_path is None:
        st.info(
            "아직 판단 평가 결과가 없습니다. 일일 작업을 실행한 뒤 며칠 지나서 "
            "`python main.py evaluate-decision-journal --holding-days 5`를 실행하면 "
            "이곳에 실제 결과 비교가 표시됩니다."
        )
    else:
        journal_evaluation = load_decision_journal_evaluation(journal_evaluation_path)
        journal_brief = beginner_decision_journal_brief(journal_evaluation)
        st.info(
            f"**{journal_brief['headline']}**\n\n"
            f"{journal_brief['detail']}\n\n"
            f"다음 확인: {journal_brief['next_step']}"
        )
        journal_cols = st.columns(5)
        journal_cols[0].metric("전체 기록", len(journal_evaluation))
        journal_cols[1].metric("평가 완료", int(journal_brief["evaluated_count"]))
        journal_cols[2].metric("평가 대기", int(journal_brief["pending_count"]))
        has_evaluated_journal_rows = int(journal_brief["evaluated_count"]) > 0
        journal_cols[3].metric(
            "평균 실제 수익률",
            _format_percent(journal_brief["average_forward_return"])
            if has_evaluated_journal_rows
            else "N/A",
        )
        journal_cols[4].metric(
            "유리한 결과 비율",
            _format_percent(journal_brief["favorable_rate"])
            if has_evaluated_journal_rows
            else "N/A",
        )
        st.caption(f"최근 판단 평가 파일: {journal_evaluation_path.name}")

        journal_summary = summarize_decision_journal_evaluation(journal_evaluation)
        if not journal_summary.empty:
            chart_frame = journal_summary.copy()
            chart_frame["유리한 결과 비율(%)"] = chart_frame["favorable_rate"] * 100
            fig = px.bar(
                chart_frame,
                x="latest_action_ko",
                y="유리한 결과 비율(%)",
                text="evaluated_count",
                color="latest_action_ko",
            )
            fig.update_layout(
                showlegend=False,
                margin={"l": 12, "r": 12, "t": 12, "b": 12},
                xaxis_title=None,
                yaxis_title="유리한 결과 비율(%)",
            )
            st.plotly_chart(fig, width="stretch")
            st.dataframe(
                _koreanize_columns(
                    journal_summary[_decision_journal_summary_display_columns(journal_summary)]
                ),
                hide_index=True,
                width="stretch",
            )

        st.caption("최근 판단별 실제 결과")
        st.dataframe(
            _koreanize_columns(
                journal_evaluation[_decision_journal_evaluation_display_columns(journal_evaluation)]
            ),
            hide_index=True,
            width="stretch",
        )

    st.divider()

    left_col, right_col = st.columns([2, 1])
    with left_col:
        st.markdown('<div class="krx-section-anchor" id="universe"></div>', unsafe_allow_html=True)
        st.subheader("유니버스 순위")
        st.caption(
            "여러 종목을 한 번에 돌렸을 때 어떤 종목이 상대적으로 좋아 보이는지 보는 표입니다."
        )
        display_columns = [
            "ticker",
            "stock_name",
            "status_ko",
            "latest_action_ko",
            "latest_confidence_score",
            "latest_news_score",
            "latest_macro_score",
            "latest_market_regime_ko",
            "error",
        ]
        display_columns = [column for column in display_columns if column in summary_frame.columns]
        st.dataframe(
            _koreanize_columns(summary_frame[display_columns]),
            hide_index=True,
            width="stretch",
        )

    with right_col:
        st.subheader("최종 판단 분포")
        counts = action_counts(summary_frame)
        if counts.empty:
            st.info("최종 판단 데이터가 없습니다.")
        else:
            counts = counts.copy()
            counts["latest_action"] = counts["latest_action"].map(_action_ko)
            fig = px.bar(
                counts,
                x="latest_action",
                y="count",
                text="count",
                color="latest_action",
            )
            fig.update_layout(
                showlegend=False,
                margin={"l": 12, "r": 12, "t": 12, "b": 12},
                xaxis_title=None,
                yaxis_title=None,
            )
            st.plotly_chart(fig, width="stretch")

    st.divider()

    st.markdown('<div class="krx-section-anchor" id="screening"></div>', unsafe_allow_html=True)
    st.subheader("자동 스크리너")
    st.caption("전체 종목 중 사람이 다시 검토할 만한 후보를 걸러내는 단계입니다.")
    if screening_path is None:
        st.info("스크리너 결과가 없습니다.")
    else:
        if screening_frame.empty:
            st.info("스크리너 결과가 비어 있습니다.")
        else:
            passed_frame = screening_frame[screening_frame["passed"]]
            top_screen = screening_frame.iloc[0]
            screen_cols = st.columns(5)
            screen_cols[0].metric("검사 종목", len(screening_frame))
            screen_cols[1].metric("통과", len(passed_frame))
            screen_cols[2].metric("상위 종목", _stock_label(top_screen))
            screen_cols[3].metric("상위 점수", _format_score(top_screen["screen_score"]))
            screen_cols[4].metric(
                "상위 판단",
                str(top_screen.get("final_action_ko", top_screen["final_action"])),
            )
            st.caption(f"최근 스크리너 파일: {screening_path.name}")
            st.caption(
                f"우선순위 요약: {_format_count_summary(screening_frame, 'review_priority_ko')}"
            )
            st.caption(
                f"상태 요약: {_format_count_summary(screening_frame, 'screen_status_reason_ko')}"
            )
            filter_cols = st.columns(3)
            passed_only = filter_cols[0].checkbox("통과 종목만 보기", value=False)
            selected_priorities = filter_cols[1].multiselect(
                "우선순위",
                _sorted_unique_values(screening_frame, "review_priority_ko"),
            )
            selected_statuses = filter_cols[2].multiselect(
                "상태",
                _sorted_unique_values(screening_frame, "screen_status_reason_ko"),
            )
            display_screening_frame = filter_screening_result(
                screening_frame,
                priorities=_reverse_values(selected_priorities, "review_priority"),
                status_reasons=_reverse_values(selected_statuses, "screen_status"),
                passed_only=passed_only,
            )
            display_passed_frame = display_screening_frame[display_screening_frame["passed"]]
            st.caption(f"표시 행 수: {len(display_screening_frame)}")
            if passed_frame.empty:
                st.info(
                    "오늘 스크리너 통과 종목은 없습니다. 오류가 아니라 점수, 신뢰도, "
                    "리스크 기준을 모두 만족한 종목이 없다는 뜻입니다. 이 경우에는 "
                    "보류/확인 필요 종목에서 왜 탈락했는지 확인하세요."
                )
            if not display_passed_frame.empty:
                st.caption("검토 후보 카드")
                for _, row in display_passed_frame.head(5).iterrows():
                    with st.expander(
                        f"{_stock_label(row)} | {row.get('review_priority_ko', 'N/A')} | "
                        f"{row.get('final_action_ko', row['final_action'])} | "
                        f"점수 {_format_score(row['screen_score'])}",
                    ):
                        st.write(f"초보자 해석: {row.get('beginner_summary_ko', 'N/A')}")
                        st.write(f"근거: {row.get('evidence_summary_ko', 'N/A')}")
                        st.write(f"주의점: {row.get('caution_summary_ko', 'N/A')}")
                        st.write(f"리스크 설명: {row.get('risk_flags_ko', 'N/A')}")
                        st.write(f"다음 확인: {row.get('next_check_ko', 'N/A')}")
                        st.write(f"확인 항목: {row.get('review_checklist_ko', 'N/A')}")
            review_queue_frame = screening_review_queue(display_screening_frame)
            if not review_queue_frame.empty:
                st.caption("보류/확인 필요 종목")
                for _, row in review_queue_frame.iterrows():
                    status_label = row.get(
                        "screen_status_reason_ko",
                        row.get("screen_status_reason", "N/A"),
                    )
                    with st.expander(
                        f"{_stock_label(row)} | {row.get('review_priority_ko', 'N/A')} | "
                        f"{status_label}",
                    ):
                        st.write(f"초보자 해석: {row.get('beginner_summary_ko', 'N/A')}")
                        st.write(f"점수: {_format_score(row.get('screen_score', 0.0))}")
                        st.write(f"신뢰도: {_format_score(row.get('confidence_score', 0.0))}")
                        st.write(f"근거: {row.get('evidence_summary_ko', 'N/A')}")
                        st.write(f"주의점: {row.get('caution_summary_ko', 'N/A')}")
                        st.write(f"리스크 설명: {row.get('risk_flags_ko', 'N/A')}")
                        st.write(f"다음 확인: {row.get('next_check_ko', 'N/A')}")
                        st.write(f"확인 항목: {row.get('review_checklist_ko', 'N/A')}")
            if display_screening_frame.empty:
                st.info("선택한 조건에 맞는 스크리너 행이 없습니다.")
            else:
                st.dataframe(
                    _koreanize_columns(
                        display_screening_frame[_screening_display_columns(display_screening_frame)]
                    ),
                    hide_index=True,
                    width="stretch",
                )

    st.divider()

    st.markdown('<div class="krx-section-anchor" id="kis"></div>', unsafe_allow_html=True)
    st.subheader("KIS 모의투자 검토 후보")
    st.caption(
        "모의투자 계좌 잔고를 기준으로 매수/추가매수 검토 후보를 계산합니다. "
        "실제 주문은 보내지 않습니다."
    )
    if kis_candidate_path is None:
        st.info("KIS 모의투자 후보 결과가 없습니다.")
    else:
        if kis_candidate_frame.empty:
            st.info("KIS 모의투자 후보 결과가 비어 있습니다.")
        else:
            review_actions = ["review_buy", "review_add"]
            review_frame = kis_candidate_frame[
                kis_candidate_frame["candidate_action"].isin(review_actions)
            ]
            manual_frame = kis_candidate_frame[
                kis_candidate_frame["candidate_action"] == "manual_price_required"
            ]
            order_count = (
                int(kis_candidate_frame["orders_sent"].sum())
                if "orders_sent" in kis_candidate_frame.columns
                else 0
            )
            candidate_cols = st.columns(5)
            candidate_cols[0].metric("후보 수", len(kis_candidate_frame))
            candidate_cols[1].metric("매수/추가 검토", len(review_frame))
            candidate_cols[2].metric("가격 확인 필요", len(manual_frame))
            candidate_cols[3].metric(
                "예상 금액",
                f"{float(review_frame['estimated_amount'].sum()):,.0f}"
                if "estimated_amount" in review_frame.columns
                else "0",
            )
            candidate_cols[4].metric("실제 주문 수", order_count)
            st.caption(f"최근 KIS 후보 파일: {kis_candidate_path.name}")
            _render_kis_account_note(kis_candidate_frame)
            if review_frame.empty:
                st.info(
                    "KIS 매수/추가매수 검토 후보가 없습니다. 스크리너를 통과한 종목이 "
                    "없거나 리스크 필터가 차단한 경우에는 예상 수량과 금액이 0으로 표시됩니다."
                )

            available_actions = _sorted_unique_values(kis_candidate_frame, "candidate_action")
            default_actions = [action for action in review_actions if action in available_actions]
            action_filter = st.multiselect(
                "후보 상태",
                [_candidate_action_ko(action) for action in available_actions],
                default=[_candidate_action_ko(action) for action in default_actions],
            )
            display_candidate_frame = kis_candidate_frame
            if action_filter:
                selected_raw_actions = _reverse_values(action_filter, "candidate_action")
                display_candidate_frame = display_candidate_frame[
                    display_candidate_frame["candidate_action"]
                    .astype(str)
                    .isin(selected_raw_actions)
                ]

            if not review_frame.empty:
                st.caption("매수/추가매수 검토 후보")
                for _, row in review_frame.head(5).iterrows():
                    action_label = row.get(
                        "candidate_action_ko",
                        row["candidate_action"],
                    )
                    with st.expander(
                        f"{_stock_label(row)} | {action_label} | "
                        f"{int(row.get('estimated_quantity', 0))}주 | "
                        f"{float(row.get('estimated_amount', 0.0)):,.0f}",
                    ):
                        st.write(f"초보자 해석: {row.get('beginner_summary_ko', 'N/A')}")
                        st.write(f"이유: {row.get('reason', 'N/A')}")
                        st.write(f"근거: {row.get('evidence_summary_ko', 'N/A')}")
                        st.write(f"주의점: {row.get('caution_summary_ko', 'N/A')}")
                        st.write(f"리스크 설명: {row.get('risk_flags_ko', 'N/A')}")
                        st.write(f"다음 확인: {row.get('next_check_ko', 'N/A')}")

            if display_candidate_frame.empty:
                st.info("선택한 조건에 맞는 KIS 후보가 없습니다.")
            else:
                st.dataframe(
                    _koreanize_columns(
                        display_candidate_frame[
                            _kis_candidate_display_columns(display_candidate_frame)
                        ]
                    ),
                    hide_index=True,
                    width="stretch",
                )

    st.divider()

    st.markdown('<div class="krx-section-anchor" id="market-data"></div>', unsafe_allow_html=True)
    st.subheader("뉴스 감성 분석")
    st.caption("뉴스 제목과 요약을 바탕으로 긍정/부정 분위기를 점수화한 결과입니다.")
    news_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "features" / "news_sentiment_daily",
        active_period,
    )
    if news_path is None:
        st.info("현재 분석 기간의 뉴스 감성 분석 결과가 없습니다.")
    else:
        news_frame = load_news_sentiment(news_path)
        if news_frame.empty:
            st.info("뉴스 감성 분석 결과가 비어 있습니다.")
        else:
            latest_news = news_frame.iloc[0]
            average_score = float(news_frame["news_score"].mean())
            negative_count = int(news_frame["negative_news_count"].sum())
            positive_count = int(news_frame["positive_news_count"].sum())
            news_cols = st.columns(5)
            news_cols[0].metric("종목", _stock_label(latest_news))
            news_cols[1].metric("최근 뉴스 점수", _format_score(latest_news["news_score"]))
            news_cols[2].metric("평균 점수", _format_score(average_score))
            news_cols[3].metric("긍정 뉴스", positive_count)
            news_cols[4].metric("부정 뉴스", negative_count)
            st.caption(f"최근 뉴스 감성 파일: {news_path.name}")

            chart_frame = news_frame.copy()
            chart_frame["date"] = chart_frame["date"].astype(str)
            fig = px.line(
                chart_frame,
                x="date",
                y="news_score",
                markers=True,
                title=None,
            )
            fig.update_layout(
                margin={"l": 12, "r": 12, "t": 12, "b": 12},
                xaxis_title=None,
                yaxis_title="뉴스 점수",
            )
            st.plotly_chart(fig, width="stretch")

            st.dataframe(
                _koreanize_columns(news_frame[_news_sentiment_display_columns(news_frame)]),
                hide_index=True,
                width="stretch",
            )

    st.divider()

    st.subheader("거시 환경")
    st.caption("미국 금리와 환율 같은 큰 시장 환경이 우호적인지 확인하는 영역입니다.")
    macro_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "features" / "macro_fred_daily",
        active_period,
    )
    if macro_path is None:
        st.info("현재 분석 기간의 거시 환경 결과가 없습니다.")
    else:
        macro_frame = load_macro_features(macro_path)
        if macro_frame.empty:
            st.info("거시 환경 피처가 비어 있습니다.")
        else:
            latest_macro = macro_frame.iloc[0]
            macro_cols = st.columns(5)
            macro_cols[0].metric("거시 점수", _format_score(latest_macro["macro_score"]))
            macro_cols[1].metric("미국 10년 금리", _format_optional(latest_macro["us_10y_yield"]))
            macro_cols[2].metric("미국 기준금리", _format_optional(latest_macro["fed_funds_rate"]))
            macro_cols[3].metric("달러/원", _format_optional(latest_macro["usdkrw"]))
            macro_cols[4].metric(
                "5일 환율 변화", _format_percent(latest_macro["usdkrw_change_pct_5d"])
            )
            st.caption(f"최근 거시 피처 파일: {macro_path.name}")
            _warn_if_period_mismatch("거시 환경", macro_path, active_period)

            chart_frame = macro_frame.copy().sort_values("date")
            chart_frame["date"] = chart_frame["date"].astype(str)
            fig = px.line(
                chart_frame,
                x="date",
                y="macro_score",
                markers=True,
                title=None,
            )
            fig.update_layout(
                margin={"l": 12, "r": 12, "t": 12, "b": 12},
                xaxis_title=None,
                yaxis_title="거시 점수",
            )
            st.plotly_chart(fig, width="stretch")

            st.dataframe(
                _koreanize_columns(macro_frame[_macro_feature_display_columns(macro_frame)]),
                hide_index=True,
                width="stretch",
            )

    st.divider()

    st.markdown('<div class="krx-section-anchor" id="validation"></div>', unsafe_allow_html=True)
    st.subheader("백테스트 요약")
    st.caption("과거 데이터에서 이 신호가 어떻게 작동했는지 비용과 슬리피지를 반영해 확인합니다.")
    metrics_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "backtest" / "metrics",
        active_period,
    )
    if metrics_path is None:
        st.info("현재 분석 기간의 백테스트 결과가 없습니다.")
    else:
        metrics_frame = load_backtest_metrics(metrics_path)
        if metrics_frame.empty:
            st.info("백테스트 결과가 비어 있습니다.")
        else:
            metric = metrics_frame.iloc[0]
            backtest_cols = st.columns(6)
            backtest_cols[0].metric("종목", _stock_label(metric))
            backtest_cols[1].metric("거래 수", int(metric["trade_count"]))
            backtest_cols[2].metric("승률", _format_percent(metric["win_rate"]))
            backtest_cols[3].metric(
                "누적 수익률",
                _format_percent(metric["cumulative_return"]),
            )
            backtest_cols[4].metric("최대 낙폭", _format_percent(metric["max_drawdown"]))
            backtest_cols[5].metric("샤프비율", f"{float(metric['sharpe_ratio']):.2f}")

            st.caption(f"최근 백테스트 파일: {metrics_path.name}")
            _warn_if_period_mismatch("백테스트", metrics_path, active_period)
            st.dataframe(
                _koreanize_columns(metrics_frame),
                hide_index=True,
                width="stretch",
            )

            trades_frame = load_backtest_trades(metrics_path)
            if not trades_frame.empty:
                st.subheader("백테스트 거래 내역")
                display_trade_columns = [
                    "ticker",
                    "signal_date",
                    "entry_date",
                    "exit_date",
                    "entry_price",
                    "exit_price",
                    "net_return",
                    "signal_confidence",
                ]
                st.dataframe(
                    _koreanize_columns(trades_frame[display_trade_columns]),
                    hide_index=True,
                    width="stretch",
                )

    st.divider()

    st.subheader("페이퍼 포트폴리오")
    st.caption("실제 돈을 쓰지 않고 가상 현금으로 신호를 검증한 결과입니다.")
    portfolio_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "backtest" / "paper_portfolio_summary",
        active_period,
    )
    if portfolio_path is None:
        st.info("현재 분석 기간의 페이퍼 포트폴리오 결과가 없습니다.")
    else:
        portfolio_summary = load_paper_portfolio_summary(portfolio_path)
        if portfolio_summary.empty:
            st.info("페이퍼 포트폴리오 결과가 비어 있습니다.")
        else:
            portfolio_metric = portfolio_summary.iloc[0]
            portfolio_cols = st.columns(6)
            portfolio_cols[0].metric("유니버스", str(portfolio_metric["universe"]))
            portfolio_cols[1].metric(
                "종목 수",
                f"{int(portfolio_metric['loaded_ticker_count'])}/"
                f"{int(portfolio_metric['requested_ticker_count'])}",
            )
            portfolio_cols[2].metric("거래 수", int(portfolio_metric["trade_count"]))
            portfolio_cols[3].metric(
                "최종 평가금액",
                _format_money(portfolio_metric["ending_equity"]),
            )
            portfolio_cols[4].metric(
                "가상 수익률",
                _format_percent(portfolio_metric["cumulative_return"]),
            )
            portfolio_cols[5].metric(
                "투자 노출",
                f"{float(portfolio_metric['gross_exposure_pct']):.2f}%",
            )

            st.caption(f"최근 페이퍼 포트폴리오 파일: {portfolio_path.name}")
            _warn_if_period_mismatch("페이퍼 포트폴리오", portfolio_path, active_period)
            portfolio_trades = load_paper_portfolio_trades(portfolio_path)
            _render_paper_portfolio_audit(portfolio_metric, portfolio_trades)
            st.dataframe(
                _koreanize_columns(
                    portfolio_summary[_paper_portfolio_summary_display_columns(portfolio_summary)]
                ),
                hide_index=True,
                width="stretch",
            )

            if not portfolio_trades.empty:
                st.subheader("페이퍼 포트폴리오 거래장")
                st.dataframe(
                    _koreanize_columns(
                        portfolio_trades[_paper_trade_display_columns(portfolio_trades)]
                    ),
                    hide_index=True,
                    width="stretch",
                )

    portfolio_history = load_paper_portfolio_history(PROJECT_ROOT)
    if not portfolio_history.empty:
        st.subheader("페이퍼 포트폴리오 실행 이력")
        latest_history = portfolio_history.iloc[-1]
        history_cols = st.columns(5)
        history_cols[0].metric("실행 횟수", int(latest_history["run_sequence"]))
        history_cols[1].metric(
            "최근 평가금액",
            _format_money(latest_history["ending_equity"]),
        )
        history_cols[2].metric(
            "최근 수익률",
            _format_percent(latest_history["cumulative_return"]),
        )
        history_cols[3].metric(
            "낙폭",
            _format_percent(latest_history["drawdown"]),
        )
        history_cols[4].metric(
            "누적 거래 수",
            int(latest_history["cumulative_trade_count"]),
        )

        chart_frame = portfolio_history.copy()
        chart_frame["generated_at"] = chart_frame["generated_at"].astype(str)
        fig = px.line(
            chart_frame,
            x="generated_at",
            y="ending_equity",
            color="universe",
            markers=True,
            title=None,
        )
        fig.update_layout(
            margin={"l": 12, "r": 12, "t": 12, "b": 12},
            xaxis_title=None,
            yaxis_title="최종 평가금액",
        )
        st.plotly_chart(fig, width="stretch")

        st.dataframe(
            _koreanize_columns(
                portfolio_history[_paper_portfolio_history_display_columns(portfolio_history)]
            ),
            hide_index=True,
            width="stretch",
        )

    st.divider()

    st.subheader("단일 종목 페이퍼트레이딩")
    paper_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "backtest" / "paper_summary",
        active_period,
    )
    if paper_path is None:
        st.info("현재 분석 기간의 단일 종목 페이퍼트레이딩 결과가 없습니다.")
    else:
        paper_summary = load_paper_summary(paper_path)
        if paper_summary.empty:
            st.info("단일 종목 페이퍼트레이딩 결과가 비어 있습니다.")
        else:
            paper_metric = paper_summary.iloc[0]
            paper_cols = st.columns(6)
            paper_cols[0].metric("종목", _stock_label(paper_metric))
            paper_cols[1].metric("거래 수", int(paper_metric["trade_count"]))
            paper_cols[2].metric("최종 평가금액", _format_money(paper_metric["ending_equity"]))
            paper_cols[3].metric(
                "가상 수익률",
                _format_percent(paper_metric["cumulative_return"]),
            )
            paper_cols[4].metric("실현 손익", _format_money(paper_metric["realized_pnl"]))
            paper_cols[5].metric(
                "평가 손익",
                _format_money(paper_metric["unrealized_pnl"]),
            )

            st.caption(f"최근 페이퍼트레이딩 파일: {paper_path.name}")
            _warn_if_period_mismatch("단일 종목 페이퍼트레이딩", paper_path, active_period)
            paper_trades = load_paper_trades(paper_path)
            _render_single_paper_audit(paper_metric, paper_trades)
            st.dataframe(
                _koreanize_columns(paper_summary),
                hide_index=True,
                width="stretch",
            )

            if not paper_trades.empty:
                st.subheader("페이퍼트레이딩 거래장")
                st.dataframe(
                    _koreanize_columns(paper_trades[_paper_trade_display_columns(paper_trades)]),
                    hide_index=True,
                    width="stretch",
                )

    st.divider()

    st.subheader("워크포워드 검증")
    st.caption("기간을 여러 구간으로 나눠서 신호가 특정 기간에만 잘 맞는지 확인합니다.")
    walk_forward_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "backtest" / "walk_forward_summary",
        active_period,
    )
    if walk_forward_path is None:
        st.info("현재 분석 기간의 워크포워드 검증 결과가 없습니다.")
    else:
        walk_forward_summary = load_walk_forward_summary(walk_forward_path)
        if walk_forward_summary.empty:
            st.info("워크포워드 검증 결과가 비어 있습니다.")
        else:
            walk_forward_metric = walk_forward_summary.iloc[0]
            walk_forward_cols = st.columns(6)
            walk_forward_cols[0].metric("종목", _stock_label(walk_forward_metric))
            walk_forward_cols[1].metric("검증 구간 수", int(walk_forward_metric["fold_count"]))
            walk_forward_cols[2].metric(
                "거래 수",
                int(walk_forward_metric["total_trade_count"]),
            )
            walk_forward_cols[3].metric(
                "복리 수익률",
                _format_percent(walk_forward_metric["compounded_return"]),
            )
            walk_forward_cols[4].metric(
                "최악 낙폭",
                _format_percent(walk_forward_metric["worst_max_drawdown"]),
            )
            walk_forward_cols[5].metric(
                "수익 구간 비율",
                _format_percent(walk_forward_metric["positive_fold_ratio"]),
            )

            st.caption(f"최근 워크포워드 파일: {walk_forward_path.name}")
            _warn_if_period_mismatch("워크포워드 검증", walk_forward_path, active_period)
            folds_frame = load_walk_forward_folds(walk_forward_path)
            _render_walk_forward_audit(walk_forward_metric, folds_frame)
            st.dataframe(
                _koreanize_columns(walk_forward_summary),
                hide_index=True,
                width="stretch",
            )

            if not folds_frame.empty:
                st.subheader("워크포워드 구간별 결과")
                display_fold_columns = [
                    "ticker",
                    "fold",
                    "train_start",
                    "train_end",
                    "test_start",
                    "test_end",
                    "signal_count",
                    "trade_count",
                    "win_rate",
                    "cumulative_return",
                    "max_drawdown",
                    "sharpe_ratio",
                ]
                display_fold_columns = [
                    column for column in display_fold_columns if column in folds_frame.columns
                ]
                st.dataframe(
                    _koreanize_columns(folds_frame[display_fold_columns]),
                    hide_index=True,
                    width="stretch",
                )

    st.divider()

    st.subheader("ML 확률 베이스라인")
    st.caption("미래 초과수익 가능성을 단순 확률 모델로 추정한 실험 결과입니다.")
    ml_metrics_path = _find_matching_period_file(
        PROJECT_ROOT / "data" / "signals" / "ml_metrics",
        active_period,
    )
    if ml_metrics_path is None:
        st.info("현재 분석 기간의 ML 베이스라인 결과가 없습니다.")
    else:
        ml_metrics_frame = load_ml_metrics(ml_metrics_path)
        if ml_metrics_frame.empty:
            st.info("ML 베이스라인 결과가 비어 있습니다.")
        else:
            ml_metric = _select_ml_metric(ml_metrics_frame)
            ml_cols = st.columns(6)
            ml_cols[0].metric(
                "데이터 구분",
                str(ml_metric.get("split_ko", ml_metric["split"])),
            )
            ml_cols[1].metric("행 수", int(ml_metric["row_count"]))
            ml_cols[2].metric("ROC-AUC", f"{float(ml_metric['roc_auc']):.3f}")
            ml_cols[3].metric("F1-score", f"{float(ml_metric['f1_score']):.3f}")
            ml_cols[4].metric("정밀도", _format_percent(ml_metric["precision"]))
            ml_cols[5].metric("재현율", _format_percent(ml_metric["recall"]))
            investment_cols = st.columns(4)
            investment_cols[0].metric(
                "초과수익 라벨 비율",
                _format_percent(ml_metric.get("excess_label_rate", 0.0)),
            )
            investment_cols[1].metric(
                "TopK 정밀도",
                _format_percent(ml_metric.get("precision_at_top_k", 0.0)),
            )
            investment_cols[2].metric(
                "TopK 평균 초과수익",
                _format_percent(ml_metric.get("top_k_average_excess_return", 0.0)),
            )
            investment_cols[3].metric(
                "선택 후보 평균수익",
                _format_percent(ml_metric.get("selected_average_forward_return", 0.0)),
            )

            st.caption(f"최근 ML 결과 파일: {ml_metrics_path.name}")
            _warn_if_period_mismatch("ML 확률 베이스라인", ml_metrics_path, active_period)
            st.dataframe(
                _koreanize_columns(ml_metrics_frame),
                hide_index=True,
                width="stretch",
            )

            ml_predictions_frame = load_ml_predictions(ml_metrics_path)
            if not ml_predictions_frame.empty:
                st.subheader("ML 예측 결과")
                st.dataframe(
                    _koreanize_columns(
                        ml_predictions_frame[_ml_prediction_display_columns(ml_predictions_frame)]
                    ),
                    hide_index=True,
                    width="stretch",
                )

    st.divider()

    st.subheader("드리프트 모니터링")
    st.caption("최근 데이터나 성능이 예전과 달라졌는지 확인합니다.")
    drift_path = find_latest_drift_result(PROJECT_ROOT)
    if drift_path is None:
        st.info("드리프트 결과가 없습니다.")
    else:
        drift_frame = load_drift_result(drift_path)
        if drift_frame.empty:
            st.info("드리프트 결과가 비어 있습니다.")
        else:
            drift_count = int(drift_frame["drift_detected"].sum())
            drift_cols = st.columns(4)
            drift_cols[0].metric("최근 파일", drift_path.stem)
            drift_cols[1].metric("검사 행", len(drift_frame))
            drift_cols[2].metric("변화 감지", drift_count)
            drift_cols[3].metric("상태", "경고" if drift_count else "안정")
            st.caption(f"최근 드리프트 파일: {drift_path.name}")
            st.dataframe(
                _koreanize_columns(drift_frame[_drift_display_columns(drift_frame)]),
                hide_index=True,
                width="stretch",
            )

    st.divider()

    st.markdown('<div class="krx-section-anchor" id="operations"></div>', unsafe_allow_html=True)
    st.subheader("API 상태")
    api_health_path = find_latest_api_health(PROJECT_ROOT)
    if api_health_path is None:
        st.info("API 상태 점검 결과가 없습니다.")
    else:
        api_health_frame = load_api_health(api_health_path)
        if api_health_frame.empty:
            st.info("API 상태 점검 결과가 비어 있습니다.")
        else:
            status_values = api_health_frame["status"].astype(str)
            api_cols = st.columns(5)
            api_cols[0].metric("최근 파일", api_health_path.stem)
            api_cols[1].metric("검사 수", len(api_health_frame))
            api_cols[2].metric("정상", int((status_values == "OK").sum()))
            api_cols[3].metric("누락", int((status_values == "MISSING").sum()))
            api_cols[4].metric("실패", int((status_values == "FAILED").sum()))
            st.caption(f"최근 API 상태 파일: {api_health_path.name}")
            st.dataframe(
                _koreanize_columns(api_health_frame[_api_health_display_columns(api_health_frame)]),
                hide_index=True,
                width="stretch",
            )

    st.divider()

    st.subheader("운영 상태")
    st.caption("필요한 산출물이 정상적으로 생성됐는지, 너무 오래되지 않았는지 확인합니다.")
    health_path = find_latest_operations_health(PROJECT_ROOT)
    if health_path is None:
        st.info("운영 상태 점검 결과가 없습니다.")
    else:
        health_frame = load_operations_health(health_path)
        if health_frame.empty:
            st.info("운영 상태 점검 결과가 비어 있습니다.")
        else:
            status_values = health_frame["status"].astype(str)
            warning_count = int(status_values.isin(["WARN", "STALE"]).sum())
            problem_count = int(status_values.isin(["MISSING", "EMPTY", "FAILED"]).sum())
            health_cols = st.columns(5)
            health_cols[0].metric("최근 파일", health_path.stem)
            health_cols[1].metric("검사 수", len(health_frame))
            health_cols[2].metric("정상", int((status_values == "OK").sum()))
            health_cols[3].metric("주의", warning_count)
            health_cols[4].metric("문제", problem_count)
            st.caption(f"최근 운영 상태 파일: {health_path.name}")
            st.dataframe(
                _koreanize_columns(health_frame[_operations_health_display_columns(health_frame)]),
                hide_index=True,
                width="stretch",
            )

    st.divider()

    st.markdown('<div class="krx-section-anchor" id="reports"></div>', unsafe_allow_html=True)
    st.subheader("리포트 보기")
    successful = summary_frame[summary_frame["status"] == "success"]
    if successful.empty:
        st.info("성공한 리포트가 없습니다.")
        return

    ticker_options = successful.apply(_stock_label, axis=1).tolist()
    selected_label = st.selectbox("종목", ticker_options)
    selected_ticker = str(successful.iloc[ticker_options.index(selected_label)]["ticker"])
    selected_row = successful[successful["ticker"] == selected_ticker].iloc[0]
    report_path = Path(str(selected_row["report_path"]))
    st.markdown(load_markdown(report_path))


def _format_percent(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value) * 100:.2f}%"


def _truthy_count(series: pd.Series) -> int:
    if pd.api.types.is_bool_dtype(series):
        return int(series.fillna(False).sum())
    return int(series.fillna(False).astype(str).str.lower().isin({"true", "1", "yes"}).sum())


def _format_score(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):.2f}"


def _format_money(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.0f}"


def _format_optional(value: Any) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.2f}"


ACTION_KO = {
    "buy_candidate": "매수 검토",
    "watch": "관망",
    "hold": "보유/중립",
    "avoid": "회피",
    "blocked": "리스크 차단",
}

CANDIDATE_ACTION_KO = {
    "review_buy": "매수 검토",
    "review_add": "추가매수 검토",
    "manual_price_required": "가격 확인 필요",
    "hold_review": "관망",
    "skip": "제외",
}

REVERSE_LABELS = {
    "review_priority": {
        "높음": "high",
        "중간": "medium",
        "관찰": "watchlist",
        "낮음": "low",
        "차단": "blocked",
    },
    "screen_status": {
        "조건 통과": "passed",
        "신뢰도 부족": "confidence_below_threshold",
        "신뢰도/점수 부족": "confidence_and_score_below_threshold",
        "점수 부족": "score_below_threshold",
        "허용되지 않은 판단": "action_not_allowed",
        "리스크 차단": "risk_blocked",
        "신호 파일 없음": "signal_file_missing_or_empty",
    },
    "candidate_action": {value: key for key, value in CANDIDATE_ACTION_KO.items()},
}

KOREAN_COLUMN_LABELS = {
    "ticker": "종목코드",
    "stock_name": "종목명",
    "status": "처리상태",
    "status_ko": "처리상태",
    "latest_action": "최신 판단",
    "latest_action_ko": "최신 판단",
    "latest_confidence_score": "신뢰도",
    "latest_news_score": "뉴스 점수",
    "latest_macro_score": "거시 점수",
    "latest_market_regime": "시장 국면",
    "latest_market_regime_ko": "시장 국면",
    "error": "오류",
    "passed": "통과",
    "screen_status_reason": "스크리너 상태",
    "screen_status_reason_ko": "스크리너 상태",
    "review_priority": "우선순위",
    "review_priority_ko": "우선순위",
    "screen_score": "스크리너 점수",
    "final_action": "최종 판단",
    "final_action_ko": "최종 판단",
    "confidence_score": "신뢰도",
    "market_regime": "시장 국면",
    "market_regime_ko": "시장 국면",
    "risk_flags": "리스크 표시",
    "risk_flags_ko": "리스크 설명",
    "beginner_summary_ko": "초보자 해석",
    "next_check_ko": "다음 확인",
    "suggested_position_pct": "제안 비중(%)",
    "trading_value": "거래대금",
    "trading_value_change_5d": "5일 거래대금 변화",
    "rsi_14": "RSI 14",
    "volatility_5d": "5일 변동성",
    "reasons": "근거 태그",
    "evidence_summary": "긍정 근거",
    "evidence_summary_ko": "긍정 근거",
    "caution_summary": "주의점",
    "caution_summary_ko": "주의점",
    "review_checklist": "확인 항목",
    "review_checklist_ko": "확인 항목",
    "candidate_action": "후보 상태",
    "candidate_action_ko": "후보 상태",
    "candidate_type": "후보 유형",
    "estimated_quantity": "예상 수량",
    "estimated_amount": "예상 금액",
    "target_position_pct": "목표 비중(%)",
    "current_quantity": "현재 수량",
    "current_value": "현재 평가금액",
    "reference_price": "기준 가격",
    "reference_price_source": "가격 출처",
    "reason": "이유",
    "orders_sent": "실제 주문 수",
    "date": "날짜",
    "execution_date": "체결일",
    "side": "매수/매도",
    "side_ko": "매수/매도",
    "shares": "수량",
    "execution_price": "체결가",
    "gross_amount": "거래금액",
    "fees": "수수료",
    "realized_pnl": "실현 손익",
    "unrealized_pnl": "평가 손익",
    "equity_after": "거래 후 평가금액",
    "signal_action": "신호 판단",
    "signal_action_ko": "신호 판단",
    "signal_date": "신호일",
    "entry_date": "진입일",
    "exit_date": "청산일",
    "entry_price": "진입가",
    "exit_price": "청산가",
    "net_return": "순수익률",
    "signal_confidence": "신호 신뢰도",
    "universe": "유니버스",
    "requested_ticker_count": "요청 종목 수",
    "loaded_ticker_count": "불러온 종목 수",
    "skipped_tickers": "건너뛴 종목",
    "ending_equity": "최종 평가금액",
    "cumulative_return": "누적 수익률",
    "drawdown": "낙폭",
    "trade_count": "거래 수",
    "cumulative_trade_count": "누적 거래 수",
    "active_position_count": "보유 포지션 수",
    "gross_exposure_pct": "투자 노출(%)",
    "cash_pct": "현금 비중(%)",
    "generated_at": "생성 시각",
    "decision_date": "판단일",
    "holding_days": "평가 보유일",
    "evaluation_date": "평가일",
    "entry_close": "기준 종가",
    "evaluation_close": "평가 종가",
    "outcome_status": "평가 상태",
    "outcome_status_ko": "평가 상태",
    "outcome_ko": "평가 결과",
    "outcome_summary_ko": "쉬운 해석",
    "favorable_outcome": "유리한 결과",
    "favorable_outcome_ko": "유리한 결과",
    "decision_count": "판단 수",
    "evaluated_count": "평가 완료",
    "pending_count": "평가 대기",
    "average_forward_return": "평균 실제 수익률",
    "positive_return_rate": "상승 비율",
    "favorable_rate": "유리한 결과 비율",
    "fold": "구간",
    "train_start": "학습 시작",
    "train_end": "학습 종료",
    "test_start": "검증 시작",
    "test_end": "검증 종료",
    "signal_count": "신호 수",
    "win_rate": "승률",
    "max_drawdown": "최대 낙폭",
    "sharpe_ratio": "샤프비율",
    "split": "데이터 구분",
    "split_ko": "데이터 구분",
    "row_count": "행 수",
    "roc_auc": "ROC-AUC",
    "f1_score": "F1-score",
    "precision": "정밀도",
    "recall": "재현율",
    "probability_target_return": "모델 확률",
    "probability_positive_forward_return": "상승 확률(호환)",
    "predicted_label": "예측 라벨",
    "target_positive_forward_return": "실제 라벨",
    "target_excess_forward_return": "초과수익 라벨",
    "forward_return": "미래 수익률",
    "benchmark_forward_return": "벤치마크 수익률",
    "excess_forward_return": "초과수익률",
    "excess_label_rate": "초과수익 라벨 비율",
    "model_target_label_rate": "모델 목표 라벨 비율",
    "selected_count": "선택 후보 수",
    "selected_average_forward_return": "선택 후보 평균수익",
    "selected_average_excess_return": "선택 후보 평균 초과수익",
    "top_k_count": "TopK 수",
    "precision_at_top_k": "TopK 정밀도",
    "top_k_average_forward_return": "TopK 평균수익",
    "top_k_average_excess_return": "TopK 평균 초과수익",
    "label_end_date": "라벨 종료일",
    "top_feature_reason": "주요 근거",
    "news_score": "뉴스 점수",
    "sentiment_score": "감성 점수",
    "news_count": "뉴스 수",
    "positive_news_count": "긍정 뉴스 수",
    "negative_news_count": "부정 뉴스 수",
    "news_reason": "뉴스 이유",
    "top_headline": "대표 제목",
    "summary": "요약",
    "source": "출처",
    "macro_score": "거시 점수",
    "macro_reason": "거시 이유",
    "us_10y_yield": "미국 10년 금리",
    "us_10y_yield_change_5d": "미국 10년 금리 5일 변화",
    "fed_funds_rate": "미국 기준금리",
    "usdkrw": "달러/원",
    "usdkrw_change_pct_5d": "달러/원 5일 변화",
    "feature": "피처",
    "run_type": "실행 유형",
    "metric": "지표",
    "baseline_mean": "기준 평균",
    "recent_mean": "최근 평균",
    "mean_shift_score": "평균 변화 점수",
    "std_ratio": "표준편차 비율",
    "missing_rate_delta": "결측률 변화",
    "drift_detected": "변화 감지",
    "drift_reason": "변화 이유",
    "checked_at": "점검 시각",
    "api": "API",
    "detail": "상세",
    "action": "조치",
    "check_name": "점검명",
    "category": "분류",
    "age_hours": "경과 시간",
    "path": "경로",
}

LOCALIZED_COLUMN_PAIRS = (
    ("status", "status_ko"),
    ("latest_action", "latest_action_ko"),
    ("final_action", "final_action_ko"),
    ("signal_action", "signal_action_ko"),
    ("candidate_action", "candidate_action_ko"),
    ("screen_status_reason", "screen_status_reason_ko"),
    ("review_priority", "review_priority_ko"),
    ("market_regime", "market_regime_ko"),
    ("latest_market_regime", "latest_market_regime_ko"),
    ("side", "side_ko"),
    ("split", "split_ko"),
)


def _stock_label(row: Any) -> str:
    if row is None:
        return "N/A"
    ticker = str(row.get("ticker", ""))
    stock_name = str(row.get("stock_name", "") or "")
    if stock_name:
        return f"{ticker} {stock_name}"
    return ticker or "N/A"


def _action_ko(value: Any) -> str:
    text = str(value)
    return ACTION_KO.get(text, text)


def _candidate_action_ko(value: Any) -> str:
    text = str(value)
    return CANDIDATE_ACTION_KO.get(text, text)


def _reverse_values(values: list[str], mapping_name: str) -> list[str]:
    mapping = REVERSE_LABELS.get(mapping_name, {})
    return [mapping.get(value, value) for value in values]


def _koreanize_columns(frame: Any) -> Any:
    display_frame = frame.copy()
    drop_columns = [
        raw_column
        for raw_column, localized_column in LOCALIZED_COLUMN_PAIRS
        if raw_column in display_frame.columns and localized_column in display_frame.columns
    ]
    if drop_columns:
        display_frame = display_frame.drop(columns=drop_columns)
    return display_frame.rename(columns=KOREAN_COLUMN_LABELS)


def _drift_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "feature",
        "run_type",
        "metric",
        "baseline_mean",
        "recent_mean",
        "mean_shift_score",
        "std_ratio",
        "missing_rate_delta",
        "drift_detected",
        "drift_reason",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _operations_health_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "check_name",
        "category",
        "status_ko",
        "row_count",
        "age_hours",
        "detail",
        "action",
        "path",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _api_health_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "checked_at",
        "api",
        "status_ko",
        "detail",
        "action",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _screening_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "ticker",
        "stock_name",
        "passed",
        "screen_status_reason_ko",
        "review_priority_ko",
        "screen_score",
        "final_action_ko",
        "confidence_score",
        "market_regime_ko",
        "beginner_summary_ko",
        "risk_flags_ko",
        "next_check_ko",
        "suggested_position_pct",
        "trading_value",
        "trading_value_change_5d",
        "rsi_14",
        "volatility_5d",
        "reasons",
        "evidence_summary_ko",
        "caution_summary_ko",
        "review_checklist_ko",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _kis_candidate_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "ticker",
        "stock_name",
        "candidate_action_ko",
        "candidate_type",
        "estimated_quantity",
        "estimated_amount",
        "target_position_pct",
        "current_quantity",
        "current_value",
        "reference_price",
        "reference_price_source",
        "confidence_score",
        "screen_score",
        "final_action_ko",
        "beginner_summary_ko",
        "reason",
        "risk_flags_ko",
        "next_check_ko",
        "orders_sent",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _decision_journal_summary_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "latest_action_ko",
        "decision_count",
        "evaluated_count",
        "pending_count",
        "average_forward_return",
        "positive_return_rate",
        "favorable_rate",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _decision_journal_evaluation_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "decision_date",
        "ticker",
        "stock_name",
        "latest_action_ko",
        "latest_confidence_score",
        "holding_days",
        "entry_date",
        "entry_close",
        "evaluation_date",
        "evaluation_close",
        "forward_return",
        "outcome_status_ko",
        "outcome_summary_ko",
        "favorable_outcome_ko",
        "latest_market_regime_ko",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _format_count_summary(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return "N/A"
    counts = frame[column].fillna("unknown").astype(str).value_counts()
    return ", ".join(f"{name} {count}" for name, count in counts.items())


def _render_kis_account_note(frame: Any) -> None:
    if frame.empty:
        return

    row = frame.iloc[0]
    total_equity = _safe_number(row.get("total_equity"))
    cash_amount = _safe_number(row.get("cash_amount"))
    account = str(row.get("account", "N/A"))
    order_count = (
        int(pd.to_numeric(frame["orders_sent"], errors="coerce").fillna(0).sum())
        if "orders_sent" in frame.columns
        else 0
    )
    if total_equity <= 0 and cash_amount <= 0:
        return

    st.info(
        f"KIS 모의계좌 {account}의 총평가금액 {_format_money(total_equity)}과 "
        f"현금 {_format_money(cash_amount)}은 증권사 모의투자 계좌 조회값입니다. "
        "페이퍼 포트폴리오의 가상 시작금액과 별개이며, 이 화면은 후보 수량 계산용입니다. "
        f"실제 주문 전송 수는 {order_count}건입니다."
    )


def _render_paper_portfolio_audit(metric: Any, trades: Any) -> None:
    if _safe_int(metric.get("trade_count")) == 0:
        st.info(
            "검산: 체결 거래가 0회라 현금이 그대로 남았습니다. "
            "최종 평가금액은 현금과 보유 주식 평가금액을 더한 값입니다."
        )
    st.dataframe(
        _paper_audit_frame(metric, trades),
        hide_index=True,
        width="stretch",
    )


def _render_single_paper_audit(metric: Any, trades: Any) -> None:
    if _safe_int(metric.get("trade_count")) == 0:
        st.info(
            "검산: 이 단일 종목에서도 매수 후보 신호가 없어 가상 체결이 없었습니다. "
            "그래서 최종 평가금액은 시작 가상 현금과 같습니다."
        )
    st.dataframe(
        _paper_audit_frame(metric, trades),
        hide_index=True,
        width="stretch",
    )


def _render_walk_forward_audit(metric: Any, folds: Any) -> None:
    if _safe_int(metric.get("total_trade_count")) == 0:
        st.info(
            "검산: 워크포워드는 테스트 구간별로 매수 후보 신호를 실제 거래로 바꿔 검증합니다. "
            "이번 기간에는 매수 후보 신호가 없어 거래 수와 수익률이 0으로 표시됩니다."
        )
    st.dataframe(
        _walk_forward_audit_frame(metric, folds),
        hide_index=True,
        width="stretch",
    )


def _paper_audit_frame(metric: Any, trades: Any) -> pd.DataFrame:
    initial_cash = _safe_number(metric.get("initial_cash"))
    ending_cash = _safe_number(metric.get("ending_cash"))
    ending_position_value = _safe_number(metric.get("ending_position_value"))
    ending_equity = _safe_number(metric.get("ending_equity"))
    calculated_equity = ending_cash + ending_position_value
    calculated_return = calculated_equity / initial_cash - 1 if initial_cash else 0.0
    filled_trade_count = _filled_trade_count(trades)

    return pd.DataFrame(
        [
            {
                "검산 항목": "시작 가상 현금",
                "값": _format_money(initial_cash),
                "확인 방법": "페이퍼트레이딩 설정값입니다.",
            },
            {
                "검산 항목": "체결 거래 수",
                "값": f"{filled_trade_count}회",
                "확인 방법": "거래장(status=filled) 행 개수입니다.",
            },
            {
                "검산 항목": "보유 주식 평가금액",
                "값": _format_money(ending_position_value),
                "확인 방법": "열린 포지션 수량 x 마지막 종가입니다.",
            },
            {
                "검산 항목": "최종 평가금액",
                "값": _format_money(ending_equity),
                "확인 방법": (
                    f"현금 {_format_money(ending_cash)} + "
                    f"보유 평가 {_format_money(ending_position_value)} = "
                    f"{_format_money(calculated_equity)}"
                ),
            },
            {
                "검산 항목": "가상 수익률",
                "값": _format_percent(calculated_return),
                "확인 방법": "최종 평가금액 / 시작 가상 현금 - 1",
            },
        ]
    )


def _walk_forward_audit_frame(metric: Any, folds: Any) -> pd.DataFrame:
    fold_count = _safe_int(metric.get("fold_count"))
    total_trade_count = _safe_int(metric.get("total_trade_count"))
    total_exposure_count = _safe_int(metric.get("total_exposure_count"))
    fold_trade_sum = _column_sum_int(folds, "trade_count")
    fold_exposure_sum = _column_sum_int(folds, "exposure_count")

    return pd.DataFrame(
        [
            {
                "검산 항목": "검증 구간 수",
                "값": f"{fold_count}개",
                "확인 방법": "구간별 결과 표의 행 개수입니다.",
            },
            {
                "검산 항목": "매수 후보 신호 수",
                "값": f"{total_exposure_count}개",
                "확인 방법": (
                    "각 테스트 구간의 exposure_count 합계입니다. "
                    f"구간별 합계: {fold_exposure_sum}개"
                ),
            },
            {
                "검산 항목": "거래 수",
                "값": f"{total_trade_count}회",
                "확인 방법": (
                    f"각 테스트 구간의 trade_count 합계입니다. 구간별 합계: {fold_trade_sum}회"
                ),
            },
            {
                "검산 항목": "복리 수익률",
                "값": _format_percent(metric.get("compounded_return")),
                "확인 방법": "구간별 수익률을 순서대로 복리 누적한 값입니다.",
            },
        ]
    )


def _filled_trade_count(trades: Any) -> int:
    if trades.empty or "status" not in trades.columns:
        return 0
    return int((trades["status"].astype(str) == "filled").sum())


def _column_sum_int(frame: Any, column: str) -> int:
    if frame.empty or column not in frame.columns:
        return 0
    return int(pd.to_numeric(frame[column], errors="coerce").fillna(0).sum())


def _safe_number(value: Any) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    return int(round(_safe_number(value)))


def _period_from_artifact_name(path: Path | None) -> tuple[str, str] | None:
    if path is None:
        return None

    compact_dates = [part for part in path.stem.split("_") if len(part) == 8 and part.isdigit()]
    if len(compact_dates) < 2:
        return None

    return (
        _format_compact_date(compact_dates[-2]),
        _format_compact_date(compact_dates[-1]),
    )


def _format_compact_date(value: str) -> str:
    return f"{value[:4]}-{value[4:6]}-{value[6:8]}"


def _period_label(period: tuple[str, str] | None) -> str:
    if period is None:
        return "파일명에서 기간을 확인할 수 없음"
    return f"{period[0]} ~ {period[1]}"


def _artifact_matches_period(path: Path | None, active_period: tuple[str, str] | None) -> bool:
    if path is None or active_period is None:
        return False
    return _period_from_artifact_name(path) == active_period


def _warn_if_period_mismatch(
    section_label: str,
    artifact_path: Path | None,
    active_period: tuple[str, str] | None,
) -> None:
    if artifact_path is None or active_period is None:
        return

    artifact_period = _period_from_artifact_name(artifact_path)
    if artifact_period is None or artifact_period == active_period:
        return

    st.warning(
        f"{section_label} 섹션은 현재 분석 기간({_period_label(active_period)})과 "
        f"다른 파일({_period_label(artifact_period)})을 보고 있습니다. "
        "이 값은 오늘 실행 결과가 아니라 과거 검증/샘플 산출물입니다."
    )


def _find_matching_period_file(
    directory: Path,
    active_period: tuple[str, str] | None,
    pattern: str = "*.parquet",
) -> Path | None:
    if active_period is None or not directory.exists():
        return None

    matching_paths = [
        path
        for path in directory.glob(pattern)
        if path.is_file() and _artifact_matches_period(path, active_period)
    ]
    if not matching_paths:
        return None

    return max(matching_paths, key=lambda path: (path.stat().st_mtime, path.name))


def _sorted_unique_values(frame: pd.DataFrame, column: str) -> list[str]:
    if frame.empty or column not in frame.columns:
        return []
    return sorted(frame[column].dropna().astype(str).unique().tolist())


def _select_ml_metric(frame: Any) -> Any:
    test_rows = frame[frame["split"] == "test"]
    if not test_rows.empty:
        return test_rows.iloc[0]
    return frame.iloc[0]


def _ml_prediction_display_columns(frame: Any) -> list[str]:
    probability_column = (
        "probability_target_return"
        if "probability_target_return" in frame.columns
        else "probability_positive_forward_return"
    )
    preferred_columns = [
        "date",
        "ticker",
        "stock_name",
        "split_ko",
        probability_column,
        "predicted_label",
        "target_positive_forward_return",
        "target_excess_forward_return",
        "forward_return",
        "excess_forward_return",
        "label_end_date",
        "top_feature_reason",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _news_sentiment_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "date",
        "ticker",
        "stock_name",
        "news_score",
        "sentiment_score",
        "news_count",
        "positive_news_count",
        "negative_news_count",
        "news_reason",
        "top_headline",
        "summary",
        "source",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _macro_feature_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "date",
        "macro_score",
        "macro_reason",
        "us_10y_yield",
        "us_10y_yield_change_5d",
        "fed_funds_rate",
        "usdkrw",
        "usdkrw_change_pct_5d",
        "source",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _paper_portfolio_summary_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "universe",
        "ticker",
        "requested_ticker_count",
        "loaded_ticker_count",
        "skipped_tickers",
        "ending_equity",
        "cumulative_return",
        "realized_pnl",
        "unrealized_pnl",
        "trade_count",
        "active_position_count",
        "gross_exposure_pct",
        "cash_pct",
        "generated_at",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _paper_portfolio_history_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "universe",
        "run_sequence",
        "summary_file",
        "generated_at",
        "ending_equity",
        "cumulative_return",
        "drawdown",
        "trade_count",
        "cumulative_trade_count",
        "active_position_count",
        "gross_exposure_pct",
        "cash_pct",
        "skipped_tickers",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _paper_trade_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "date",
        "execution_date",
        "ticker",
        "stock_name",
        "side_ko",
        "status_ko",
        "shares",
        "execution_price",
        "gross_amount",
        "fees",
        "realized_pnl",
        "equity_after",
        "signal_action_ko",
        "reason",
    ]
    return [column for column in preferred_columns if column in frame.columns]


if __name__ == "__main__":
    main()
