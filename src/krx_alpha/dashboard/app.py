from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import streamlit as st

from krx_alpha.dashboard.data_loader import (
    action_counts,
    filter_screening_result,
    find_latest_api_health,
    find_latest_backtest_metrics,
    find_latest_drift_result,
    find_latest_kis_paper_candidates,
    find_latest_macro_features,
    find_latest_ml_metrics,
    find_latest_news_sentiment,
    find_latest_operations_health,
    find_latest_paper_portfolio_summary,
    find_latest_paper_summary,
    find_latest_screening_result,
    find_latest_universe_summary,
    find_latest_walk_forward_summary,
    load_api_health,
    load_backtest_metrics,
    load_backtest_trades,
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
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def main() -> None:
    st.set_page_config(
        page_title="KRX Alpha Platform",
        page_icon="",
        layout="wide",
    )

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
    success_count = int((summary_frame["status"] == "success").sum())
    failed_count = int((summary_frame["status"] == "failed").sum())
    top_row = summary_frame.iloc[0] if not summary_frame.empty else None

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

    st.divider()

    left_col, right_col = st.columns([2, 1])
    with left_col:
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
            use_container_width=True,
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
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    st.subheader("자동 스크리너")
    st.caption("전체 종목 중 사람이 다시 검토할 만한 후보를 걸러내는 단계입니다.")
    screening_path = find_latest_screening_result(PROJECT_ROOT)
    if screening_path is None:
        st.info("스크리너 결과가 없습니다.")
    else:
        screening_frame = load_screening_result(screening_path)
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
            if not display_passed_frame.empty:
                st.caption("검토 후보 카드")
                for _, row in display_passed_frame.head(5).iterrows():
                    with st.expander(
                        f"{_stock_label(row)} | {row.get('review_priority_ko', 'N/A')} | "
                        f"{row.get('final_action_ko', row['final_action'])} | "
                        f"점수 {_format_score(row['screen_score'])}",
                    ):
                        st.write(f"근거: {row.get('evidence_summary', 'N/A')}")
                        st.write(f"주의점: {row.get('caution_summary', 'N/A')}")
                        st.write(f"리스크 표시: {row.get('risk_flags', 'none') or 'none'}")
                        st.write(f"확인할 것: {row.get('review_checklist', 'N/A')}")
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
                        st.write(f"점수: {_format_score(row.get('screen_score', 0.0))}")
                        st.write(f"신뢰도: {_format_score(row.get('confidence_score', 0.0))}")
                        st.write(f"주의점: {row.get('caution_summary', 'N/A')}")
                        st.write(f"확인할 것: {row.get('review_checklist', 'N/A')}")
            if display_screening_frame.empty:
                st.info("선택한 조건에 맞는 스크리너 행이 없습니다.")
            else:
                st.dataframe(
                    _koreanize_columns(
                        display_screening_frame[_screening_display_columns(display_screening_frame)]
                    ),
                    hide_index=True,
                    use_container_width=True,
                )

    st.divider()

    st.subheader("KIS 모의투자 검토 후보")
    st.caption(
        "모의투자 계좌 잔고를 기준으로 매수/추가매수 검토 후보를 계산합니다. "
        "실제 주문은 보내지 않습니다."
    )
    kis_candidate_path = find_latest_kis_paper_candidates(PROJECT_ROOT)
    if kis_candidate_path is None:
        st.info("KIS 모의투자 후보 결과가 없습니다.")
    else:
        kis_candidate_frame = load_kis_paper_candidates(kis_candidate_path)
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
                        st.write(f"이유: {row.get('reason', 'N/A')}")
                        st.write(f"근거: {row.get('evidence_summary', 'N/A')}")
                        st.write(f"주의점: {row.get('caution_summary', 'N/A')}")
                        st.write(f"리스크 표시: {row.get('risk_flags', 'none') or 'none'}")

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
                    use_container_width=True,
                )

    st.divider()

    st.subheader("뉴스 감성 분석")
    st.caption("뉴스 제목과 요약을 바탕으로 긍정/부정 분위기를 점수화한 결과입니다.")
    news_path = find_latest_news_sentiment(PROJECT_ROOT)
    if news_path is None:
        st.info("뉴스 감성 분석 결과가 없습니다.")
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
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                _koreanize_columns(news_frame[_news_sentiment_display_columns(news_frame)]),
                hide_index=True,
                use_container_width=True,
            )

    st.divider()

    st.subheader("거시 환경")
    st.caption("미국 금리와 환율 같은 큰 시장 환경이 우호적인지 확인하는 영역입니다.")
    macro_path = find_latest_macro_features(PROJECT_ROOT)
    if macro_path is None:
        st.info("거시 환경 피처가 없습니다.")
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
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                _koreanize_columns(macro_frame[_macro_feature_display_columns(macro_frame)]),
                hide_index=True,
                use_container_width=True,
            )

    st.divider()

    st.subheader("백테스트 요약")
    st.caption("과거 데이터에서 이 신호가 어떻게 작동했는지 비용과 슬리피지를 반영해 확인합니다.")
    metrics_path = find_latest_backtest_metrics(PROJECT_ROOT)
    if metrics_path is None:
        st.info("백테스트 결과가 없습니다.")
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
            st.dataframe(
                _koreanize_columns(metrics_frame),
                hide_index=True,
                use_container_width=True,
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
                    use_container_width=True,
                )

    st.divider()

    st.subheader("페이퍼 포트폴리오")
    st.caption("실제 돈을 쓰지 않고 가상 현금으로 신호를 검증한 결과입니다.")
    portfolio_path = find_latest_paper_portfolio_summary(PROJECT_ROOT)
    if portfolio_path is None:
        st.info("페이퍼 포트폴리오 결과가 없습니다.")
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
            st.dataframe(
                _koreanize_columns(
                    portfolio_summary[_paper_portfolio_summary_display_columns(portfolio_summary)]
                ),
                hide_index=True,
                use_container_width=True,
            )

            portfolio_trades = load_paper_portfolio_trades(portfolio_path)
            if not portfolio_trades.empty:
                st.subheader("페이퍼 포트폴리오 거래장")
                st.dataframe(
                    _koreanize_columns(
                        portfolio_trades[_paper_trade_display_columns(portfolio_trades)]
                    ),
                    hide_index=True,
                    use_container_width=True,
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
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            _koreanize_columns(
                portfolio_history[_paper_portfolio_history_display_columns(portfolio_history)]
            ),
            hide_index=True,
            use_container_width=True,
        )

    st.divider()

    st.subheader("단일 종목 페이퍼트레이딩")
    paper_path = find_latest_paper_summary(PROJECT_ROOT)
    if paper_path is None:
        st.info("단일 종목 페이퍼트레이딩 결과가 없습니다.")
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
            st.dataframe(
                _koreanize_columns(paper_summary),
                hide_index=True,
                use_container_width=True,
            )

            paper_trades = load_paper_trades(paper_path)
            if not paper_trades.empty:
                st.subheader("페이퍼트레이딩 거래장")
                st.dataframe(
                    _koreanize_columns(paper_trades[_paper_trade_display_columns(paper_trades)]),
                    hide_index=True,
                    use_container_width=True,
                )

    st.divider()

    st.subheader("워크포워드 검증")
    st.caption("기간을 여러 구간으로 나눠서 신호가 특정 기간에만 잘 맞는지 확인합니다.")
    walk_forward_path = find_latest_walk_forward_summary(PROJECT_ROOT)
    if walk_forward_path is None:
        st.info("워크포워드 검증 결과가 없습니다.")
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
            st.dataframe(
                _koreanize_columns(walk_forward_summary),
                hide_index=True,
                use_container_width=True,
            )

            folds_frame = load_walk_forward_folds(walk_forward_path)
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
                    use_container_width=True,
                )

    st.divider()

    st.subheader("ML 확률 베이스라인")
    st.caption("미래 수익률이 양수일 확률을 단순 모델로 추정한 실험 결과입니다.")
    ml_metrics_path = find_latest_ml_metrics(PROJECT_ROOT)
    if ml_metrics_path is None:
        st.info("ML 베이스라인 결과가 없습니다.")
    else:
        ml_metrics_frame = load_ml_metrics(ml_metrics_path)
        if ml_metrics_frame.empty:
            st.info("ML 베이스라인 결과가 비어 있습니다.")
        else:
            ml_metric = _select_ml_metric(ml_metrics_frame)
            ml_cols = st.columns(6)
            ml_cols[0].metric("데이터 구분", str(ml_metric["split"]))
            ml_cols[1].metric("행 수", int(ml_metric["row_count"]))
            ml_cols[2].metric("ROC-AUC", f"{float(ml_metric['roc_auc']):.3f}")
            ml_cols[3].metric("F1-score", f"{float(ml_metric['f1_score']):.3f}")
            ml_cols[4].metric("정밀도", _format_percent(ml_metric["precision"]))
            ml_cols[5].metric("재현율", _format_percent(ml_metric["recall"]))

            st.caption(f"최근 ML 결과 파일: {ml_metrics_path.name}")
            st.dataframe(
                _koreanize_columns(ml_metrics_frame),
                hide_index=True,
                use_container_width=True,
            )

            ml_predictions_frame = load_ml_predictions(ml_metrics_path)
            if not ml_predictions_frame.empty:
                st.subheader("ML 예측 결과")
                st.dataframe(
                    _koreanize_columns(
                        ml_predictions_frame[_ml_prediction_display_columns(ml_predictions_frame)]
                    ),
                    hide_index=True,
                    use_container_width=True,
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
                use_container_width=True,
            )

    st.divider()

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
                use_container_width=True,
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
                use_container_width=True,
            )

    st.divider()

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
    "suggested_position_pct": "제안 비중(%)",
    "trading_value": "거래대금",
    "trading_value_change_5d": "5일 거래대금 변화",
    "rsi_14": "RSI 14",
    "volatility_5d": "5일 변동성",
    "reasons": "근거 태그",
    "evidence_summary": "긍정 근거",
    "caution_summary": "주의점",
    "review_checklist": "확인 항목",
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
    "probability_positive_forward_return": "상승 확률",
    "predicted_label": "예측 라벨",
    "target_positive_forward_return": "실제 라벨",
    "forward_return": "미래 수익률",
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
    return frame.rename(columns=KOREAN_COLUMN_LABELS)


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
        "risk_flags",
        "suggested_position_pct",
        "trading_value",
        "trading_value_change_5d",
        "rsi_14",
        "volatility_5d",
        "reasons",
        "evidence_summary",
        "caution_summary",
        "review_checklist",
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
        "reason",
        "risk_flags",
        "orders_sent",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _format_count_summary(frame: pd.DataFrame, column: str) -> str:
    if frame.empty or column not in frame.columns:
        return "N/A"
    counts = frame[column].fillna("unknown").astype(str).value_counts()
    return ", ".join(f"{name} {count}" for name, count in counts.items())


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
    preferred_columns = [
        "date",
        "ticker",
        "stock_name",
        "split_ko",
        "probability_positive_forward_return",
        "predicted_label",
        "target_positive_forward_return",
        "forward_return",
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
