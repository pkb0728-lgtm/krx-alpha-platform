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

    summary_path = find_latest_universe_summary(PROJECT_ROOT)
    if summary_path is None:
        st.warning("No universe summary found.")
        return

    summary_frame = load_universe_summary(summary_path)
    success_count = int((summary_frame["status"] == "success").sum())
    failed_count = int((summary_frame["status"] == "failed").sum())
    top_row = summary_frame.iloc[0] if not summary_frame.empty else None

    metric_cols = st.columns(6)
    metric_cols[0].metric("Tickers", len(summary_frame))
    metric_cols[1].metric("Success", success_count)
    metric_cols[2].metric("Failed", failed_count)
    metric_cols[3].metric(
        "Top candidate",
        str(top_row["ticker"]) if top_row is not None else "N/A",
        str(top_row["latest_action"]) if top_row is not None else None,
    )
    top_regime = (
        str(top_row["latest_market_regime"])
        if top_row is not None and "latest_market_regime" in summary_frame.columns
        else "N/A"
    )
    metric_cols[4].metric("Top regime", top_regime)
    top_news = (
        _format_score(top_row["latest_news_score"])
        if top_row is not None and "latest_news_score" in summary_frame.columns
        else "N/A"
    )
    metric_cols[5].metric("Top news", top_news)

    st.divider()

    left_col, right_col = st.columns([2, 1])
    with left_col:
        st.subheader("Universe Ranking")
        display_columns = [
            "ticker",
            "status",
            "latest_action",
            "latest_confidence_score",
            "latest_news_score",
            "latest_macro_score",
            "latest_market_regime",
            "error",
        ]
        display_columns = [column for column in display_columns if column in summary_frame.columns]
        st.dataframe(
            summary_frame[display_columns],
            hide_index=True,
            use_container_width=True,
        )

    with right_col:
        st.subheader("Actions")
        counts = action_counts(summary_frame)
        if counts.empty:
            st.info("No action data.")
        else:
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

    st.subheader("Auto Screener")
    screening_path = find_latest_screening_result(PROJECT_ROOT)
    if screening_path is None:
        st.info("No screening result found.")
    else:
        screening_frame = load_screening_result(screening_path)
        if screening_frame.empty:
            st.info("Screening result is empty.")
        else:
            passed_frame = screening_frame[screening_frame["passed"]]
            top_screen = screening_frame.iloc[0]
            screen_cols = st.columns(5)
            screen_cols[0].metric("Checked", len(screening_frame))
            screen_cols[1].metric("Passed", len(passed_frame))
            screen_cols[2].metric("Top ticker", str(top_screen["ticker"]))
            screen_cols[3].metric("Top score", _format_score(top_screen["screen_score"]))
            screen_cols[4].metric("Top action", str(top_screen["final_action"]))
            st.caption(f"Latest screening file: {screening_path.name}")
            st.caption(
                f"Priority summary: {_format_count_summary(screening_frame, 'review_priority')}"
            )
            st.caption(
                f"Status summary: {_format_count_summary(screening_frame, 'screen_status_reason')}"
            )
            filter_cols = st.columns(3)
            passed_only = filter_cols[0].checkbox("Passed only", value=False)
            selected_priorities = filter_cols[1].multiselect(
                "Priority",
                _sorted_unique_values(screening_frame, "review_priority"),
            )
            selected_statuses = filter_cols[2].multiselect(
                "Status",
                _sorted_unique_values(screening_frame, "screen_status_reason"),
            )
            display_screening_frame = filter_screening_result(
                screening_frame,
                priorities=selected_priorities,
                status_reasons=selected_statuses,
                passed_only=passed_only,
            )
            display_passed_frame = display_screening_frame[display_screening_frame["passed"]]
            st.caption(f"Displayed rows: {len(display_screening_frame)}")
            if not display_passed_frame.empty:
                st.caption("Candidate review cards")
                for _, row in display_passed_frame.head(5).iterrows():
                    with st.expander(
                        f"{row['ticker']} | {row.get('review_priority', 'N/A')} | "
                        f"{row['final_action']} | "
                        f"score {_format_score(row['screen_score'])}",
                    ):
                        st.write(f"Evidence: {row.get('evidence_summary', 'N/A')}")
                        st.write(f"Caution: {row.get('caution_summary', 'N/A')}")
                        st.write(f"Risk flags: {row.get('risk_flags', 'none') or 'none'}")
                        st.write(f"Checklist: {row.get('review_checklist', 'N/A')}")
            review_queue_frame = screening_review_queue(display_screening_frame)
            if not review_queue_frame.empty:
                st.caption("Review queue")
                for _, row in review_queue_frame.iterrows():
                    with st.expander(
                        f"{row['ticker']} | {row.get('review_priority', 'N/A')} | "
                        f"{row.get('screen_status_reason', 'N/A')}",
                    ):
                        st.write(f"Score: {_format_score(row.get('screen_score', 0.0))}")
                        st.write(f"Confidence: {_format_score(row.get('confidence_score', 0.0))}")
                        st.write(f"Caution: {row.get('caution_summary', 'N/A')}")
                        st.write(f"Checklist: {row.get('review_checklist', 'N/A')}")
            if display_screening_frame.empty:
                st.info("No screening rows match the selected filters.")
            else:
                st.dataframe(
                    display_screening_frame[_screening_display_columns(display_screening_frame)],
                    hide_index=True,
                    use_container_width=True,
                )

    st.divider()

    st.subheader("KIS Paper Review Candidates")
    kis_candidate_path = find_latest_kis_paper_candidates(PROJECT_ROOT)
    if kis_candidate_path is None:
        st.info("No KIS paper candidate result found.")
    else:
        kis_candidate_frame = load_kis_paper_candidates(kis_candidate_path)
        if kis_candidate_frame.empty:
            st.info("KIS paper candidate result is empty.")
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
            candidate_cols[0].metric("Candidates", len(kis_candidate_frame))
            candidate_cols[1].metric("Review buy/add", len(review_frame))
            candidate_cols[2].metric("Manual price", len(manual_frame))
            candidate_cols[3].metric(
                "Est. amount",
                f"{float(review_frame['estimated_amount'].sum()):,.0f}"
                if "estimated_amount" in review_frame.columns
                else "0",
            )
            candidate_cols[4].metric("Orders sent", order_count)
            st.caption(f"Latest KIS candidate file: {kis_candidate_path.name}")

            available_actions = _sorted_unique_values(kis_candidate_frame, "candidate_action")
            default_actions = [action for action in review_actions if action in available_actions]
            action_filter = st.multiselect(
                "Candidate action",
                available_actions,
                default=default_actions,
            )
            display_candidate_frame = kis_candidate_frame
            if action_filter:
                display_candidate_frame = display_candidate_frame[
                    display_candidate_frame["candidate_action"].astype(str).isin(action_filter)
                ]

            if not review_frame.empty:
                st.caption("Review candidates")
                for _, row in review_frame.head(5).iterrows():
                    with st.expander(
                        f"{row['ticker']} | {row['candidate_action']} | "
                        f"{int(row.get('estimated_quantity', 0))} shares | "
                        f"{float(row.get('estimated_amount', 0.0)):,.0f}",
                    ):
                        st.write(f"Reason: {row.get('reason', 'N/A')}")
                        st.write(f"Evidence: {row.get('evidence_summary', 'N/A')}")
                        st.write(f"Caution: {row.get('caution_summary', 'N/A')}")
                        st.write(f"Risk flags: {row.get('risk_flags', 'none') or 'none'}")

            if display_candidate_frame.empty:
                st.info("No KIS candidate rows match the selected filters.")
            else:
                st.dataframe(
                    display_candidate_frame[
                        _kis_candidate_display_columns(display_candidate_frame)
                    ],
                    hide_index=True,
                    use_container_width=True,
                )

    st.divider()

    st.subheader("News Sentiment")
    news_path = find_latest_news_sentiment(PROJECT_ROOT)
    if news_path is None:
        st.info("No news sentiment feature found.")
    else:
        news_frame = load_news_sentiment(news_path)
        if news_frame.empty:
            st.info("News sentiment feature is empty.")
        else:
            latest_news = news_frame.iloc[0]
            average_score = float(news_frame["news_score"].mean())
            negative_count = int(news_frame["negative_news_count"].sum())
            positive_count = int(news_frame["positive_news_count"].sum())
            news_cols = st.columns(5)
            news_cols[0].metric("Ticker", str(latest_news["ticker"]))
            news_cols[1].metric("Latest score", _format_score(latest_news["news_score"]))
            news_cols[2].metric("Average score", _format_score(average_score))
            news_cols[3].metric("Positive", positive_count)
            news_cols[4].metric("Negative", negative_count)
            st.caption(f"Latest news sentiment file: {news_path.name}")

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
                yaxis_title="News score",
            )
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                news_frame[_news_sentiment_display_columns(news_frame)],
                hide_index=True,
                use_container_width=True,
            )

    st.divider()

    st.subheader("Macro Environment")
    macro_path = find_latest_macro_features(PROJECT_ROOT)
    if macro_path is None:
        st.info("No macro feature found.")
    else:
        macro_frame = load_macro_features(macro_path)
        if macro_frame.empty:
            st.info("Macro feature is empty.")
        else:
            latest_macro = macro_frame.iloc[0]
            macro_cols = st.columns(5)
            macro_cols[0].metric("Macro score", _format_score(latest_macro["macro_score"]))
            macro_cols[1].metric("US 10Y", _format_optional(latest_macro["us_10y_yield"]))
            macro_cols[2].metric("Fed funds", _format_optional(latest_macro["fed_funds_rate"]))
            macro_cols[3].metric("USD/KRW", _format_optional(latest_macro["usdkrw"]))
            macro_cols[4].metric("5d FX", _format_percent(latest_macro["usdkrw_change_pct_5d"]))
            st.caption(f"Latest macro feature file: {macro_path.name}")

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
                yaxis_title="Macro score",
            )
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                macro_frame[_macro_feature_display_columns(macro_frame)],
                hide_index=True,
                use_container_width=True,
            )

    st.divider()

    st.subheader("Backtest Summary")
    metrics_path = find_latest_backtest_metrics(PROJECT_ROOT)
    if metrics_path is None:
        st.info("No backtest metrics found.")
    else:
        metrics_frame = load_backtest_metrics(metrics_path)
        if metrics_frame.empty:
            st.info("Backtest metrics are empty.")
        else:
            metric = metrics_frame.iloc[0]
            backtest_cols = st.columns(6)
            backtest_cols[0].metric("Ticker", str(metric["ticker"]))
            backtest_cols[1].metric("Trades", int(metric["trade_count"]))
            backtest_cols[2].metric("Win rate", _format_percent(metric["win_rate"]))
            backtest_cols[3].metric(
                "Cumulative return",
                _format_percent(metric["cumulative_return"]),
            )
            backtest_cols[4].metric("MDD", _format_percent(metric["max_drawdown"]))
            backtest_cols[5].metric("Sharpe", f"{float(metric['sharpe_ratio']):.2f}")

            st.caption(f"Latest metrics file: {metrics_path.name}")
            st.dataframe(
                metrics_frame,
                hide_index=True,
                use_container_width=True,
            )

            trades_frame = load_backtest_trades(metrics_path)
            if not trades_frame.empty:
                st.subheader("Backtest Trades")
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
                    trades_frame[display_trade_columns],
                    hide_index=True,
                    use_container_width=True,
                )

    st.divider()

    st.subheader("Paper Portfolio")
    portfolio_path = find_latest_paper_portfolio_summary(PROJECT_ROOT)
    if portfolio_path is None:
        st.info("No paper portfolio summary found.")
    else:
        portfolio_summary = load_paper_portfolio_summary(portfolio_path)
        if portfolio_summary.empty:
            st.info("Paper portfolio summary is empty.")
        else:
            portfolio_metric = portfolio_summary.iloc[0]
            portfolio_cols = st.columns(6)
            portfolio_cols[0].metric("Universe", str(portfolio_metric["universe"]))
            portfolio_cols[1].metric(
                "Tickers",
                f"{int(portfolio_metric['loaded_ticker_count'])}/"
                f"{int(portfolio_metric['requested_ticker_count'])}",
            )
            portfolio_cols[2].metric("Trades", int(portfolio_metric["trade_count"]))
            portfolio_cols[3].metric(
                "Ending equity",
                _format_money(portfolio_metric["ending_equity"]),
            )
            portfolio_cols[4].metric(
                "Paper return",
                _format_percent(portfolio_metric["cumulative_return"]),
            )
            portfolio_cols[5].metric(
                "Exposure",
                f"{float(portfolio_metric['gross_exposure_pct']):.2f}%",
            )

            st.caption(f"Latest paper portfolio file: {portfolio_path.name}")
            st.dataframe(
                portfolio_summary[_paper_portfolio_summary_display_columns(portfolio_summary)],
                hide_index=True,
                use_container_width=True,
            )

            portfolio_trades = load_paper_portfolio_trades(portfolio_path)
            if not portfolio_trades.empty:
                st.subheader("Paper Portfolio Ledger")
                st.dataframe(
                    portfolio_trades[_paper_trade_display_columns(portfolio_trades)],
                    hide_index=True,
                    use_container_width=True,
                )

    portfolio_history = load_paper_portfolio_history(PROJECT_ROOT)
    if not portfolio_history.empty:
        st.subheader("Paper Portfolio History")
        latest_history = portfolio_history.iloc[-1]
        history_cols = st.columns(5)
        history_cols[0].metric("Runs", int(latest_history["run_sequence"]))
        history_cols[1].metric(
            "Latest equity",
            _format_money(latest_history["ending_equity"]),
        )
        history_cols[2].metric(
            "Latest return",
            _format_percent(latest_history["cumulative_return"]),
        )
        history_cols[3].metric(
            "Drawdown",
            _format_percent(latest_history["drawdown"]),
        )
        history_cols[4].metric(
            "Total trades",
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
            yaxis_title="Ending equity",
        )
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(
            portfolio_history[_paper_portfolio_history_display_columns(portfolio_history)],
            hide_index=True,
            use_container_width=True,
        )

    st.divider()

    st.subheader("Single-Stock Paper Trading")
    paper_path = find_latest_paper_summary(PROJECT_ROOT)
    if paper_path is None:
        st.info("No paper trading summary found.")
    else:
        paper_summary = load_paper_summary(paper_path)
        if paper_summary.empty:
            st.info("Paper trading summary is empty.")
        else:
            paper_metric = paper_summary.iloc[0]
            paper_cols = st.columns(6)
            paper_cols[0].metric("Ticker", str(paper_metric["ticker"]))
            paper_cols[1].metric("Trades", int(paper_metric["trade_count"]))
            paper_cols[2].metric("Ending equity", _format_money(paper_metric["ending_equity"]))
            paper_cols[3].metric(
                "Paper return",
                _format_percent(paper_metric["cumulative_return"]),
            )
            paper_cols[4].metric("Realized PnL", _format_money(paper_metric["realized_pnl"]))
            paper_cols[5].metric(
                "Unrealized PnL",
                _format_money(paper_metric["unrealized_pnl"]),
            )

            st.caption(f"Latest paper summary file: {paper_path.name}")
            st.dataframe(
                paper_summary,
                hide_index=True,
                use_container_width=True,
            )

            paper_trades = load_paper_trades(paper_path)
            if not paper_trades.empty:
                st.subheader("Paper Trade Ledger")
                st.dataframe(
                    paper_trades[_paper_trade_display_columns(paper_trades)],
                    hide_index=True,
                    use_container_width=True,
                )

    st.divider()

    st.subheader("Walk-Forward Validation")
    walk_forward_path = find_latest_walk_forward_summary(PROJECT_ROOT)
    if walk_forward_path is None:
        st.info("No walk-forward summary found.")
    else:
        walk_forward_summary = load_walk_forward_summary(walk_forward_path)
        if walk_forward_summary.empty:
            st.info("Walk-forward summary is empty.")
        else:
            walk_forward_metric = walk_forward_summary.iloc[0]
            walk_forward_cols = st.columns(6)
            walk_forward_cols[0].metric("Ticker", str(walk_forward_metric["ticker"]))
            walk_forward_cols[1].metric("Folds", int(walk_forward_metric["fold_count"]))
            walk_forward_cols[2].metric(
                "Trades",
                int(walk_forward_metric["total_trade_count"]),
            )
            walk_forward_cols[3].metric(
                "Compounded return",
                _format_percent(walk_forward_metric["compounded_return"]),
            )
            walk_forward_cols[4].metric(
                "Worst MDD",
                _format_percent(walk_forward_metric["worst_max_drawdown"]),
            )
            walk_forward_cols[5].metric(
                "Positive folds",
                _format_percent(walk_forward_metric["positive_fold_ratio"]),
            )

            st.caption(f"Latest walk-forward file: {walk_forward_path.name}")
            st.dataframe(
                walk_forward_summary,
                hide_index=True,
                use_container_width=True,
            )

            folds_frame = load_walk_forward_folds(walk_forward_path)
            if not folds_frame.empty:
                st.subheader("Walk-Forward Folds")
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
                    folds_frame[display_fold_columns],
                    hide_index=True,
                    use_container_width=True,
                )

    st.divider()

    st.subheader("ML Probability Baseline")
    ml_metrics_path = find_latest_ml_metrics(PROJECT_ROOT)
    if ml_metrics_path is None:
        st.info("No ML baseline metrics found.")
    else:
        ml_metrics_frame = load_ml_metrics(ml_metrics_path)
        if ml_metrics_frame.empty:
            st.info("ML baseline metrics are empty.")
        else:
            ml_metric = _select_ml_metric(ml_metrics_frame)
            ml_cols = st.columns(6)
            ml_cols[0].metric("Split", str(ml_metric["split"]))
            ml_cols[1].metric("Rows", int(ml_metric["row_count"]))
            ml_cols[2].metric("ROC-AUC", f"{float(ml_metric['roc_auc']):.3f}")
            ml_cols[3].metric("F1-score", f"{float(ml_metric['f1_score']):.3f}")
            ml_cols[4].metric("Precision", _format_percent(ml_metric["precision"]))
            ml_cols[5].metric("Recall", _format_percent(ml_metric["recall"]))

            st.caption(f"Latest ML metrics file: {ml_metrics_path.name}")
            st.dataframe(
                ml_metrics_frame,
                hide_index=True,
                use_container_width=True,
            )

            ml_predictions_frame = load_ml_predictions(ml_metrics_path)
            if not ml_predictions_frame.empty:
                st.subheader("ML Predictions")
                st.dataframe(
                    ml_predictions_frame[_ml_prediction_display_columns(ml_predictions_frame)],
                    hide_index=True,
                    use_container_width=True,
                )

    st.divider()

    st.subheader("Drift Monitoring")
    drift_path = find_latest_drift_result(PROJECT_ROOT)
    if drift_path is None:
        st.info("No drift result found.")
    else:
        drift_frame = load_drift_result(drift_path)
        if drift_frame.empty:
            st.info("Drift result is empty.")
        else:
            drift_count = int(drift_frame["drift_detected"].sum())
            drift_cols = st.columns(4)
            drift_cols[0].metric("Latest file", drift_path.stem)
            drift_cols[1].metric("Rows", len(drift_frame))
            drift_cols[2].metric("Drifted", drift_count)
            drift_cols[3].metric("Status", "alert" if drift_count else "stable")
            st.caption(f"Latest drift file: {drift_path.name}")
            st.dataframe(
                drift_frame[_drift_display_columns(drift_frame)],
                hide_index=True,
                use_container_width=True,
            )

    st.divider()

    st.subheader("API Health")
    api_health_path = find_latest_api_health(PROJECT_ROOT)
    if api_health_path is None:
        st.info("No API health result found.")
    else:
        api_health_frame = load_api_health(api_health_path)
        if api_health_frame.empty:
            st.info("API health result is empty.")
        else:
            status_values = api_health_frame["status"].astype(str)
            api_cols = st.columns(5)
            api_cols[0].metric("Latest file", api_health_path.stem)
            api_cols[1].metric("Checks", len(api_health_frame))
            api_cols[2].metric("OK", int((status_values == "OK").sum()))
            api_cols[3].metric("Missing", int((status_values == "MISSING").sum()))
            api_cols[4].metric("Failed", int((status_values == "FAILED").sum()))
            st.caption(f"Latest API health file: {api_health_path.name}")
            st.dataframe(
                api_health_frame[_api_health_display_columns(api_health_frame)],
                hide_index=True,
                use_container_width=True,
            )

    st.divider()

    st.subheader("Operations Health")
    health_path = find_latest_operations_health(PROJECT_ROOT)
    if health_path is None:
        st.info("No operations health result found.")
    else:
        health_frame = load_operations_health(health_path)
        if health_frame.empty:
            st.info("Operations health result is empty.")
        else:
            status_values = health_frame["status"].astype(str)
            warning_count = int(status_values.isin(["WARN", "STALE"]).sum())
            problem_count = int(status_values.isin(["MISSING", "EMPTY", "FAILED"]).sum())
            health_cols = st.columns(5)
            health_cols[0].metric("Latest file", health_path.stem)
            health_cols[1].metric("Checks", len(health_frame))
            health_cols[2].metric("OK", int((status_values == "OK").sum()))
            health_cols[3].metric("Warnings", warning_count)
            health_cols[4].metric("Problems", problem_count)
            st.caption(f"Latest operations health file: {health_path.name}")
            st.dataframe(
                health_frame[_operations_health_display_columns(health_frame)],
                hide_index=True,
                use_container_width=True,
            )

    st.divider()

    st.subheader("Report Viewer")
    successful = summary_frame[summary_frame["status"] == "success"]
    if successful.empty:
        st.info("No successful reports.")
        return

    selected_ticker = st.selectbox("Ticker", successful["ticker"].tolist())
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
        "status",
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
        "status",
        "detail",
        "action",
    ]
    return [column for column in preferred_columns if column in frame.columns]


def _screening_display_columns(frame: Any) -> list[str]:
    preferred_columns = [
        "ticker",
        "passed",
        "screen_status_reason",
        "review_priority",
        "screen_score",
        "final_action",
        "confidence_score",
        "market_regime",
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
        "candidate_action",
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
        "final_action",
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
        "split",
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
        "side",
        "status",
        "shares",
        "execution_price",
        "gross_amount",
        "fees",
        "realized_pnl",
        "equity_after",
        "signal_action",
        "reason",
    ]
    return [column for column in preferred_columns if column in frame.columns]


if __name__ == "__main__":
    main()
