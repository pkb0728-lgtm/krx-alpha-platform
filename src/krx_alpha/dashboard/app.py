from pathlib import Path
from typing import Any

import plotly.express as px
import streamlit as st

from krx_alpha.dashboard.data_loader import (
    action_counts,
    find_latest_backtest_metrics,
    find_latest_drift_result,
    find_latest_ml_metrics,
    find_latest_universe_summary,
    find_latest_walk_forward_summary,
    load_backtest_metrics,
    load_backtest_trades,
    load_drift_result,
    load_markdown,
    load_ml_metrics,
    load_ml_predictions,
    load_universe_summary,
    load_walk_forward_folds,
    load_walk_forward_summary,
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

    metric_cols = st.columns(5)
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

    st.divider()

    left_col, right_col = st.columns([2, 1])
    with left_col:
        st.subheader("Universe Ranking")
        display_columns = [
            "ticker",
            "status",
            "latest_action",
            "latest_confidence_score",
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
    return f"{float(value) * 100:.2f}%"


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


if __name__ == "__main__":
    main()
