from pathlib import Path

import plotly.express as px
import streamlit as st

from krx_alpha.dashboard.data_loader import (
    action_counts,
    find_latest_universe_summary,
    load_markdown,
    load_universe_summary,
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

    metric_cols = st.columns(4)
    metric_cols[0].metric("Tickers", len(summary_frame))
    metric_cols[1].metric("Success", success_count)
    metric_cols[2].metric("Failed", failed_count)
    metric_cols[3].metric(
        "Top candidate",
        str(top_row["ticker"]) if top_row is not None else "N/A",
        str(top_row["latest_action"]) if top_row is not None else None,
    )

    st.divider()

    left_col, right_col = st.columns([2, 1])
    with left_col:
        st.subheader("Universe Ranking")
        display_columns = [
            "ticker",
            "status",
            "latest_action",
            "latest_confidence_score",
            "error",
        ]
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

    st.subheader("Report Viewer")
    successful = summary_frame[summary_frame["status"] == "success"]
    if successful.empty:
        st.info("No successful reports.")
        return

    selected_ticker = st.selectbox("Ticker", successful["ticker"].tolist())
    selected_row = successful[successful["ticker"] == selected_ticker].iloc[0]
    report_path = Path(str(selected_row["report_path"]))
    st.markdown(load_markdown(report_path))


if __name__ == "__main__":
    main()
