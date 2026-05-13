import pandas as pd

from krx_alpha.dashboard.data_loader import (
    action_counts,
    find_latest_universe_summary,
    load_markdown,
    load_universe_summary,
)


def test_dashboard_data_loader_reads_latest_summary(tmp_path) -> None:
    summary_dir = tmp_path / "data" / "signals" / "universe_summary_daily"
    summary_dir.mkdir(parents=True)
    summary_path = summary_dir / "universe_20240101_20240131.parquet"
    pd.DataFrame(
        {
            "ticker": ["005930", "005380"],
            "status": ["success", "success"],
            "latest_action": ["watch", "buy_candidate"],
            "latest_confidence_score": [63.0, 72.0],
            "error": ["", ""],
        }
    ).to_parquet(summary_path, index=False)

    latest_path = find_latest_universe_summary(tmp_path)
    assert latest_path == summary_path

    frame = load_universe_summary(summary_path)
    assert frame.loc[0, "ticker"] == "005380"

    counts = action_counts(frame)
    assert set(counts["latest_action"]) == {"watch", "buy_candidate"}


def test_dashboard_load_markdown(tmp_path) -> None:
    report_path = tmp_path / "report.md"
    report_path.write_text("# Report", encoding="utf-8")

    assert load_markdown(report_path) == "# Report"
