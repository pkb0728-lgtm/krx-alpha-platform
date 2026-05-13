import pandas as pd
from krx_alpha.reports.universe_report import UniverseReportGenerator


def test_universe_report_generator_creates_markdown() -> None:
    summary_frame = pd.DataFrame(
        {
            "ticker": ["005380", "005930", "000000"],
            "status": ["success", "success", "failed"],
            "latest_action": ["buy_candidate", "watch", ""],
            "latest_confidence_score": [72.83, 63.78, 0.0],
            "signal_path": ["signal1.parquet", "signal2.parquet", ""],
            "report_path": ["report1.md", "report2.md", ""],
            "error": ["", "", "mock failure"],
        }
    )

    report = UniverseReportGenerator().generate(
        summary_frame,
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    assert "# Universe Screening Report" in report
    assert "| 1 | 005380 | buy_candidate | 72.83 | report1.md |" in report
    assert "| 000000 | mock failure |" in report
    assert "This report is a screening aid" in report
