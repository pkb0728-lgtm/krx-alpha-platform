import pandas as pd

from krx_alpha.features.dart_disclosure_events import DartDisclosureEventBuilder


def test_dart_disclosure_event_builder_flags_risk_event() -> None:
    disclosure_frame = pd.DataFrame(
        {
            "corp_code": ["00126380"],
            "corp_name": ["Demo Company"],
            "stock_code": ["005930"],
            "report_nm": ["Capital increase decision"],
            "rcept_no": ["20240131000001"],
            "rcept_dt": ["20240131"],
            "flr_nm": ["Demo Company"],
            "rm": ["demo"],
            "source": ["opendart_demo"],
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    event_frame = DartDisclosureEventBuilder().build(disclosure_frame)

    assert len(event_frame) == 1
    assert event_frame.loc[0, "ticker"] == "005930"
    assert bool(event_frame.loc[0, "event_risk_flag"]) is True
    assert event_frame.loc[0, "event_score"] == 20.0
    assert event_frame.loc[0, "event_reason"] == "disclosure_risk_capital_increase"


def test_dart_disclosure_event_builder_scores_routine_report_neutral() -> None:
    disclosure_frame = pd.DataFrame(
        {
            "corp_code": ["00126380"],
            "corp_name": ["Demo Company"],
            "stock_code": ["005930"],
            "report_nm": ["Annual report"],
            "rcept_no": ["20240131000002"],
            "rcept_dt": ["20240131"],
            "flr_nm": ["Demo Company"],
            "rm": ["demo"],
            "source": ["opendart_demo"],
            "collected_at": [pd.Timestamp("2026-05-13T00:00:00Z")],
        }
    )

    event_frame = DartDisclosureEventBuilder().build(disclosure_frame)

    assert bool(event_frame.loc[0, "event_risk_flag"]) is False
    assert event_frame.loc[0, "event_score"] == 50.0
    assert event_frame.loc[0, "event_reason"] == "disclosure_routine_report"
