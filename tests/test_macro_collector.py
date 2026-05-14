import pandas as pd

from krx_alpha.collectors.macro_collector import FredMacroCollector, MacroRequest


def test_fred_macro_collector_normalizes_provider_payload() -> None:
    def provider(
        series_id: str,
        start_date: str,
        end_date: str,
        api_key: str,
    ) -> dict[str, object]:
        assert start_date == "2024-01-02"
        assert end_date == "2024-01-03"
        assert api_key == "fred-key"
        return {
            "observations": [
                {"date": "2024-01-02", "value": "4.10"},
                {"date": "2024-01-03", "value": "4.12"},
            ]
        }

    request = MacroRequest.from_strings(
        start_date="2024-01-02",
        end_date="2024-01-03",
        series_ids="DGS10,DEXKOUS",
        demo=False,
    )

    frame = FredMacroCollector(api_key="fred-key", provider=provider).collect(request)

    assert len(frame) == 4
    assert set(frame["series_id"]) == {"DGS10", "DEXKOUS"}
    assert pd.api.types.is_float_dtype(frame["value"])
    assert frame["source"].eq("fred").all()


def test_fred_macro_collector_demo_mode_creates_rows() -> None:
    request = MacroRequest.from_strings(
        start_date="2024-01-01",
        end_date="2024-01-31",
        series_ids="DGS10,DFF,DEXKOUS",
    )

    frame = FredMacroCollector().collect(request)

    assert not frame.empty
    assert set(frame["series_id"]) == {"DGS10", "DFF", "DEXKOUS"}
    assert frame["source"].eq("fred_demo").all()
