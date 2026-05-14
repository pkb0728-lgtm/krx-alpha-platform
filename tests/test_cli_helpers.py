from krx_alpha.cli import (
    _parse_console_filter_values,
    _screening_cli_display_columns,
    _short_console_text,
)


def test_parse_console_filter_values_trims_and_normalizes() -> None:
    assert _parse_console_filter_values(" high, medium ,,watchlist ") == {
        "high",
        "medium",
        "watchlist",
    }
    assert _parse_console_filter_values(None) == set()


def test_short_console_text_truncates_long_values() -> None:
    assert _short_console_text("short", limit=10) == "short"
    assert _short_console_text("abcdefghijklmnopqrstuvwxyz", limit=10) == "abcdefg..."


def test_screening_cli_display_columns_support_compact_mode() -> None:
    compact_columns = _screening_cli_display_columns(compact=True)
    full_columns = _screening_cli_display_columns(compact=False)

    assert "reasons" not in compact_columns
    assert "reasons" in full_columns
    assert "screen_status_reason" in compact_columns
