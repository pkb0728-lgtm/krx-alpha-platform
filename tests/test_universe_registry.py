import pandas as pd
import pytest

from krx_alpha.contracts.universe_contract import validate_universe_frame
from krx_alpha.database.storage import universe_csv_path, universe_file_path
from krx_alpha.universe.static_universe import UniverseRegistry


def test_universe_registry_returns_demo_universe() -> None:
    registry = UniverseRegistry()

    definition = registry.get("demo")
    frame = definition.to_frame()

    assert definition.tickers() == ["005930", "000660", "005380"]
    assert set(frame["sector"]) == {"semiconductor", "auto"}
    assert "large_cap" in registry.names()


def test_universe_registry_lists_available_universes() -> None:
    frame = UniverseRegistry().list_definitions()

    assert set(frame["universe"]) == {"demo", "large_cap", "semiconductor_auto"}
    assert (frame["count"] > 0).all()


def test_universe_registry_rejects_unknown_name() -> None:
    with pytest.raises(KeyError):
        UniverseRegistry().get("missing")


def test_validate_universe_frame_rejects_invalid_ticker() -> None:
    frame = pd.DataFrame(
        {
            "ticker": ["5930"],
            "name": ["Invalid"],
            "market": ["KOSPI"],
            "sector": ["test"],
            "reason": ["bad ticker"],
            "is_active": [True],
        }
    )

    with pytest.raises(ValueError, match="six-digit"):
        validate_universe_frame(frame)


def test_universe_storage_paths(tmp_path) -> None:
    assert (
        universe_file_path(tmp_path, "demo")
        .as_posix()
        .endswith("data/processed/universe/demo.parquet")
    )
    assert (
        universe_csv_path(tmp_path, "demo").as_posix().endswith("data/processed/universe/demo.csv")
    )
