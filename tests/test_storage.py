from pathlib import Path

from krx_alpha.database.storage import DATA_LAYERS, ensure_project_dirs


def test_ensure_project_dirs(tmp_path: Path) -> None:
    ensure_project_dirs(tmp_path)

    for layer in DATA_LAYERS:
        assert (tmp_path / "data" / layer).is_dir()
