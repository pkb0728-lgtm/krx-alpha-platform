from krx_alpha.configs.settings import Settings


def test_settings_defaults() -> None:
    settings = Settings()

    assert settings.environment == "local"
    assert settings.data_dir.name == "data"
