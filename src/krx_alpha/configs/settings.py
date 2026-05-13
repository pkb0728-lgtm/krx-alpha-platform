from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = Field(default="local")
    log_level: str = Field(default="INFO")
    project_root: Path = Field(default_factory=lambda: Path.cwd())

    dart_api_key: str | None = None
    naver_client_id: str | None = None
    naver_client_secret: str | None = None
    gemini_api_key: str | None = None
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    telegram_timeout_seconds: float = 10.0
    telegram_max_retries: int = 2
    telegram_retry_sleep_seconds: float = 1.0
    kis_app_key: str | None = None
    kis_app_secret: str | None = None
    kis_account_no: str | None = None
    krx_id: str | None = None
    krx_pw: str | None = None
    fred_api_key: str | None = None
    alphavantage_api_key: str | None = None

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def logs_dir(self) -> Path:
        return self.project_root / "logs"

    @property
    def models_dir(self) -> Path:
        return self.project_root / "models"


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
