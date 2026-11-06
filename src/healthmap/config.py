from datetime import date
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    start_date: date = date(2026, 1, 1)
    data_dir: Path = Path("./data")

    strava_client_id: str = ""
    strava_client_secret: str = ""
    strava_refresh_token: str = ""

    garmin_api_key: str = ""
    garmin_min_interval: float = 1.0

    # Días recientes que se re-descargan siempre (Garmin recalcula datos de días cercanos)
    refresh_recent_days: int = 3

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def tokens_dir(self) -> Path:
        return self.data_dir / "tokens"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "healthmap.duckdb"


settings = Settings()
