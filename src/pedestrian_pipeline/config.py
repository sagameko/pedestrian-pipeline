"""Runtime configuration, resolved from environment variables or a local .env file."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

MELBOURNE_TZ = "Australia/Melbourne"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PEDESTRIAN_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_base_url: str = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
    counts_dataset: str = "pedestrian-counting-system-past-hour-counts-per-minute"
    sensors_dataset: str = "pedestrian-counting-system-sensor-locations"

    database_path: Path = Path("data/pedestrian.duckdb")

    request_timeout_seconds: float = Field(default=90.0, gt=0)
    max_retries: int = Field(default=4, ge=0)
    backoff_base_seconds: float = Field(default=1.0, gt=0)

    def dataset_export_url(self, dataset: str) -> str:
        return f"{self.api_base_url.rstrip('/')}/{dataset}/exports/json"
