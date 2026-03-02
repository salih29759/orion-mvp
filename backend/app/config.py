from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        protected_namespaces=("settings_",),
    )

    api_key: str = "orion-dev-key-2024"
    database_url: str = "sqlite:///./orion.db"
    allowed_origins: str = "http://localhost:3000,http://localhost:3001"
    model_version: str = "orion-climate-v2.1"
    confidence_score: float = 0.942
    default_data_source: str = "open-meteo"
    firms_map_key: str | None = None
    firms_source: str = "VIIRS_SNPP_NRT"
    firms_day_range: int = 2
    wildfire_radius_km: int = 75
    cron_secret: str | None = None
    daily_backfill_days: int = 2
    cdsapi_url: str = "https://cds.climate.copernicus.eu/api"
    cdsapi_key: str | None = None
    cds_dataset: str = "reanalysis-era5-single-levels"
    cds_variable: str = "2m_temperature"
    cds_area_north: float = 42.5
    cds_area_west: float = 25.0
    cds_area_south: float = 35.5
    cds_area_east: float = 45.0
    era5_gcs_bucket: str | None = None
    era5_max_concurrent_jobs: int = 1
    app_env: str = "development"


settings = Settings()
