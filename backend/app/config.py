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
    app_env: str = "development"


settings = Settings()
