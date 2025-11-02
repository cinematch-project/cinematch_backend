from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    frontend_origin: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
