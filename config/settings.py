# config/settings.py
from pydantic import Field, AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="forbid")

    SECRET_KEY: str
    ALGORITHM: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    EMAIL_HOST: str
    EMAIL_PORT: int
    EMAIL_USERNAME: str
    EMAIL_PASSWORD: str
    EMAIL_USE_TLS: bool = False
    EMAIL_USE_SSL: bool = True
    EMAIL_FROM_NAME: str
    EMAIL_SENDER: str

    # ✅ ADICIONE ISSO AQUI (aliases)
    SMTP_HOST: str = Field(validation_alias=AliasChoices("SMTP_HOST", "EMAIL_HOST"))
    SMTP_PORT: int = Field(validation_alias=AliasChoices("SMTP_PORT", "EMAIL_PORT"))
    SMTP_USER: str = Field(validation_alias=AliasChoices("SMTP_USER", "EMAIL_USERNAME"))
    SMTP_PASS: str = Field(validation_alias=AliasChoices("SMTP_PASS", "EMAIL_PASSWORD"))
    SMTP_FROM: str = Field(validation_alias=AliasChoices("SMTP_FROM", "EMAIL_SENDER"))

    GED_CONTA: str
    GED_USUARIO: str
    GED_SENHA: str

    ENVIRONMENT: str

    ODOO_URL: str
    ODOO_DB: str
    ODOO_USER: str
    ODOO_PASSWORD: str
    ODOO_HTTP_TIMEOUT: int = 20

    HELPDESK_TEAM_ID: int
    AUTO_TICKET_ON_CLOSE: bool = True
    odoo_livechat_close_action_id: int | None = None

settings = Settings()
