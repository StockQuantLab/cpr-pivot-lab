"""Settings — loaded from Doppler env vars. Never use .env files."""

from __future__ import annotations

from typing import Any

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")

    # Ollama LLM (cloud or local)
    ollama_model: str = Field(default="llama3.2")
    ollama_base_url: str = Field(default="http://localhost:11434")
    ollama_api_key: str | None = Field(default=None)

    # PostgreSQL — operational data ONLY (sessions, signals, alerts)
    db_host: str = Field(default="127.0.0.1")
    db_port: int = Field(default=5433)
    db_user: str = Field(default="postgres")
    postgres_password: str = Field(default="")
    db_name: str = Field(default="cpr_pivot")
    db_pool_size: int = Field(default=10)
    db_max_overflow: int = Field(default=20)
    db_pool_recycle_sec: int = Field(default=3600)

    # DuckDB / Parquet paths (relative to project root)
    duckdb_path: str = Field(default="data/market.duckdb")
    parquet_dir: str = Field(default="data/parquet")
    raw_data_dir: str = Field(default="data/raw")

    # Paper trading runtime controls
    paper_trading_enabled: bool = Field(default=False)
    paper_default_strategy: str = Field(default="CPR_LEVELS")
    paper_default_symbols: str | None = Field(default=None)
    paper_max_daily_loss_pct: float = Field(default=0.03)
    paper_max_drawdown_pct: float = Field(default=0.10)
    paper_max_positions: int = Field(default=10)
    paper_max_position_pct: float = Field(default=0.10)
    paper_stale_feed_timeout_sec: int = Field(default=120)
    paper_flatten_time: str = Field(default="15:15:00")
    paper_live_poll_interval_sec: float = Field(default=1.0)
    paper_candle_interval_minutes: int = Field(default=5)
    paper_live_quote_batch_size: int = Field(default=500)
    feed_audit_retention_days: int = Field(
        default=7,
        validation_alias=AliasChoices(
            "feed_audit_retention_days",
            "paper_feed_audit_retention_days",
        ),
    )

    # Telegram alerts (optional — Doppler: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS)
    telegram_bot_token: str | None = Field(default=None)
    telegram_chat_ids: str | None = Field(default=None)  # comma-separated chat IDs

    # Email alerts (optional — Doppler: SMTP_USER, SMTP_PASSWORD, ALERT_TO_EMAIL)
    smtp_host: str = Field(default="smtp.gmail.com")
    smtp_port: int = Field(default=587)
    smtp_user: str | None = Field(default=None)
    smtp_password: str | None = Field(default=None)
    alert_to_email: str | None = Field(default=None)

    # Alert event toggles
    alert_on_trade_open: bool = Field(default=True)
    alert_on_trade_close: bool = Field(default=True)
    alert_on_sl_hit: bool = Field(default=True)
    alert_on_target_hit: bool = Field(default=True)
    alert_on_session_start: bool = Field(default=True)
    alert_on_session_complete: bool = Field(default=True)
    alert_on_risk_limit: bool = Field(default=True)
    alert_on_daily_summary: bool = Field(default=True)

    # Kite live data (optional)
    kite_api_key: str | None = Field(default=None)
    kite_api_secret: str | None = Field(default=None)
    kite_access_token: str | None = Field(default=None)

    def get_pg_url(self) -> URL:
        """Async SQLAlchemy URL (psycopg driver) — for db/postgres.py."""
        return URL.create(
            drivername="postgresql+psycopg",
            username=self.db_user,
            password=self.postgres_password,
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
        )

    def get_pg_sync_url(self, *, mask_password: bool = True) -> str:
        """Sync URL — for Phidata PgAgentStorage and direct psycopg connections.

        Args:
            mask_password:
                When True (default), returns a redacted DSN for safer diagnostics.
                Pass False only at the point of use where a real connection is needed.
        """
        return URL.create(
            drivername="postgresql",
            username=self.db_user,
            password=self.postgres_password,
            host=self.db_host,
            port=self.db_port,
            database=self.db_name,
        ).render_as_string(hide_password=mask_password)

    def get_safe_dict(self) -> dict[str, Any]:
        """Return config as a redacted dictionary for logging and telemetry."""
        values = self.model_dump()
        for key in (
            "postgres_password",
            "kite_api_secret",
            "kite_access_token",
            "smtp_password",
            "ollama_api_key",
            "telegram_bot_token",
        ):
            if values.get(key):
                values[key] = "***"
        return values


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
