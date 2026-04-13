"""Tests for config/settings.py."""

from __future__ import annotations

from sqlalchemy.engine import URL

from config.settings import Settings


def test_get_pg_url_returns_sqlalchemy_url() -> None:
    settings = Settings(
        db_user="postgres",
        postgres_password="pa:ss@word/with+chars",
        db_host="127.0.0.1",
        db_port=5433,
        db_name="cpr_pivot",
    )
    pg_url = settings.get_pg_url()
    assert isinstance(pg_url, URL)
    assert pg_url.drivername == "postgresql+psycopg"
    # Safe rendering should mask secret in logs/diagnostics.
    assert "***" in pg_url.render_as_string(hide_password=True)


def test_get_pg_sync_url_is_rendered_and_encoded() -> None:
    settings = Settings(
        db_user="postgres",
        postgres_password="pa:ss@word/with+chars",
        db_host="127.0.0.1",
        db_port=5433,
        db_name="cpr_pivot",
    )
    sync_url = settings.get_pg_sync_url()
    assert "***" in sync_url

    real_sync_url = settings.get_pg_sync_url(mask_password=False)
    assert real_sync_url.startswith("postgresql://postgres:")
    assert "@127.0.0.1:5433/cpr_pivot" in real_sync_url
    assert "pa:ss@word/with+chars" not in real_sync_url


def test_get_pg_sync_url_can_return_masked_and_unmasked() -> None:
    settings = Settings(
        db_user="postgres",
        postgres_password="secret",
        db_host="127.0.0.1",
        db_port=5433,
        db_name="cpr_pivot",
    )
    masked = settings.get_pg_sync_url()
    unmasked = settings.get_pg_sync_url(mask_password=False)
    assert "***" in masked
    assert "***" not in unmasked
    assert "secret" not in masked
    assert unmasked.startswith("postgresql://postgres:")
    assert "@127.0.0.1:5433/cpr_pivot" in unmasked


def test_paper_trading_settings_defaults_are_present() -> None:
    settings = Settings()

    assert settings.paper_trading_enabled is False
    assert settings.paper_default_strategy == "CPR_LEVELS"
    assert settings.paper_default_symbols is None
    assert settings.paper_max_daily_loss_pct == 0.03
    assert settings.paper_max_positions == 10
    assert settings.paper_max_position_pct == 0.10
    assert settings.paper_stale_feed_timeout_sec == 120
    assert settings.paper_flatten_time == "15:15:00"
    assert settings.paper_live_poll_interval_sec == 1.0
    assert settings.paper_candle_interval_minutes == 5
    assert settings.paper_live_quote_batch_size == 500


def test_settings_redact_sensitive_values_in_safe_dict() -> None:
    settings = Settings(
        postgres_password="secret",
        smtp_password="smtp-secret",
        ollama_api_key="ollama-secret",
        kite_api_secret="kite-secret",
        kite_access_token="kite-token",
    )

    safe = settings.get_safe_dict()

    assert safe["postgres_password"] == "***"
    assert safe["smtp_password"] == "***"
    assert safe["ollama_api_key"] == "***"
    assert safe["kite_api_secret"] == "***"
    assert safe["kite_access_token"] == "***"


def test_settings_include_configurable_postgres_pool_defaults() -> None:
    settings = Settings()

    assert settings.db_pool_size == 10
    assert settings.db_max_overflow == 20
    assert settings.db_pool_recycle_sec == 3600
