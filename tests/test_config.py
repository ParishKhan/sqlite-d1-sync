"""Tests for configuration module."""

import os
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from d1_sync.config import Settings, TierLimits, Tier, load_settings


class TestSettings:
    """Test Settings class."""

    def test_default_settings(self) -> None:
        """Test default settings creation."""
        settings = Settings()
        assert settings.tier == Tier.FREE
        assert settings.limits.max_sql_length_bytes == 100 * 1024
        assert settings.limits.max_rows_per_batch == 100

    def test_tier_limits_free(self) -> None:
        """Test free tier limits."""
        limits = TierLimits.FREE
        assert limits.max_rows_per_batch == 100
        assert limits.daily_row_writes == 100_000
        assert limits.concurrent_batches == 1

    def test_tier_limits_paid(self) -> None:
        """Test paid tier limits."""
        limits = TierLimits.PAID
        assert limits.max_rows_per_batch == 500
        assert limits.daily_row_writes is None  # Unlimited
        assert limits.concurrent_batches == 3

    def test_tier_limits_for_tier(self) -> None:
        """Test TierLimits.for_tier() method."""
        free_limits = TierLimits.for_tier(Tier.FREE)
        assert free_limits.max_rows_per_batch == 100

        paid_limits = TierLimits.for_tier(Tier.PAID)
        assert paid_limits.max_rows_per_batch == 500

    def test_settings_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test settings loading from environment variables."""
        monkeypatch.setenv("D1_SYNC_CLOUDFLARE_ACCOUNT_ID", "test-account")
        monkeypatch.setenv("D1_SYNC_DATABASE_NAME", "test-db")
        monkeypatch.setenv("D1_SYNC_TIER", "paid")

        settings = Settings()
        assert settings.cloudflare_account_id == "test-account"
        assert settings.database_name == "test-db"
        assert settings.tier == Tier.PAID

    def test_settings_validate_credentials_missing(self) -> None:
        """Test credential validation with missing values."""
        settings = Settings()
        errors = settings.validate_credentials()
        assert len(errors) > 0
        assert any("api_token" in e for e in errors)

    def test_settings_validate_credentials_complete(self) -> None:
        """Test credential validation with all values."""
        from pydantic import SecretStr
        settings = Settings(
            cloudflare_api_token=SecretStr("test-token"),
            cloudflare_account_id="test-account",
            database_name="test-db",
        )
        errors = settings.validate_credentials()
        assert len(errors) == 0

    def test_settings_to_file_json(self, tmp_path: Path) -> None:
        """Test saving settings to JSON file."""
        settings = Settings(
            cloudflare_account_id="test-account",
            database_name="test-db",
        )
        output_path = tmp_path / "config.json"
        settings.to_file(output_path)
        
        assert output_path.exists()
        content = output_path.read_text()
        assert "test-account" in content
        assert "REDACTED" in content  # Token should be redacted


class TestSyncOptions:
    """Test SyncOptions class."""

    def test_default_sync_options(self) -> None:
        """Test default sync options."""
        settings = Settings()
        assert settings.sync.dry_run is False
        assert settings.sync.overwrite is False
        assert settings.sync.resume is True
        assert settings.sync.verify_after_sync is True

    def test_sync_options_modification(self) -> None:
        """Test modifying sync options."""
        settings = Settings()
        settings.sync.dry_run = True
        settings.sync.overwrite = True
        
        assert settings.sync.dry_run is True
        assert settings.sync.overwrite is True
