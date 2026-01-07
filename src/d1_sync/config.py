"""
D1 Sync Configuration System.

This module provides a comprehensive, type-safe configuration system using Pydantic.
Settings can be loaded from:
1. Environment variables (prefixed with D1_SYNC_)
2. Config file (TOML or JSON)
3. CLI arguments (highest priority)

Example usage:
    from d1_sync.config import Settings, TierLimits
    
    # Load from environment
    settings = Settings()
    
    # Or with explicit values
    settings = Settings(
        cloudflare_account_id="your-account-id",
        cloudflare_api_token="your-token",
        tier="paid"
    )
"""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Self

from pydantic import BaseModel, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Tier(str, Enum):
    """Cloudflare account tier affecting API limits."""

    FREE = "free"
    PAID = "paid"


class Limits(BaseModel):
    """API and operation limits based on Cloudflare tier."""

    max_sql_length_bytes: int = Field(
        default=100 * 1024,
        description="Maximum SQL statement length in bytes (100KB)",
    )
    max_rows_per_batch: int = Field(
        default=100,
        description="Maximum rows to include in a single INSERT batch",
    )
    max_query_duration_seconds: int = Field(
        default=30,
        description="Maximum query execution time",
    )
    max_bound_params: int = Field(
        default=100,
        description="Maximum bound parameters per query",
    )
    daily_row_reads: int | None = Field(
        default=5_000_000,
        description="Daily row read limit (None = unlimited)",
    )
    daily_row_writes: int | None = Field(
        default=100_000,
        description="Daily row write limit (None = unlimited)",
    )
    batch_safety_margin: float = Field(
        default=0.85,
        ge=0.5,
        le=1.0,
        description="Safety margin for batch size calculation (use 85% of max)",
    )
    concurrent_batches: int = Field(
        default=1,
        ge=1,
        le=6,
        description="Number of concurrent batch uploads (max 6 per D1 limits)",
    )


class TierLimits:
    """Pre-configured limits for each Cloudflare tier."""

    FREE = Limits(
        max_sql_length_bytes=100 * 1024,
        max_rows_per_batch=100,
        max_query_duration_seconds=30,
        max_bound_params=100,
        daily_row_reads=5_000_000,
        daily_row_writes=100_000,
        batch_safety_margin=0.85,
        concurrent_batches=1,
    )

    PAID = Limits(
        max_sql_length_bytes=100 * 1024,
        max_rows_per_batch=500,
        max_query_duration_seconds=30,
        max_bound_params=100,
        daily_row_reads=None,  # Unlimited
        daily_row_writes=None,  # Unlimited
        batch_safety_margin=0.90,
        concurrent_batches=3,
    )

    @classmethod
    def for_tier(cls, tier: Tier) -> Limits:
        """Get limits for the specified tier."""
        return cls.PAID if tier == Tier.PAID else cls.FREE


class SyncOptions(BaseModel):
    """Options controlling sync behavior."""

    # Sync mode
    dry_run: bool = Field(
        default=False,
        description="Preview changes without executing them",
    )
    overwrite: bool = Field(
        default=False,
        description="Use INSERT OR REPLACE instead of INSERT OR IGNORE",
    )
    
    # Table selection
    tables: list[str] = Field(
        default_factory=list,
        description="Specific tables to sync (empty = all tables)",
    )
    exclude_tables: list[str] = Field(
        default_factory=list,
        description="Tables to exclude from sync",
    )
    
    # Row limits
    limit: int | None = Field(
        default=None,
        ge=1,
        description="Maximum rows to sync per table (for testing)",
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Starting row offset for sync",
    )
    
    # Schema handling
    sync_schema: bool = Field(
        default=True,
        description="Sync table schema (CREATE TABLE) before data",
    )
    drop_before_sync: bool = Field(
        default=False,
        description="Drop and recreate tables before syncing (DANGEROUS)",
    )
    
    # Data integrity
    verify_after_sync: bool = Field(
        default=True,
        description="Verify data integrity after sync completes",
    )
    checksum_algorithm: str = Field(
        default="md5",
        pattern="^(md5|sha256)$",
        description="Algorithm for row checksums",
    )
    
    # Performance
    batch_size_override: int | None = Field(
        default=None,
        ge=1,
        le=1000,
        description="Override automatic batch size calculation",
    )
    
    # Resume
    resume: bool = Field(
        default=True,
        description="Resume from last checkpoint if available",
    )
    state_file: Path = Field(
        default=Path(".d1-sync-state.json"),
        description="Path to state file for resume capability",
    )


class LoggingConfig(BaseModel):
    """Logging configuration."""

    level: str = Field(
        default="INFO",
        pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$",
        description="Log level",
    )
    file: Path | None = Field(
        default=None,
        description="Log file path (None = console only)",
    )
    format: str = Field(
        default="rich",
        pattern="^(rich|json|simple)$",
        description="Log format: rich (colored), json, or simple",
    )
    failed_rows_file: Path = Field(
        default=Path("failed_rows.json"),
        description="File to store failed row details",
    )
    max_file_size_mb: int = Field(
        default=10,
        ge=1,
        le=100,
        description="Max log file size before rotation",
    )
    backup_count: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Number of rotated log files to keep",
    )


class DatabaseConfig(BaseModel):
    """Database connection configuration."""

    # Required
    source_path: Path | None = Field(
        default=None,
        description="Path to local SQLite database file",
    )
    
    # Remote SQLite (optional)
    source_url: str | None = Field(
        default=None,
        description="URL for remote SQLite database (read-only)",
    )
    
    @model_validator(mode="after")
    def validate_source(self) -> Self:
        """Ensure at least one source is provided when needed."""
        # Allow both to be None during initial config loading
        return self


class Settings(BaseSettings):
    """
    Main settings class for D1 Sync.
    
    Settings are loaded in this priority (highest first):
    1. Explicit constructor arguments
    2. Environment variables (D1_SYNC_* prefix)
    3. Config file (if specified)
    4. Defaults
    
    Example:
        # From environment
        export D1_SYNC_CLOUDFLARE_API_TOKEN="your-token"
        export D1_SYNC_CLOUDFLARE_ACCOUNT_ID="your-account-id"
        settings = Settings()
        
        # From config file
        settings = Settings.from_file("config.toml")
    """

    model_config = SettingsConfigDict(
        env_prefix="D1_SYNC_",
        env_nested_delimiter="__",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Cloudflare credentials
    cloudflare_api_token: SecretStr = Field(
        default=SecretStr(""),
        description="Cloudflare API token with D1 permissions",
    )
    cloudflare_account_id: str = Field(
        default="",
        description="Cloudflare account ID",
    )
    
    # D1 database
    database_name: str = Field(
        default="",
        description="D1 database name",
    )
    database_id: str = Field(
        default="",
        description="D1 database UUID",
    )
    
    # Account tier
    tier: Tier = Field(
        default=Tier.FREE,
        description="Cloudflare account tier (free/paid)",
    )
    
    # Nested configs
    limits: Limits = Field(default_factory=Limits)
    sync: SyncOptions = Field(default_factory=SyncOptions)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)

    @model_validator(mode="after")
    def apply_tier_limits(self) -> Self:
        """Apply tier-specific limits if using defaults."""
        if self.limits == Limits():
            self.limits = TierLimits.for_tier(self.tier)
        return self

    @field_validator("cloudflare_api_token", mode="before")
    @classmethod
    def validate_token(cls, v: Any) -> SecretStr:
        """Handle token from various sources."""
        if isinstance(v, SecretStr):
            return v
        if isinstance(v, str):
            return SecretStr(v)
        return SecretStr("")

    @classmethod
    def from_file(cls, path: Path | str) -> "Settings":
        """Load settings from a TOML or JSON config file."""
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        content = path.read_text()
        
        if path.suffix in (".toml", ".tml"):
            try:
                import tomllib
            except ImportError:
                import tomli as tomllib  # type: ignore
            data = tomllib.loads(content)
        elif path.suffix == ".json":
            data = json.loads(content)
        else:
            raise ValueError(f"Unsupported config format: {path.suffix}")

        return cls.model_validate(data)

    def to_file(self, path: Path | str) -> None:
        """Save current settings to a config file."""
        path = Path(path)
        data = self.model_dump(mode="json", exclude_none=True)
        
        # Mask sensitive data
        if "cloudflare_api_token" in data:
            data["cloudflare_api_token"] = "***REDACTED***"
        
        if path.suffix == ".json":
            path.write_text(json.dumps(data, indent=2))
        elif path.suffix in (".toml", ".tml"):
            # Basic TOML serialization
            lines = []
            for key, value in data.items():
                if isinstance(value, dict):
                    lines.append(f"\n[{key}]")
                    for k, v in value.items():
                        lines.append(f'{k} = {json.dumps(v)}')
                else:
                    lines.append(f'{key} = {json.dumps(value)}')
            path.write_text("\n".join(lines))
        else:
            path.write_text(json.dumps(data, indent=2))

    def get_effective_limits(self) -> Limits:
        """Get the effective limits based on tier and overrides."""
        return self.limits

    def validate_credentials(self) -> list[str]:
        """Validate that required credentials are present. Returns list of errors."""
        errors = []
        if not self.cloudflare_api_token.get_secret_value():
            errors.append("cloudflare_api_token is required")
        if not self.cloudflare_account_id:
            errors.append("cloudflare_account_id is required")
        if not self.database_name and not self.database_id:
            errors.append("database_name or database_id is required")
        return errors


# Convenience function for loading settings
def load_settings(
    config_file: Path | str | None = None,
    **overrides: Any,
) -> Settings:
    """
    Load settings with optional config file and overrides.
    
    Args:
        config_file: Optional path to config file
        **overrides: Settings to override (highest priority)
    
    Returns:
        Configured Settings instance
    """
    if config_file:
        settings = Settings.from_file(config_file)
        if overrides:
            # Apply overrides
            data = settings.model_dump()
            data.update(overrides)
            return Settings.model_validate(data)
        return settings
    return Settings(**overrides)
