"""
D1 Sync CLI - Command Line Interface.

Professional CLI for SQLite ↔ Cloudflare D1 synchronization.

Commands:
    push    Push local SQLite to Cloudflare D1
    pull    Pull from Cloudflare D1 to local SQLite
    status  Show sync status and progress
    verify  Verify data integrity
    config  Manage configuration
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from d1_sync import __version__
from d1_sync.config import Settings, Tier, load_settings
from d1_sync.core.engine import SyncEngine, SyncStats
from d1_sync.utils.display import (
    ProgressDisplay,
    print_summary,
    print_error,
    print_success,
    print_info,
    print_warning,
)
from d1_sync.utils.logger import setup_logging, console as log_console


# Create the Typer app
app = typer.Typer(
    name="d1-sync",
    help="Professional SQLite ↔ Cloudflare D1 synchronization tool.",
    add_completion=True,
    rich_markup_mode="rich",
)

console = Console()


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        console.print(f"[bold cyan]d1-sync[/bold cyan] version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    """D1 Sync - Professional SQLite ↔ Cloudflare D1 synchronization."""
    pass


# =============================================================================
# PUSH Command
# =============================================================================
@app.command()
def push(
    source: Path = typer.Option(
        ...,
        "--source",
        "-s",
        help="Path to source SQLite database.",
        exists=True,
        file_okay=True,
        dir_okay=False,
        resolve_path=True,
    ),
    database: str = typer.Option(
        None,
        "--database",
        "-d",
        help="D1 database name (overrides config).",
    ),
    database_id: str = typer.Option(
        None,
        "--database-id",
        help="D1 database ID (overrides config).",
    ),
    account_id: str = typer.Option(
        None,
        "--account-id",
        help="Cloudflare account ID (overrides config).",
    ),
    api_token: str = typer.Option(
        None,
        "--api-token",
        envvar="D1_SYNC_CLOUDFLARE_API_TOKEN",
        help="Cloudflare API token.",
    ),
    config_file: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file.",
        exists=True,
    ),
    tier: Tier = typer.Option(
        Tier.FREE,
        "--tier",
        "-t",
        help="Account tier (affects limits).",
    ),
    tables: Optional[list[str]] = typer.Option(
        None,
        "--table",
        help="Specific tables to sync (can be repeated).",
    ),
    exclude: Optional[list[str]] = typer.Option(
        None,
        "--exclude",
        help="Tables to exclude (can be repeated).",
    ),
    limit: Optional[int] = typer.Option(
        None,
        "--limit",
        "-l",
        help="Maximum rows per table (for testing).",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Use INSERT OR REPLACE (overwrites existing rows).",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-n",
        help="Preview changes without executing.",
    ),
    resume: bool = typer.Option(
        True,
        "--resume/--no-resume",
        help="Resume from last checkpoint if available.",
    ),
    verify: bool = typer.Option(
        True,
        "--verify/--no-verify",
        help="Verify data integrity after sync.",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Minimal output.",
    ),
) -> None:
    """
    Push local SQLite database to Cloudflare D1.
    
    Example:
        d1-sync push --source ./my-database.db --database my-d1-db
    """
    # Build settings
    try:
        settings = _build_settings(
            config_file=config_file,
            database=database,
            database_id=database_id,
            account_id=account_id,
            api_token=api_token,
            tier=tier,
            tables=tables,
            exclude=exclude,
            limit=limit,
            overwrite=overwrite,
            dry_run=dry_run,
            resume=resume,
            verify=verify,
        )
    except ValueError as e:
        print_error(str(e))
        raise typer.Exit(1)

    # Validate credentials
    errors = settings.validate_credentials()
    if errors:
        for err in errors:
            print_error(err)
        print_info("Use --help for configuration options.")
        raise typer.Exit(1)

    # Setup logging
    setup_logging(
        level="WARNING" if quiet else settings.logging.level,
        log_file=settings.logging.file,
        format_style=settings.logging.format,
    )

    # Run sync
    if dry_run:
        print_warning("DRY RUN - No changes will be made")

    engine = SyncEngine(settings)
    
    display = ProgressDisplay() if not quiet else None

    def on_progress(stats: SyncStats) -> None:
        if display:
            display.update(
                rows_processed=stats.rows_processed,
                rows_failed=stats.rows_failed,
                rate=stats.rows_per_second,
                bytes_transferred=stats.bytes_transferred,
                tables_processed=stats.tables_processed,
            )

    try:
        # Start display
        if display:
            from d1_sync.connectors.sqlite import SQLiteConnector
            sqlite = SQLiteConnector(source, readonly=True)
            tables_info = sqlite.get_tables()
            total_rows = sum(t.row_count for t in tables_info)
            sqlite.close()

            display.start(
                operation="push",
                source=str(source.name),
                destination=f"{settings.database_name}@D1",
                total_rows=total_rows,
                total_tables=len(tables_info),
            )

        # Run async push
        stats = asyncio.run(engine.push(source, on_progress=on_progress))

    finally:
        if display:
            display.stop()

    # Print summary
    if not quiet:
        console.print()
        print_summary({
            "operation": "PUSH",
            "duration": stats.duration_seconds,
            "tables_processed": stats.tables_processed,
            "tables_total": stats.tables_total,
            "rows_processed": stats.rows_processed,
            "rows_failed": stats.rows_failed,
            "bytes_transferred": stats.bytes_transferred,
            "rows_per_second": stats.rows_per_second,
        })

    # Handle errors
    if stats.errors:
        console.print()
        print_warning(f"{len(stats.errors)} errors occurred:")
        for err in stats.errors[:10]:
            print_error(f"  • {err}")
        if len(stats.errors) > 10:
            print_info(f"  ... and {len(stats.errors) - 10} more")

    if stats.rows_failed > 0:
        print_warning(
            f"Failed rows saved to: {settings.logging.failed_rows_file}"
        )
        raise typer.Exit(1)
    else:
        print_success("Push completed successfully!")


# =============================================================================
# PULL Command
# =============================================================================
@app.command()
def pull(
    destination: Path = typer.Option(
        ...,
        "--destination",
        "-d",
        help="Path to destination SQLite database.",
        resolve_path=True,
    ),
    database: str = typer.Option(
        None,
        "--database",
        help="D1 database name.",
    ),
    config_file: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        "-n",
        help="Preview changes without executing.",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Minimal output.",
    ),
) -> None:
    """
    Pull data from Cloudflare D1 to local SQLite.
    
    Example:
        d1-sync pull --destination ./backup.db --database my-d1-db
    """
    print_info("Pull operation - pulling from D1 to local SQLite...")
    
    # TODO: Implement full pull logic
    print_warning("Pull is not fully implemented yet.")
    raise typer.Exit(0)


# =============================================================================
# STATUS Command
# =============================================================================
@app.command()
def status(
    state_file: Path = typer.Option(
        Path(".d1-sync-state.json"),
        "--state-file",
        help="Path to state file.",
    ),
) -> None:
    """Show current sync status and progress."""
    from d1_sync.core.state import StateManager
    
    state_mgr = StateManager(state_file)
    state = state_mgr.load()
    
    if not state:
        print_info("No sync state found. Run a push or pull operation first.")
        raise typer.Exit(0)

    # Build status table
    table = Table(title="Sync Status", border_style="blue")
    table.add_column("Property", style="cyan")
    table.add_column("Value")

    table.add_row("Operation", state.operation.upper())
    table.add_row("Status", _format_status(state.status))
    table.add_row("Source", state.source)
    table.add_row("Destination", state.destination)
    table.add_row("Started", state.started_at)
    table.add_row("Updated", state.updated_at)
    table.add_row("Rows Processed", f"{state.total_rows_processed:,}")
    table.add_row("Rows Failed", f"{state.total_rows_failed:,}")

    console.print(table)

    # Tables progress
    if state.tables:
        console.print()
        tables_table = Table(title="Table Progress", border_style="green")
        tables_table.add_column("Table")
        tables_table.add_column("Status")
        tables_table.add_column("Progress", justify="right")
        tables_table.add_column("Failed", justify="right")

        for name, progress in state.tables.items():
            pct = (
                f"{progress.processed_rows:,}/{progress.total_rows:,} "
                f"({progress.processed_rows/progress.total_rows*100:.1f}%)"
                if progress.total_rows > 0 else "0"
            )
            tables_table.add_row(
                name,
                _format_status(progress.status),
                pct,
                str(progress.failed_rows),
            )

        console.print(tables_table)


def _format_status(status: str) -> str:
    """Format status with color."""
    colors = {
        "completed": "[green]✓ completed[/green]",
        "in_progress": "[yellow]⟳ in progress[/yellow]",
        "failed": "[red]✗ failed[/red]",
        "pending": "[dim]pending[/dim]",
    }
    return colors.get(status, status)


# =============================================================================
# VERIFY Command
# =============================================================================
@app.command()
def verify(
    source: Path = typer.Option(
        ...,
        "--source",
        "-s",
        help="Path to source SQLite database.",
        exists=True,
    ),
    database: str = typer.Option(
        None,
        "--database",
        "-d",
        help="D1 database name.",
    ),
    config_file: Path = typer.Option(
        None,
        "--config",
        "-c",
        help="Path to config file.",
    ),
) -> None:
    """Verify data integrity between local and D1."""
    print_info("Verifying data integrity...")
    
    # TODO: Implement full verification
    print_warning("Verify is not fully implemented yet.")
    raise typer.Exit(0)


# =============================================================================
# CONFIG Command
# =============================================================================
@app.command()
def config(
    show: bool = typer.Option(
        False,
        "--show",
        help="Show current configuration.",
    ),
    init: bool = typer.Option(
        False,
        "--init",
        help="Initialize example config file.",
    ),
    output: Path = typer.Option(
        Path("config.toml"),
        "--output",
        "-o",
        help="Output path for config file.",
    ),
) -> None:
    """Manage configuration."""
    if init:
        # Copy example config
        import shutil
        example = Path(__file__).parent.parent.parent.parent / "config.example.toml"
        if example.exists():
            shutil.copy(example, output)
            print_success(f"Created config file: {output}")
        else:
            # Generate default config
            settings = Settings()
            settings.to_file(output)
            print_success(f"Generated config file: {output}")
        return

    if show:
        settings = Settings()
        table = Table(title="Current Configuration", border_style="cyan")
        table.add_column("Setting", style="cyan")
        table.add_column("Value")

        table.add_row("Account ID", settings.cloudflare_account_id or "[dim]not set[/dim]")
        table.add_row("Database Name", settings.database_name or "[dim]not set[/dim]")
        table.add_row("Database ID", settings.database_id or "[dim]not set[/dim]")
        table.add_row("Tier", settings.tier.value)
        table.add_row("Max Batch Size", f"{settings.limits.max_rows_per_batch} rows")
        table.add_row("Max SQL Size", f"{settings.limits.max_sql_length_bytes // 1024} KB")

        console.print(table)
        return

    # Default: show help
    console.print("Use --show to view config or --init to create config file.")


# =============================================================================
# Helper Functions
# =============================================================================
def _build_settings(
    config_file: Path | None = None,
    **overrides: any,
) -> Settings:
    """Build settings from config file and overrides."""
    # Load from file or create default
    if config_file:
        settings = load_settings(config_file)
    else:
        settings = Settings()

    # Apply CLI overrides
    if overrides.get("database"):
        settings.database_name = overrides["database"]
    if overrides.get("database_id"):
        settings.database_id = overrides["database_id"]
    if overrides.get("account_id"):
        settings.cloudflare_account_id = overrides["account_id"]
    if overrides.get("api_token"):
        from pydantic import SecretStr
        settings.cloudflare_api_token = SecretStr(overrides["api_token"])
    if overrides.get("tier"):
        settings.tier = overrides["tier"]
    if overrides.get("tables"):
        settings.sync.tables = list(overrides["tables"])
    if overrides.get("exclude"):
        settings.sync.exclude_tables = list(overrides["exclude"])
    if overrides.get("limit"):
        settings.sync.limit = overrides["limit"]
    if overrides.get("overwrite") is not None:
        settings.sync.overwrite = overrides["overwrite"]
    if overrides.get("dry_run") is not None:
        settings.sync.dry_run = overrides["dry_run"]
    if overrides.get("resume") is not None:
        settings.sync.resume = overrides["resume"]
    if overrides.get("verify") is not None:
        settings.sync.verify_after_sync = overrides["verify"]

    return settings


if __name__ == "__main__":
    app()
