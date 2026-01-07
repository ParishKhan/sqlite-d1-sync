"""
Rich Terminal Display Components.

Provides beautiful console UI for:
- Progress bars with ETA
- Statistics tables
- Status updates
- Summary reports
"""

from __future__ import annotations

from typing import Any

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TaskProgressColumn,
    TimeRemainingColumn,
    SpinnerColumn,
    MofNCompleteColumn,
)
from rich.table import Table
from rich.text import Text


console = Console()


class ProgressDisplay:
    """
    Rich terminal UI for sync progress.
    
    Shows:
    - Overall progress bar
    - Per-table progress
    - Live statistics
    - ETA estimation
    
    Example:
        display = ProgressDisplay()
        display.start()
        
        display.update(
            table="users",
            rows_done=500,
            rows_total=1000,
            rate=100.0,
        )
        
        display.stop()
    """

    def __init__(self) -> None:
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
        )
        self._live: Live | None = None
        self._main_task_id: Any = None
        self._table_task_id: Any = None
        self._stats: dict[str, Any] = {}

    def start(
        self,
        operation: str,
        source: str,
        destination: str,
        total_rows: int,
        total_tables: int,
    ) -> None:
        """Start the progress display."""
        self._stats = {
            "operation": operation,
            "source": source,
            "destination": destination,
            "total_rows": total_rows,
            "total_tables": total_tables,
            "rows_processed": 0,
            "rows_failed": 0,
            "current_table": "",
            "rate": 0.0,
            "bytes_transferred": 0,
        }

        self._main_task_id = self.progress.add_task(
            f"[cyan]{operation.upper()}",
            total=total_rows,
        )

        self._live = Live(
            self._build_display(),
            console=console,
            refresh_per_second=4,
        )
        self._live.start()

    def stop(self) -> None:
        """Stop the progress display."""
        if self._live:
            self._live.stop()
            self._live = None

    def update(
        self,
        rows_processed: int | None = None,
        rows_failed: int | None = None,
        current_table: str | None = None,
        rate: float | None = None,
        bytes_transferred: int | None = None,
        tables_processed: int | None = None,
    ) -> None:
        """Update progress display."""
        if rows_processed is not None:
            self._stats["rows_processed"] = rows_processed
            if self._main_task_id is not None:
                self.progress.update(
                    self._main_task_id,
                    completed=rows_processed,
                )

        if rows_failed is not None:
            self._stats["rows_failed"] = rows_failed

        if current_table is not None:
            self._stats["current_table"] = current_table

        if rate is not None:
            self._stats["rate"] = rate

        if bytes_transferred is not None:
            self._stats["bytes_transferred"] = bytes_transferred

        if tables_processed is not None:
            self._stats["tables_processed"] = tables_processed

        if self._live:
            self._live.update(self._build_display())

    def _build_display(self) -> Panel:
        """Build the display panel."""
        # Header
        op = self._stats.get("operation", "sync").upper()
        title = f"[bold white]D1 Sync - {op} Operation[/bold white]"

        # Info table
        info_table = Table.grid(padding=(0, 2))
        info_table.add_column(style="dim")
        info_table.add_column()

        info_table.add_row(
            "Source:",
            self._stats.get("source", ""),
        )
        info_table.add_row(
            "Destination:",
            self._stats.get("destination", ""),
        )

        # Stats table
        stats_table = Table.grid(padding=(0, 3))
        stats_table.add_column(justify="center")
        stats_table.add_column(justify="center")
        stats_table.add_column(justify="center")
        stats_table.add_column(justify="center")

        total_tables = self._stats.get("total_tables", 0)
        tables_done = self._stats.get("tables_processed", 0)

        stats_table.add_row(
            f"[cyan]Tables:[/cyan] {tables_done}/{total_tables}",
            f"[green]Rows:[/green] {self._stats.get('rows_processed', 0):,}",
            f"[red]Failed:[/red] {self._stats.get('rows_failed', 0):,}",
            f"[yellow]Speed:[/yellow] {self._stats.get('rate', 0):,.0f}/s",
        )

        # Current status
        current = self._stats.get("current_table", "")
        status_text = Text()
        if current:
            status_text.append("Current: ", style="dim")
            status_text.append(current, style="bold cyan")

        # Combine elements
        display = Group(
            info_table,
            Text(),  # Spacer
            self.progress,
            Text(),  # Spacer
            stats_table,
            status_text,
        )

        return Panel(
            display,
            title=title,
            border_style="blue",
            padding=(1, 2),
        )

    def __enter__(self) -> "ProgressDisplay":
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


def print_summary(stats: dict[str, Any]) -> None:
    """Print a summary table after sync completion."""
    table = Table(title="Sync Summary", border_style="green")
    
    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")

    table.add_row("Operation", stats.get("operation", "N/A"))
    table.add_row("Duration", f"{stats.get('duration', 0):.1f}s")
    table.add_row("Tables", f"{stats.get('tables_processed', 0)}/{stats.get('tables_total', 0)}")
    table.add_row("Rows Processed", f"{stats.get('rows_processed', 0):,}")
    table.add_row("Rows Failed", f"{stats.get('rows_failed', 0):,}")
    table.add_row("Data Transferred", format_bytes(stats.get("bytes_transferred", 0)))
    table.add_row("Average Speed", f"{stats.get('rows_per_second', 0):,.0f} rows/s")

    console.print(table)


def format_bytes(size: int) -> str:
    """Format bytes as human-readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(size) < 1024:
            return f"{size:.1f} {unit}"
        size //= 1024
    return f"{size:.1f} TB"


def print_error(message: str) -> None:
    """Print an error message."""
    console.print(f"[red bold]Error:[/red bold] {message}")


def print_success(message: str) -> None:
    """Print a success message."""
    console.print(f"[green bold]✓[/green bold] {message}")


def print_warning(message: str) -> None:
    """Print a warning message."""
    console.print(f"[yellow bold]⚠[/yellow bold] {message}")


def print_info(message: str) -> None:
    """Print an info message."""
    console.print(f"[blue]ℹ[/blue] {message}")
