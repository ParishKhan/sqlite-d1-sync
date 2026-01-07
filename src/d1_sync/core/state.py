"""
State Manager - Resume and recovery functionality.

Provides persistent state tracking for:
- Sync progress (which rows have been processed)
- Failed rows (with full error details)
- Checkpoint management (for resuming)
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class FailedRow:
    """Information about a failed row."""

    table: str
    row_offset: int
    row_data: dict[str, Any]
    error: str
    timestamp: str
    retry_count: int = 0


@dataclass
class TableProgress:
    """Progress tracking for a single table."""

    name: str
    total_rows: int
    processed_rows: int = 0
    failed_rows: int = 0
    last_offset: int = 0
    checksum: str = ""
    status: str = "pending"  # pending, in_progress, completed, failed
    started_at: str | None = None
    completed_at: str | None = None


@dataclass
class SyncState:
    """Complete sync state for persistence."""

    operation: str  # push or pull
    source: str
    destination: str
    started_at: str
    updated_at: str
    status: str = "in_progress"  # in_progress, completed, failed, interrupted
    tables: dict[str, TableProgress] = field(default_factory=dict)
    failed_rows: list[FailedRow] = field(default_factory=list)
    total_rows_processed: int = 0
    total_rows_failed: int = 0
    settings_hash: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "operation": self.operation,
            "source": self.source,
            "destination": self.destination,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "status": self.status,
            "tables": {
                name: asdict(progress) for name, progress in self.tables.items()
            },
            "failed_rows": [asdict(row) for row in self.failed_rows],
            "total_rows_processed": self.total_rows_processed,
            "total_rows_failed": self.total_rows_failed,
            "settings_hash": self.settings_hash,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SyncState":
        """Create from dictionary."""
        tables = {}
        for name, progress_data in data.get("tables", {}).items():
            tables[name] = TableProgress(**progress_data)

        failed_rows = [
            FailedRow(**row_data) for row_data in data.get("failed_rows", [])
        ]

        return cls(
            operation=data.get("operation", "push"),
            source=data.get("source", ""),
            destination=data.get("destination", ""),
            started_at=data.get("started_at", ""),
            updated_at=data.get("updated_at", ""),
            status=data.get("status", "in_progress"),
            tables=tables,
            failed_rows=failed_rows,
            total_rows_processed=data.get("total_rows_processed", 0),
            total_rows_failed=data.get("total_rows_failed", 0),
            settings_hash=data.get("settings_hash", ""),
        )


class StateManager:
    """
    State persistence and recovery manager.
    
    Provides:
    - Automatic state saving after each batch
    - Resume from last checkpoint
    - Failed row tracking
    - Progress querying
    
    Example:
        state_mgr = StateManager(Path(".d1-sync-state.json"))
        
        # Start new sync or resume
        state = state_mgr.get_or_create_state("push", source, dest)
        
        # Update progress
        state_mgr.update_table_progress("users", processed=100)
        
        # Record failure
        state_mgr.record_failed_row("users", 42, row_data, "Error msg")
        
        # Save state
        state_mgr.save()
    """

    def __init__(
        self,
        state_file: Path | str,
        failed_rows_file: Path | str | None = None,
    ) -> None:
        """
        Initialize state manager.
        
        Args:
            state_file: Path to state file
            failed_rows_file: Optional separate file for failed rows
        """
        self.state_file = Path(state_file)
        self.failed_rows_file = Path(failed_rows_file) if failed_rows_file else None
        self._state: SyncState | None = None
        self._dirty = False

    @property
    def state(self) -> SyncState | None:
        """Current state or None if not initialized."""
        return self._state

    def load(self) -> SyncState | None:
        """Load state from file if it exists."""
        if not self.state_file.exists():
            return None

        try:
            data = json.loads(self.state_file.read_text())
            self._state = SyncState.from_dict(data)
            return self._state
        except (json.JSONDecodeError, KeyError) as e:
            # Corrupted state file
            print(f"Warning: Could not load state file: {e}")
            return None

    def save(self) -> None:
        """Save current state to file."""
        if self._state is None:
            return

        self._state.updated_at = datetime.now(timezone.utc).isoformat()
        self.state_file.write_text(json.dumps(self._state.to_dict(), indent=2))
        self._dirty = False

        # Also save failed rows separately if configured
        if self.failed_rows_file and self._state.failed_rows:
            failed_data = [asdict(row) for row in self._state.failed_rows]
            self.failed_rows_file.write_text(json.dumps(failed_data, indent=2))

    def get_or_create_state(
        self,
        operation: str,
        source: str,
        destination: str,
        settings_hash: str = "",
    ) -> SyncState:
        """
        Get existing state or create new one.
        
        If existing state matches operation/source/dest, returns it for resume.
        Otherwise creates a new state.
        
        Args:
            operation: "push" or "pull"
            source: Source identifier
            destination: Destination identifier
            settings_hash: Hash of current settings (for invalidation)
        
        Returns:
            SyncState object
        """
        existing = self.load()

        if (
            existing
            and existing.operation == operation
            and existing.source == source
            and existing.destination == destination
            and existing.status == "in_progress"
        ):
            # Check settings hash if provided
            if settings_hash and existing.settings_hash != settings_hash:
                # Settings changed, start fresh
                pass
            else:
                # Resume existing
                self._state = existing
                return existing

        # Create new state
        now = datetime.now(timezone.utc).isoformat()
        self._state = SyncState(
            operation=operation,
            source=source,
            destination=destination,
            started_at=now,
            updated_at=now,
            settings_hash=settings_hash,
        )
        self._dirty = True
        return self._state

    def clear_state(self) -> None:
        """Clear all state and delete state file."""
        self._state = None
        if self.state_file.exists():
            self.state_file.unlink()
        if self.failed_rows_file and self.failed_rows_file.exists():
            self.failed_rows_file.unlink()

    def init_table(
        self,
        table_name: str,
        total_rows: int,
    ) -> TableProgress:
        """
        Initialize progress tracking for a table.
        
        Args:
            table_name: Name of the table
            total_rows: Total rows in table
        
        Returns:
            TableProgress object
        """
        if self._state is None:
            raise RuntimeError("State not initialized")

        if table_name in self._state.tables:
            # Already tracking
            return self._state.tables[table_name]

        progress = TableProgress(
            name=table_name,
            total_rows=total_rows,
            status="pending",
        )
        self._state.tables[table_name] = progress
        self._dirty = True
        return progress

    def get_table_progress(self, table_name: str) -> TableProgress | None:
        """Get progress for a specific table."""
        if self._state is None:
            return None
        return self._state.tables.get(table_name)

    def update_table_progress(
        self,
        table_name: str,
        processed: int | None = None,
        failed: int | None = None,
        last_offset: int | None = None,
        status: str | None = None,
        checksum: str | None = None,
    ) -> None:
        """
        Update progress for a table.
        
        Args:
            table_name: Name of the table
            processed: New processed rows count (or increment if current + value)
            failed: New failed rows count
            last_offset: Last processed offset
            status: New status
            checksum: Checksum of processed data
        """
        if self._state is None:
            raise RuntimeError("State not initialized")

        progress = self._state.tables.get(table_name)
        if progress is None:
            raise ValueError(f"Table not initialized: {table_name}")

        if processed is not None:
            progress.processed_rows = processed
            self._state.total_rows_processed = sum(
                t.processed_rows for t in self._state.tables.values()
            )

        if failed is not None:
            progress.failed_rows = failed
            self._state.total_rows_failed = sum(
                t.failed_rows for t in self._state.tables.values()
            )

        if last_offset is not None:
            progress.last_offset = last_offset

        if status is not None:
            progress.status = status
            if status == "in_progress" and not progress.started_at:
                progress.started_at = datetime.now(timezone.utc).isoformat()
            elif status in ("completed", "failed"):
                progress.completed_at = datetime.now(timezone.utc).isoformat()

        if checksum is not None:
            progress.checksum = checksum

        self._dirty = True

    def record_failed_row(
        self,
        table: str,
        offset: int,
        row_data: dict[str, Any],
        error: str,
    ) -> None:
        """
        Record a failed row for later retry.
        
        Args:
            table: Table name
            offset: Row offset in source
            row_data: The actual row data
            error: Error message
        """
        if self._state is None:
            raise RuntimeError("State not initialized")

        # Check if already recorded
        for existing in self._state.failed_rows:
            if existing.table == table and existing.row_offset == offset:
                existing.retry_count += 1
                existing.error = error
                existing.timestamp = datetime.now(timezone.utc).isoformat()
                self._dirty = True
                return

        # Add new failure
        failed_row = FailedRow(
            table=table,
            row_offset=offset,
            row_data=row_data,
            error=error,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        self._state.failed_rows.append(failed_row)
        self._dirty = True

    def get_resume_offset(self, table_name: str) -> int:
        """
        Get the offset to resume from for a table.
        
        Returns:
            Offset to resume from (0 if starting fresh)
        """
        progress = self.get_table_progress(table_name)
        if progress and progress.status in ("in_progress", "failed"):
            return progress.last_offset
        return 0

    def should_process_table(self, table_name: str) -> bool:
        """
        Check if a table should be processed.
        
        Returns False if already completed successfully.
        """
        progress = self.get_table_progress(table_name)
        if progress and progress.status == "completed":
            return False
        return True

    def mark_sync_complete(self, status: str = "completed") -> None:
        """Mark the entire sync operation as complete."""
        if self._state is None:
            return

        self._state.status = status
        self.save()

    def get_summary(self) -> dict[str, Any]:
        """Get a summary of current state for display."""
        if self._state is None:
            return {}

        tables_summary = {}
        for name, progress in self._state.tables.items():
            tables_summary[name] = {
                "status": progress.status,
                "processed": progress.processed_rows,
                "total": progress.total_rows,
                "failed": progress.failed_rows,
                "percent": round(
                    progress.processed_rows / progress.total_rows * 100, 1
                ) if progress.total_rows > 0 else 0,
            }

        return {
            "operation": self._state.operation,
            "status": self._state.status,
            "started_at": self._state.started_at,
            "total_processed": self._state.total_rows_processed,
            "total_failed": self._state.total_rows_failed,
            "tables": tables_summary,
        }
