"""
Sync Engine - Main orchestration for sync operations.

Coordinates all components to perform push and pull operations:
- SQLite connector for local database
- D1 client for remote database
- Chunker for size-aware batching
- State manager for resume/recovery
- Integrity checker for verification
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from d1_sync.config import Settings
from d1_sync.connectors.sqlite import SQLiteConnector, RowBatch
from d1_sync.connectors.d1_client import D1Client, create_d1_client
from d1_sync.core.chunker import SQLChunker
from d1_sync.core.state import StateManager
from d1_sync.core.integrity import IntegrityChecker


@dataclass
class SyncStats:
    """Statistics for a sync operation."""

    operation: str  # push or pull
    tables_total: int = 0
    tables_processed: int = 0
    tables_failed: int = 0
    rows_total: int = 0
    rows_processed: int = 0
    rows_failed: int = 0
    bytes_transferred: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    errors: list[str] = None  # type: ignore

    def __post_init__(self) -> None:
        if self.errors is None:
            self.errors = []

    @property
    def duration_seconds(self) -> float:
        """Duration in seconds."""
        if self.end_time and self.start_time:
            return self.end_time - self.start_time
        if self.start_time:
            return time.time() - self.start_time
        return 0.0

    @property
    def rows_per_second(self) -> float:
        """Processing rate."""
        duration = self.duration_seconds
        if duration > 0:
            return self.rows_processed / duration
        return 0.0

    @property
    def percent_complete(self) -> float:
        """Completion percentage."""
        if self.rows_total > 0:
            return (self.rows_processed / self.rows_total) * 100
        return 0.0


# Progress callback type
ProgressCallback = Callable[[SyncStats], None]


class SyncEngine:
    """
    Main sync engine coordinating all operations.
    
    Example:
        engine = SyncEngine(settings)
        
        # Push local to D1
        stats = await engine.push(
            source=Path("database.db"),
            on_progress=lambda s: print(f"{s.percent_complete:.1f}%")
        )
        
        # Pull from D1 to local
        stats = await engine.pull(destination=Path("backup.db"))
    """

    def __init__(self, settings: Settings) -> None:
        """
        Initialize sync engine.
        
        Args:
            settings: Application settings
        """
        self.settings = settings
        self.chunker = SQLChunker(settings.limits)
        self.integrity = IntegrityChecker(settings.sync.checksum_algorithm)
        self.state_mgr = StateManager(
            settings.sync.state_file,
            settings.logging.failed_rows_file,
        )

    async def push(
        self,
        source: Path | str,
        on_progress: ProgressCallback | None = None,
    ) -> SyncStats:
        """
        Push data from local SQLite to Cloudflare D1.
        
        Args:
            source: Path to source SQLite database
            on_progress: Optional progress callback
        
        Returns:
            SyncStats with operation results
        """
        source_path = Path(source)
        stats = SyncStats(operation="push")
        stats.start_time = time.time()

        # Validate source
        if not source_path.exists():
            stats.errors.append(f"Source database not found: {source_path}")
            stats.end_time = time.time()
            return stats

        # Initialize state
        state = self.state_mgr.get_or_create_state(
            operation="push",
            source=str(source_path),
            destination=f"{self.settings.database_name}@cloudflare",
        )

        # Connect to source
        sqlite = SQLiteConnector(source_path, readonly=True, settings=self.settings)

        try:
            # Get tables to sync
            tables = sqlite.get_tables()
            
            # Filter tables
            if self.settings.sync.tables:
                tables = [t for t in tables if t.name in self.settings.sync.tables]
            
            tables = [
                t for t in tables
                if t.name not in self.settings.sync.exclude_tables
            ]

            stats.tables_total = len(tables)
            stats.rows_total = sum(t.row_count for t in tables)

            if on_progress:
                on_progress(stats)

            # Create D1 client
            async with create_d1_client(self.settings) as d1:
                for table in tables:
                    # Check if should process
                    if not self.state_mgr.should_process_table(table.name):
                        stats.tables_processed += 1
                        stats.rows_processed += table.row_count
                        continue

                    # Initialize table progress
                    self.state_mgr.init_table(table.name, table.row_count)
                    self.state_mgr.update_table_progress(
                        table.name, status="in_progress"
                    )

                    # Sync schema if enabled
                    if self.settings.sync.sync_schema:
                        await self._sync_table_schema(sqlite, d1, table.name)

                    # Get resume offset
                    start_offset = 0
                    if self.settings.sync.resume:
                        start_offset = self.state_mgr.get_resume_offset(table.name)

                    # Process rows in batches
                    table_rows_processed = 0
                    table_rows_failed = 0

                    columns = [c.name for c in table.columns]

                    for batch in sqlite.iter_rows(
                        table.name,
                        batch_size=self.settings.limits.max_rows_per_batch,
                        offset=start_offset,
                        limit=self.settings.sync.limit,
                    ):
                        # Chunk batch for D1 size limits
                        for chunk in self.chunker.chunk_rows(
                            table.name,
                            columns,
                            batch.rows,
                            replace=self.settings.sync.overwrite,
                            start_offset=batch.offset,
                        ):
                            try:
                                if not self.settings.sync.dry_run:
                                    # Use direct execute (more reliable than bulk import)
                                    result = await d1.execute(chunk.sql)
                                    
                                    if result.success:
                                        table_rows_processed += chunk.row_count
                                        stats.bytes_transferred += chunk.byte_size
                                    else:
                                        table_rows_failed += chunk.row_count
                                        stats.errors.append(
                                            f"{table.name}@{chunk.start_offset}: "
                                            f"{result.error}"
                                        )
                                else:
                                    # Dry run - just count
                                    table_rows_processed += chunk.row_count
                                    stats.bytes_transferred += chunk.byte_size

                            except Exception as e:
                                table_rows_failed += chunk.row_count
                                stats.errors.append(
                                    f"{table.name}@{chunk.start_offset}: {e}"
                                )

                        # Update progress
                        self.state_mgr.update_table_progress(
                            table.name,
                            processed=table_rows_processed,
                            failed=table_rows_failed,
                            last_offset=batch.offset + len(batch),
                        )

                        stats.rows_processed = sum(
                            p.processed_rows
                            for p in self.state_mgr.state.tables.values()
                        ) if self.state_mgr.state else 0
                        stats.rows_failed = table_rows_failed

                        if on_progress:
                            on_progress(stats)

                        # Save state periodically
                        self.state_mgr.save()

                    # Mark table complete
                    status = "completed" if table_rows_failed == 0 else "failed"
                    self.state_mgr.update_table_progress(table.name, status=status)
                    stats.tables_processed += 1

                # Verify if enabled
                if self.settings.sync.verify_after_sync and not self.settings.sync.dry_run:
                    await self._verify_sync(sqlite, d1, tables, stats)

        finally:
            sqlite.close()
            self.state_mgr.mark_sync_complete(
                "completed" if stats.rows_failed == 0 else "failed"
            )

        stats.end_time = time.time()
        return stats

    async def pull(
        self,
        destination: Path | str,
        on_progress: ProgressCallback | None = None,
    ) -> SyncStats:
        """
        Pull data from Cloudflare D1 to local SQLite.
        
        Args:
            destination: Path to destination SQLite database
            on_progress: Optional progress callback
        
        Returns:
            SyncStats with operation results
        """
        dest_path = Path(destination)
        stats = SyncStats(operation="pull")
        stats.start_time = time.time()

        # Initialize state
        state = self.state_mgr.get_or_create_state(
            operation="pull",
            source=f"{self.settings.database_name}@cloudflare",
            destination=str(dest_path),
        )

        # Create destination database if needed
        sqlite = SQLiteConnector(dest_path, readonly=False, settings=self.settings)

        try:
            async with create_d1_client(self.settings) as d1:
                # Get tables from D1
                table_names = await d1.get_tables()
                
                # Filter tables
                if self.settings.sync.tables:
                    table_names = [
                        t for t in table_names
                        if t in self.settings.sync.tables
                    ]
                
                table_names = [
                    t for t in table_names
                    if t not in self.settings.sync.exclude_tables
                ]

                stats.tables_total = len(table_names)

                for table_name in table_names:
                    # Get row count
                    row_count = await d1.get_table_count(table_name)
                    stats.rows_total += row_count
                    
                    self.state_mgr.init_table(table_name, row_count)
                    self.state_mgr.update_table_progress(
                        table_name, status="in_progress"
                    )

                    if on_progress:
                        on_progress(stats)

                    # Fetch and insert in batches
                    # Note: This is a simplified implementation
                    # Full implementation would handle pagination
                    result = await d1.execute(
                        f'SELECT * FROM "{table_name}" LIMIT 1000'
                    )

                    if result.success and result.results:
                        columns = list(result.results[0].keys())
                        rows = [tuple(r.values()) for r in result.results]
                        
                        if not self.settings.sync.dry_run:
                            # Sync schema first
                            # Then insert rows
                            sqlite.insert_rows(
                                table_name,
                                columns,
                                rows,
                                replace=self.settings.sync.overwrite,
                            )

                        stats.rows_processed += len(rows)

                    self.state_mgr.update_table_progress(
                        table_name,
                        processed=row_count,
                        status="completed",
                    )
                    stats.tables_processed += 1

                    if on_progress:
                        on_progress(stats)

        finally:
            sqlite.close()
            self.state_mgr.mark_sync_complete()

        stats.end_time = time.time()
        return stats

    async def _sync_table_schema(
        self,
        source: SQLiteConnector,
        d1: D1Client,
        table_name: str,
    ) -> bool:
        """Sync table schema to D1."""
        try:
            create_sql = source.get_create_statement(table_name)
            if create_sql:
                # Add IF NOT EXISTS
                create_sql = create_sql.replace(
                    f'CREATE TABLE "{table_name}"',
                    f'CREATE TABLE IF NOT EXISTS "{table_name}"',
                ).replace(
                    f"CREATE TABLE {table_name}",
                    f"CREATE TABLE IF NOT EXISTS {table_name}",
                )
                
                result = await d1.execute(create_sql)
                return result.success
            return True
        except Exception:
            return False

    async def _verify_sync(
        self,
        source: SQLiteConnector,
        d1: D1Client,
        tables: list[Any],
        stats: SyncStats,
    ) -> None:
        """Verify data integrity after sync."""
        for table in tables:
            source_count = table.row_count
            dest_count = await d1.get_table_count(table.name)
            
            if source_count != dest_count:
                stats.errors.append(
                    f"Row count mismatch in {table.name}: "
                    f"source={source_count}, dest={dest_count}"
                )

    def get_state_summary(self) -> dict[str, Any]:
        """Get summary of current sync state."""
        return self.state_mgr.get_summary()

    def clear_state(self) -> None:
        """Clear all sync state (for fresh start)."""
        self.state_mgr.clear_state()
