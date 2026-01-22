"""
Slug Sync Engine - Sync slug fixer changes to D1.

Coordinates the sync of slug fixes from local SQLite to Cloudflare D1:
- Adds slug_old column if missing
- Queries only changed rows (slug_old IS NOT NULL)
- Generates UPDATE statements
- Executes in batches with progress tracking
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from d1_sync.config import Settings
from d1_sync.connectors.d1_client import D1Client, create_d1_client


@dataclass
class SlugSyncStats:
    """Statistics for slug sync operation."""

    rows_to_sync: int = 0
    rows_updated: int = 0
    rows_failed: int = 0
    column_added: bool = False
    start_time: float = 0.0
    end_time: float = 0.0
    errors: list[str] = field(default_factory=list)

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
            return self.rows_updated / duration
        return 0.0


# Progress callback type
SlugProgressCallback = Callable[[SlugSyncStats], None]


class SlugSyncEngine:
    """
    Engine for syncing slug fixes to Cloudflare D1.

    Example:
        from d1_sync.config import Settings
        engine = SlugSyncEngine(settings)

        stats = await engine.sync(
            source=Path("tutorials.db"),
            table="tutorials",
            on_progress=lambda s: print(f"{s.rows_updated}/{s.rows_to_sync}")
        )
    """

    def __init__(self, settings: Settings) -> None:
        """
        Initialize slug sync engine.

        Args:
            settings: Application settings
        """
        self.settings = settings

    async def sync(
        self,
        source: Path | str,
        table: str = "tutorials",
        on_progress: SlugProgressCallback | None = None,
    ) -> SlugSyncStats:
        """
        Sync slug fixes to D1.

        Args:
            source: Path to local SQLite database
            table: Table name (default: tutorials)
            on_progress: Optional progress callback

        Returns:
            SlugSyncStats with operation results
        """
        source_path = Path(source)
        stats = SlugSyncStats(start_time=time.time())

        # Validate source
        if not source_path.exists():
            stats.errors.append(f"Source database not found: {source_path}")
            stats.end_time = time.time()
            return stats

        # Connect to local database for reading
        try:
            sqlite_conn = sqlite3.connect(str(source_path))
            sqlite_conn.row_factory = sqlite3.Row
        except Exception as e:
            stats.errors.append(f"Failed to open local database: {e}")
            stats.end_time = time.time()
            return stats

        try:
            async with create_d1_client(self.settings) as d1:
                # Step 1: Ensure slug_old column exists in D1
                stats.column_added = await self._ensure_slug_old_column(d1, table)

                # Step 2: Count rows to sync
                stats.rows_to_sync = self._count_rows_to_sync(sqlite_conn, table)

                if stats.rows_to_sync == 0:
                    stats.end_time = time.time()
                    return stats

                # Step 3: Fetch changed rows from local DB
                rows = self._fetch_changed_rows(sqlite_conn, table)

                # Step 4: Batch update D1
                batch_size = 50  # Smaller batches for UPDATE with CASE expressions
                total_batches = (len(rows) + batch_size - 1) // batch_size

                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    batch_num = i // batch_size

                    # Generate UPDATE statement
                    update_sql = self._generate_update_sql(table, batch)

                    # Execute
                    if not self.settings.sync.dry_run:
                        result = await d1.execute(update_sql)

                        if result.success:
                            stats.rows_updated += len(batch)
                        else:
                            stats.rows_failed += len(batch)
                            error_msg = result.error or "Unknown error"
                            stats.errors.append(
                                f"Batch {batch_num + 1}/{total_batches}: {error_msg}"
                            )
                    else:
                        # Dry run - just count
                        stats.rows_updated += len(batch)

                    # Update progress
                    if on_progress:
                        on_progress(stats)

        finally:
            sqlite_conn.close()

        stats.end_time = time.time()
        return stats

    async def _ensure_slug_old_column(
        self, d1: D1Client, table: str
    ) -> bool:
        """
        Add slug_old column to D1 if it doesn't exist.

        Uses pragma_table_info which is available in D1 (SQLite-based).

        Args:
            d1: D1 client
            table: Table name

        Returns:
            True if column was added, False if it already existed
        """
        # Check if column exists
        check_sql = f"""
            SELECT COUNT(*) as count
            FROM pragma_table_info('{table}')
            WHERE name = 'slug_old'
        """

        result = await d1.execute(check_sql)

        if result.success and result.results:
            count = result.results[0].get("count", 0)
            if count > 0:
                return False  # Column already exists

        # Add the column
        alter_sql = f'ALTER TABLE "{table}" ADD COLUMN "slug_old" TEXT'
        result = await d1.execute(alter_sql)

        return result.success

    def _count_rows_to_sync(self, conn: sqlite3.Connection, table: str) -> int:
        """
        Count rows where slug_old IS NOT NULL.

        Args:
            conn: SQLite connection
            table: Table name

        Returns:
            Number of rows to sync
        """
        try:
            cursor = conn.cursor()
            cursor.execute(
                f'SELECT COUNT(*) as count FROM "{table}" WHERE slug_old IS NOT NULL'
            )
            row = cursor.fetchone()
            return row["count"] if row else 0
        except Exception as e:
            # Table might not have slug_old column yet
            return 0

    def _fetch_changed_rows(
        self, conn: sqlite3.Connection, table: str
    ) -> list[dict[str, Any]]:
        """
        Fetch rows where slug_old IS NOT NULL.

        Args:
            conn: SQLite connection
            table: Table name

        Returns:
            List of row dictionaries with id, slug, slug_old
        """
        try:
            cursor = conn.cursor()
            cursor.execute(
                f'''
                SELECT id, slug, slug_old
                FROM "{table}"
                WHERE slug_old IS NOT NULL
                ORDER BY id
                '''
            )
            return [dict(row) for row in cursor.fetchall()]
        except Exception:
            return []

    def _generate_update_sql(self, table: str, rows: list[dict[str, Any]]) -> str:
        """
        Generate batch UPDATE statement using CASE expression.

        D1 supports multi-value UPDATE with CASE expression:

        UPDATE tutorials
        SET slug = CASE id
            WHEN 'id1' THEN 'slug1'
            WHEN 'id2' THEN 'slug2'
            END,
            slug_old = CASE id
            WHEN 'id1' THEN 'old_slug1'
            WHEN 'id2' THEN 'old_slug2'
            END
        WHERE id IN ('id1', 'id2')

        Args:
            table: Table name
            rows: List of row dictionaries

        Returns:
            SQL UPDATE statement
        """
        if not rows:
            return ""

        # Build list of IDs for WHERE clause
        id_list = ", ".join(f"'{self._escape(str(r['id']))}'" for r in rows)

        # Build CASE expression for slug
        slug_case_parts = [f"WHEN '{self._escape(str(r['id']))}' THEN '{self._escape(r['slug'])}'" for r in rows]
        slug_case = "slug = CASE id\n    " + "\n    ".join(slug_case_parts)

        # Build CASE expression for slug_old
        slug_old_case_parts = [
            f"WHEN '{self._escape(str(r['id']))}' THEN '{self._escape(r['slug_old'])}'" for r in rows
        ]
        slug_old_case = "slug_old = CASE id\n    " + "\n    ".join(slug_old_case_parts)

        return f"""
            UPDATE "{table}"
            SET {slug_case}
                END,
                {slug_old_case}
            END
            WHERE id IN ({id_list})
        """.strip()

    def _escape(self, value: str) -> str:
        """
        Escape single quotes for SQL.

        Args:
            value: String value to escape

        Returns:
            Escaped string
        """
        return value.replace("'", "''")
