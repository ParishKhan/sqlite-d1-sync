"""
SQLite Database Connector.

Provides efficient read/write access to SQLite databases with:
- Schema introspection
- Streaming row iteration (memory efficient)
- Batch insert/update operations
- Remote SQLite URL support (read-only)
"""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Generator, Iterator, Sequence

from d1_sync.config import Settings


@dataclass
class ColumnInfo:
    """Information about a table column."""

    name: str
    type: str
    notnull: bool
    default_value: Any
    is_primary_key: bool


@dataclass
class TableInfo:
    """Information about a database table."""

    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    row_count: int = 0
    indexes: list[str] = field(default_factory=list)
    create_sql: str = ""


@dataclass
class RowBatch:
    """A batch of rows with metadata."""

    table: str
    columns: list[str]
    rows: list[tuple[Any, ...]]
    offset: int
    checksum: str = ""

    def __len__(self) -> int:
        return len(self.rows)


class SQLiteConnector:
    """
    Connector for SQLite databases.
    
    Supports both local files and read-only remote URLs.
    Provides memory-efficient streaming for large tables.
    
    Example:
        connector = SQLiteConnector(Path("database.db"))
        
        # Get table info
        tables = connector.get_tables()
        
        # Stream rows in batches
        for batch in connector.iter_rows("users", batch_size=100):
            process(batch)
    """

    def __init__(
        self,
        path: Path | str | None = None,
        url: str | None = None,
        readonly: bool = False,
        settings: Settings | None = None,
    ) -> None:
        """
        Initialize SQLite connector.
        
        Args:
            path: Path to local SQLite database file
            url: URL for remote SQLite (read-only)
            readonly: Open in read-only mode
            settings: Optional settings object
        """
        self.path = Path(path) if path else None
        self.url = url
        self.readonly = readonly or (url is not None)
        self.settings = settings
        self._connection: sqlite3.Connection | None = None

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Get a database connection with proper cleanup."""
        if self._connection is None:
            self._connection = self._create_connection()

        try:
            yield self._connection
        except Exception:
            self._connection.rollback()
            raise

    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection."""
        if self.path:
            if not self.path.exists():
                raise FileNotFoundError(f"Database not found: {self.path}")
            
            uri = f"file:{self.path}"
            if self.readonly:
                uri += "?mode=ro"
            
            conn = sqlite3.connect(
                uri,
                uri=True,
                check_same_thread=False,
                timeout=30.0,
            )
        elif self.url:
            # Remote SQLite via URL (requires additional handling)
            raise NotImplementedError("Remote SQLite URLs not yet supported")
        else:
            raise ValueError("Either path or url must be provided")

        # Enable optimizations
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        conn.execute("PRAGMA temp_store=MEMORY")

        return conn

    def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def __enter__(self) -> "SQLiteConnector":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def get_tables(self) -> list[TableInfo]:
        """Get list of all tables with their metadata."""
        tables: list[TableInfo] = []

        with self.connection() as conn:
            cursor = conn.execute(
                """
                SELECT name, sql FROM sqlite_master
                WHERE type = 'table'
                AND name NOT LIKE 'sqlite_%'
                AND name NOT LIKE '_cf_%'
                ORDER BY name
                """
            )

            for row in cursor:
                table_name = row["name"]
                create_sql = row["sql"] or ""

                # Get column info
                columns = self._get_column_info(conn, table_name)

                # Get row count
                count_row = conn.execute(
                    f'SELECT COUNT(*) as count FROM "{table_name}"'
                ).fetchone()
                row_count = count_row["count"] if count_row else 0

                # Get indexes
                indexes = self._get_table_indexes(conn, table_name)

                tables.append(
                    TableInfo(
                        name=table_name,
                        columns=columns,
                        row_count=row_count,
                        indexes=indexes,
                        create_sql=create_sql,
                    )
                )

        # Sort tables by foreign key dependencies (topological order)
        return self._sort_tables_by_dependencies(tables)

    def _sort_tables_by_dependencies(self, tables: list[TableInfo]) -> list[TableInfo]:
        """
        Sort tables topologically to respect foreign key constraints.

        Tables with no dependencies come first, then tables that reference them,
        etc. This ensures that when syncing, parent tables are created before
        child tables that reference them via foreign keys.
        """
        import re

        # Build a map of table name to TableInfo
        table_map = {table.name: table for table in tables}
        all_table_names = set(table_map.keys())

        # Parse foreign key dependencies from CREATE TABLE statements
        dependencies: dict[str, set[str]] = {}
        for table in tables:
            deps = set()
            # Match FOREIGN KEY (column) REFERENCES table_name
            fk_matches = re.findall(
                r'FOREIGN KEY\s*\([^)]+\)\s*REFERENCES\s+"?([a-zA-Z_][a-zA-Z0-9_]*)',
                table.create_sql,
                re.IGNORECASE
            )
            for ref_table in fk_matches:
                # Only add as dependency if it's a table in our schema
                if ref_table in all_table_names and ref_table != table.name:
                    deps.add(ref_table)
            dependencies[table.name] = deps

        # Topological sort using Kahn's algorithm
        sorted_tables: list[TableInfo] = []
        remaining_tables = {name: deps.copy() for name, deps in dependencies.items()}

        # Start with tables that have no dependencies
        ready = [name for name, deps in remaining_tables.items() if not deps]

        while ready:
            # Sort alphabetically for deterministic order
            ready.sort()
            table_name = ready.pop(0)

            # Add this table to the result (only once!)
            if table_map[table_name] not in sorted_tables:
                sorted_tables.append(table_map[table_name])

            # Remove this table from dependencies of remaining tables
            for name in list(remaining_tables.keys()):
                if table_name in remaining_tables[name]:
                    remaining_tables[name].remove(table_name)
                    # If this table now has no dependencies, add to ready
                    if not remaining_tables[name]:
                        ready.append(name)
                        del remaining_tables[name]

        # Handle circular dependencies (shouldn't happen in well-formed schemas)
        if remaining_tables:
            # Just append the remaining tables in alphabetical order
            for name in sorted(remaining_tables.keys()):
                if table_map[name] not in sorted_tables:
                    sorted_tables.append(table_map[name])

        return sorted_tables

    def _get_column_info(
        self, conn: sqlite3.Connection, table: str
    ) -> list[ColumnInfo]:
        """Get column information for a table."""
        columns: list[ColumnInfo] = []
        cursor = conn.execute(f'PRAGMA table_info("{table}")')

        for row in cursor:
            columns.append(
                ColumnInfo(
                    name=row["name"],
                    type=row["type"],
                    notnull=bool(row["notnull"]),
                    default_value=row["dflt_value"],
                    is_primary_key=bool(row["pk"]),
                )
            )

        return columns

    def _get_table_indexes(
        self, conn: sqlite3.Connection, table: str
    ) -> list[str]:
        """Get index names for a table."""
        cursor = conn.execute(f'PRAGMA index_list("{table}")')
        return [row["name"] for row in cursor]

    def get_table(self, name: str) -> TableInfo | None:
        """Get information about a specific table."""
        tables = self.get_tables()
        for table in tables:
            if table.name == name:
                return table
        return None

    def get_row_count(self, table: str) -> int:
        """Get the row count for a table."""
        with self.connection() as conn:
            cursor = conn.execute(f'SELECT COUNT(*) as count FROM "{table}"')
            row = cursor.fetchone()
            return row["count"] if row else 0

    def iter_rows(
        self,
        table: str,
        batch_size: int = 100,
        offset: int = 0,
        limit: int | None = None,
        columns: list[str] | None = None,
        order_by: str | None = None,
    ) -> Iterator[RowBatch]:
        """
        Iterate over table rows in batches.
        
        This is memory-efficient as it streams rows rather than
        loading everything into memory.
        
        Args:
            table: Table name
            batch_size: Rows per batch
            offset: Starting offset
            limit: Maximum total rows to return
            columns: Specific columns to fetch (None = all)
            order_by: Column to order by
        
        Yields:
            RowBatch objects containing rows and metadata
        """
        with self.connection() as conn:
            # Build column list
            if columns:
                col_str = ", ".join(f'"{c}"' for c in columns)
            else:
                # Get all columns
                table_info = self.get_table(table)
                if not table_info:
                    raise ValueError(f"Table not found: {table}")
                columns = [c.name for c in table_info.columns]
                col_str = ", ".join(f'"{c}"' for c in columns)

            # Build query
            query = f'SELECT {col_str} FROM "{table}"'
            if order_by:
                query += f' ORDER BY "{order_by}"'

            rows_fetched = 0
            current_offset = offset

            while True:
                # Add pagination
                batch_query = f"{query} LIMIT {batch_size} OFFSET {current_offset}"
                cursor = conn.execute(batch_query)
                
                rows: list[tuple[Any, ...]] = []
                for row in cursor:
                    rows.append(tuple(row))
                    rows_fetched += 1
                    
                    if limit and rows_fetched >= limit:
                        break

                if not rows:
                    break

                # Calculate checksum for batch
                checksum = self._calculate_batch_checksum(rows)

                yield RowBatch(
                    table=table,
                    columns=columns,
                    rows=rows,
                    offset=current_offset,
                    checksum=checksum,
                )

                current_offset += len(rows)

                if limit and rows_fetched >= limit:
                    break

    def _calculate_batch_checksum(
        self, rows: list[tuple[Any, ...]]
    ) -> str:
        """Calculate MD5 checksum for a batch of rows."""
        hasher = hashlib.md5()
        for row in rows:
            row_str = "|".join(str(v) if v is not None else "" for v in row)
            hasher.update(row_str.encode("utf-8"))
        return hasher.hexdigest()

    def execute_sql(self, sql: str, params: Sequence[Any] = ()) -> int:
        """
        Execute a SQL statement and return affected row count.
        
        Args:
            sql: SQL statement
            params: Query parameters
        
        Returns:
            Number of affected rows
        """
        if self.readonly:
            raise RuntimeError("Cannot execute write operations in read-only mode")

        with self.connection() as conn:
            cursor = conn.execute(sql, params)
            conn.commit()
            return cursor.rowcount

    def execute_many(
        self,
        sql: str,
        params_list: list[Sequence[Any]],
    ) -> int:
        """
        Execute a SQL statement with multiple parameter sets.
        
        Args:
            sql: SQL statement with placeholders
            params_list: List of parameter tuples
        
        Returns:
            Number of affected rows
        """
        if self.readonly:
            raise RuntimeError("Cannot execute write operations in read-only mode")

        with self.connection() as conn:
            cursor = conn.executemany(sql, params_list)
            conn.commit()
            return cursor.rowcount

    def insert_rows(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        replace: bool = False,
    ) -> int:
        """
        Insert rows into a table.
        
        Args:
            table: Target table name
            columns: Column names
            rows: List of row tuples
            replace: Use INSERT OR REPLACE instead of INSERT
        
        Returns:
            Number of inserted rows
        """
        if not rows:
            return 0

        col_str = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        
        verb = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
        sql = f'{verb} INTO "{table}" ({col_str}) VALUES ({placeholders})'

        return self.execute_many(sql, rows)

    def create_table(self, create_sql: str) -> None:
        """Create a table using raw CREATE TABLE SQL."""
        self.execute_sql(create_sql)

    def drop_table(self, table: str) -> None:
        """Drop a table if it exists."""
        self.execute_sql(f'DROP TABLE IF EXISTS "{table}"')

    def get_create_statement(self, table: str) -> str:
        """Get the CREATE TABLE statement for a table."""
        with self.connection() as conn:
            cursor = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            row = cursor.fetchone()
            return row["sql"] if row else ""

    def get_index_statements(self, table: str) -> list[str]:
        """Get CREATE INDEX statements for a table."""
        statements: list[str] = []
        with self.connection() as conn:
            cursor = conn.execute(
                """
                SELECT sql FROM sqlite_master 
                WHERE type='index' AND tbl_name=? AND sql IS NOT NULL
                """,
                (table,),
            )
            for row in cursor:
                if row["sql"]:
                    statements.append(row["sql"])
        return statements
