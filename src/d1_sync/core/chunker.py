"""
SQL Chunker - Size-aware SQL statement builder.

Handles the critical task of building SQL statements that respect
Cloudflare D1's 100KB statement size limit while maximizing efficiency.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterator

from d1_sync.config import Limits


@dataclass
class InsertChunk:
    """A chunk of INSERT statements ready for execution."""

    table: str
    sql: str
    row_count: int
    byte_size: int
    start_offset: int
    end_offset: int


class SQLChunker:
    """
    Size-aware SQL statement builder.
    
    Builds INSERT statements that respect size limits, automatically
    splitting large datasets into appropriately sized chunks.
    
    Key safety features:
    - Exact byte size calculation (not estimation)
    - Safety margin to prevent edge cases
    - Proper SQL escaping (no injection or corruption)
    - Binary data handling
    
    Example:
        chunker = SQLChunker(limits)
        
        for chunk in chunker.chunk_rows("users", columns, rows):
            client.execute(chunk.sql)
    """

    def __init__(self, limits: Limits) -> None:
        """
        Initialize chunker with size limits.
        
        Args:
            limits: API limits from configuration
        """
        self.limits = limits
        self.max_size = int(
            limits.max_sql_length_bytes * limits.batch_safety_margin
        )

    def escape_value(self, value: Any) -> str:
        """
        Escape a value for safe SQL insertion.
        
        Handles all Python types and converts them to proper SQL literals.
        This is THE critical function for data integrity.
        
        Args:
            value: Any Python value
        
        Returns:
            SQL-safe string representation
        """
        if value is None:
            return "NULL"
        
        if isinstance(value, bool):
            return "1" if value else "0"
        
        if isinstance(value, (int, float)):
            # Handle special float values
            if isinstance(value, float):
                if value != value:  # NaN check
                    return "NULL"
                if value == float("inf") or value == float("-inf"):
                    return "NULL"
            return str(value)
        
        if isinstance(value, bytes):
            # Encode binary data as hex
            return f"X'{value.hex()}'"
        
        # Convert to string and escape
        str_val = str(value)
        
        # Escape single quotes by doubling them (SQL standard)
        escaped = str_val.replace("'", "''")
        
        # Handle control characters that could break SQL
        # Replace with safe representations
        escaped = escaped.replace("\x00", "")  # Remove null bytes
        
        return f"'{escaped}'"

    def calculate_row_size(
        self,
        columns: list[str],
        row: tuple[Any, ...],
    ) -> int:
        """
        Calculate the exact byte size of a row in SQL format.
        
        Args:
            columns: Column names
            row: Row values
        
        Returns:
            Size in bytes
        """
        values = [self.escape_value(v) for v in row]
        row_str = f"({', '.join(values)})"
        return len(row_str.encode("utf-8"))

    def build_insert_statement(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        replace: bool = False,
    ) -> str:
        """
        Build a complete INSERT statement for the given rows.
        
        Args:
            table: Target table name
            columns: Column names
            rows: List of row tuples
            replace: Use INSERT OR REPLACE
        
        Returns:
            Complete SQL INSERT statement
        """
        if not rows:
            return ""

        verb = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
        col_str = ", ".join(f'"{c}"' for c in columns)

        value_rows = []
        for row in rows:
            values = [self.escape_value(v) for v in row]
            value_rows.append(f"({', '.join(values)})")

        return f'{verb} INTO "{table}" ({col_str}) VALUES\n{",\n".join(value_rows)};'

    def chunk_rows(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        replace: bool = False,
        start_offset: int = 0,
    ) -> Iterator[InsertChunk]:
        """
        Split rows into size-appropriate chunks.
        
        Yields INSERT statements that respect the size limit.
        
        Args:
            table: Target table name
            columns: Column names
            rows: All rows to insert
            replace: Use INSERT OR REPLACE
            start_offset: Starting row offset
        
        Yields:
            InsertChunk objects
        """
        if not rows:
            return

        verb = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
        col_str = ", ".join(f'"{c}"' for c in columns)
        
        # Calculate base statement overhead
        base_stmt = f'{verb} INTO "{table}" ({col_str}) VALUES\n;'
        base_size = len(base_stmt.encode("utf-8"))

        current_rows: list[tuple[Any, ...]] = []
        current_size = base_size
        chunk_start = start_offset

        for i, row in enumerate(rows):
            row_sql = self._format_row(row)
            row_size = len(row_sql.encode("utf-8"))
            
            # Add comma and newline overhead (except for first row)
            separator_size = 2 if current_rows else 0  # ",\n"
            total_row_size = row_size + separator_size

            # Check if adding this row would exceed limit
            if current_rows and current_size + total_row_size > self.max_size:
                # Yield current chunk
                sql = self.build_insert_statement(
                    table, columns, current_rows, replace
                )
                yield InsertChunk(
                    table=table,
                    sql=sql,
                    row_count=len(current_rows),
                    byte_size=len(sql.encode("utf-8")),
                    start_offset=chunk_start,
                    end_offset=chunk_start + len(current_rows) - 1,
                )

                # Start new chunk
                current_rows = [row]
                current_size = base_size + row_size
                chunk_start = start_offset + i
            else:
                current_rows.append(row)
                current_size += total_row_size

        # Yield remaining rows
        if current_rows:
            sql = self.build_insert_statement(table, columns, current_rows, replace)
            yield InsertChunk(
                table=table,
                sql=sql,
                row_count=len(current_rows),
                byte_size=len(sql.encode("utf-8")),
                start_offset=chunk_start,
                end_offset=start_offset + len(rows) - 1,
            )

    def _format_row(self, row: tuple[Any, ...]) -> str:
        """Format a single row as SQL values."""
        values = [self.escape_value(v) for v in row]
        return f"({', '.join(values)})"

    def estimate_chunks_needed(
        self,
        avg_row_size: int,
        total_rows: int,
    ) -> int:
        """
        Estimate how many chunks will be needed.
        
        Args:
            avg_row_size: Average row size in bytes
            total_rows: Total number of rows
        
        Returns:
            Estimated number of chunks
        """
        if avg_row_size <= 0 or total_rows <= 0:
            return 0

        # Account for overhead
        overhead_per_row = 3  # ",\n" plus buffer
        effective_row_size = avg_row_size + overhead_per_row
        
        rows_per_chunk = max(1, self.max_size // effective_row_size)
        return (total_rows + rows_per_chunk - 1) // rows_per_chunk
