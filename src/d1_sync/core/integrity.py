"""
Data Integrity Checker.

Provides verification of data integrity between source and destination:
- Row-level checksums
- Table-level verification
- Post-sync validation
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Sequence


@dataclass
class VerificationResult:
    """Result of a verification check."""

    table: str
    source_count: int
    dest_count: int
    source_checksum: str
    dest_checksum: str
    match: bool
    mismatches: list[dict[str, Any]]
    message: str


class IntegrityChecker:
    """
    Data integrity verification system.
    
    Provides checksums and verification to ensure data
    wasn't corrupted during transfer.
    
    Example:
        checker = IntegrityChecker("md5")
        
        # Calculate checksum for a row
        checksum = checker.row_checksum(row_values)
        
        # Verify entire table
        result = await checker.verify_table(source, dest, "users")
    """

    def __init__(self, algorithm: str = "md5") -> None:
        """
        Initialize integrity checker.
        
        Args:
            algorithm: Hash algorithm ("md5" or "sha256")
        """
        if algorithm not in ("md5", "sha256"):
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        self.algorithm = algorithm

    def _get_hasher(self) -> "hashlib._Hash":
        """Get a new hash object."""
        if self.algorithm == "sha256":
            return hashlib.sha256()
        return hashlib.md5()

    def row_checksum(self, values: Sequence[Any]) -> str:
        """
        Calculate checksum for a single row.
        
        Values are converted to a canonical string representation
        and then hashed.
        
        Args:
            values: Row values in column order
        
        Returns:
            Hex digest of checksum
        """
        hasher = self._get_hasher()
        
        # Create canonical string representation
        parts = []
        for val in values:
            if val is None:
                parts.append("\\N")  # NULL marker
            elif isinstance(val, bytes):
                parts.append(val.hex())
            elif isinstance(val, bool):
                parts.append("1" if val else "0")
            else:
                parts.append(str(val))
        
        row_str = "|".join(parts)
        hasher.update(row_str.encode("utf-8"))
        return hasher.hexdigest()

    def batch_checksum(
        self,
        rows: list[Sequence[Any]],
    ) -> str:
        """
        Calculate combined checksum for a batch of rows.
        
        Args:
            rows: List of row value sequences
        
        Returns:
            Hex digest of combined checksum
        """
        hasher = self._get_hasher()
        
        for row in rows:
            row_hash = self.row_checksum(row)
            hasher.update(row_hash.encode("utf-8"))
        
        return hasher.hexdigest()

    def table_checksum(
        self,
        rows: list[Sequence[Any]],
    ) -> str:
        """
        Calculate checksum for entire table.
        
        Rows should be in consistent order (e.g., by primary key).
        
        Args:
            rows: All rows in the table
        
        Returns:
            Hex digest of table checksum
        """
        return self.batch_checksum(rows)

    def compare_checksums(
        self,
        source: str,
        dest: str,
    ) -> bool:
        """Compare two checksums for equality."""
        return source.lower() == dest.lower()

    def find_mismatches(
        self,
        source_rows: list[Sequence[Any]],
        dest_rows: list[Sequence[Any]],
        key_column: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Find rows that differ between source and destination.
        
        Args:
            source_rows: Rows from source
            dest_rows: Rows from destination
            key_column: Index of primary key column
        
        Returns:
            List of mismatch details
        """
        mismatches: list[dict[str, Any]] = []
        
        # Build lookup for destination
        dest_map: dict[Any, tuple[Sequence[Any], str]] = {}
        for row in dest_rows:
            key = row[key_column]
            checksum = self.row_checksum(row)
            dest_map[key] = (row, checksum)
        
        # Check source rows
        for row in source_rows:
            key = row[key_column]
            src_checksum = self.row_checksum(row)
            
            if key not in dest_map:
                mismatches.append({
                    "type": "missing_in_dest",
                    "key": key,
                    "source_checksum": src_checksum,
                })
            else:
                dest_row, dest_checksum = dest_map[key]
                if src_checksum != dest_checksum:
                    mismatches.append({
                        "type": "checksum_mismatch",
                        "key": key,
                        "source_checksum": src_checksum,
                        "dest_checksum": dest_checksum,
                    })
                del dest_map[key]
        
        # Check for extra rows in destination
        for key, (row, checksum) in dest_map.items():
            mismatches.append({
                "type": "extra_in_dest",
                "key": key,
                "dest_checksum": checksum,
            })
        
        return mismatches
