"""Tests for SQLite connector."""

import sqlite3
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from d1_sync.connectors.sqlite import SQLiteConnector, TableInfo, RowBatch


@pytest.fixture
def sample_db(tmp_path: Path) -> Path:
    """Create a sample SQLite database for testing."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create test table
    cursor.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            active INTEGER DEFAULT 1
        )
    """)
    
    # Insert test data
    test_data = [
        (1, "Alice", "alice@example.com", 1),
        (2, "Bob", "bob@example.com", 1),
        (3, "Charlie", "charlie@example.com", 0),
        (4, "Diana", "diana@example.com", 1),
        (5, "Eve", "eve@example.com", 1),
    ]
    cursor.executemany(
        "INSERT INTO users (id, name, email, active) VALUES (?, ?, ?, ?)",
        test_data,
    )
    
    # Create index
    cursor.execute("CREATE INDEX idx_users_email ON users(email)")
    
    conn.commit()
    conn.close()
    
    return db_path


class TestSQLiteConnector:
    """Tests for SQLiteConnector class."""

    def test_connection(self, sample_db: Path) -> None:
        """Test database connection."""
        with SQLiteConnector(sample_db) as conn:
            assert conn.path == sample_db

    def test_get_tables(self, sample_db: Path) -> None:
        """Test getting table information."""
        with SQLiteConnector(sample_db) as conn:
            tables = conn.get_tables()
            assert len(tables) == 1
            assert tables[0].name == "users"
            assert tables[0].row_count == 5
            assert len(tables[0].columns) == 4

    def test_get_table(self, sample_db: Path) -> None:
        """Test getting specific table info."""
        with SQLiteConnector(sample_db) as conn:
            table = conn.get_table("users")
            assert table is not None
            assert table.name == "users"
            
            # Non-existent table
            assert conn.get_table("nonexistent") is None

    def test_get_row_count(self, sample_db: Path) -> None:
        """Test row count retrieval."""
        with SQLiteConnector(sample_db) as conn:
            count = conn.get_row_count("users")
            assert count == 5

    def test_iter_rows(self, sample_db: Path) -> None:
        """Test row iteration."""
        with SQLiteConnector(sample_db) as conn:
            batches = list(conn.iter_rows("users", batch_size=2))
            
            # Should have 3 batches (2+2+1 rows)
            assert len(batches) == 3
            assert len(batches[0].rows) == 2
            assert len(batches[1].rows) == 2
            assert len(batches[2].rows) == 1
            
            # Check batch metadata
            assert batches[0].table == "users"
            assert len(batches[0].columns) == 4

    def test_iter_rows_with_limit(self, sample_db: Path) -> None:
        """Test row iteration with limit."""
        with SQLiteConnector(sample_db) as conn:
            batches = list(conn.iter_rows("users", batch_size=10, limit=3))
            
            total_rows = sum(len(b.rows) for b in batches)
            assert total_rows == 3

    def test_iter_rows_with_offset(self, sample_db: Path) -> None:
        """Test row iteration with offset."""
        with SQLiteConnector(sample_db) as conn:
            batches = list(conn.iter_rows("users", batch_size=10, offset=2))
            
            total_rows = sum(len(b.rows) for b in batches)
            assert total_rows == 3  # 5 total - 2 offset = 3

    def test_batch_checksum(self, sample_db: Path) -> None:
        """Test batch checksum calculation."""
        with SQLiteConnector(sample_db) as conn:
            batches = list(conn.iter_rows("users", batch_size=5))
            
            # Checksum should be consistent
            assert batches[0].checksum
            assert len(batches[0].checksum) == 32  # MD5 hex length

    def test_read_only_mode(self, sample_db: Path) -> None:
        """Test read-only mode prevents writes."""
        with SQLiteConnector(sample_db, readonly=True) as conn:
            with pytest.raises(RuntimeError, match="read-only"):
                conn.execute_sql("INSERT INTO users (name) VALUES ('Test')")

    def test_insert_rows(self, sample_db: Path) -> None:
        """Test row insertion."""
        with SQLiteConnector(sample_db, readonly=False) as conn:
            columns = ["name", "email", "active"]
            rows = [
                ("Frank", "frank@example.com", 1),
                ("Grace", "grace@example.com", 0),
            ]
            
            count = conn.insert_rows("users", columns, rows)
            assert count == 2
            
            # Verify insertion
            assert conn.get_row_count("users") == 7

    def test_get_create_statement(self, sample_db: Path) -> None:
        """Test getting CREATE TABLE statement."""
        with SQLiteConnector(sample_db) as conn:
            sql = conn.get_create_statement("users")
            assert "CREATE TABLE" in sql
            assert "users" in sql
            assert "id INTEGER PRIMARY KEY" in sql

    def test_get_index_statements(self, sample_db: Path) -> None:
        """Test getting index statements."""
        with SQLiteConnector(sample_db) as conn:
            indexes = conn.get_index_statements("users")
            assert len(indexes) == 1
            assert "idx_users_email" in indexes[0]


class TestTableInfo:
    """Tests for TableInfo dataclass."""

    def test_table_info_creation(self) -> None:
        """Test TableInfo creation."""
        info = TableInfo(
            name="test_table",
            row_count=100,
            create_sql="CREATE TABLE test_table (id INTEGER)",
        )
        assert info.name == "test_table"
        assert info.row_count == 100
        assert len(info.columns) == 0
        assert len(info.indexes) == 0
