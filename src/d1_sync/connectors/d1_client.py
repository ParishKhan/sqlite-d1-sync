"""
Cloudflare D1 REST API Client.

Provides a complete interface to Cloudflare D1 via REST API:
- Query execution (single and batch)
- Large data import via R2 upload workflow
- Rate limiting and retry logic
- Error handling with specific D1 error codes
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import httpx

from d1_sync.config import Limits, Settings


class D1Error(Exception):
    """Base exception for D1 API errors."""

    def __init__(
        self,
        message: str,
        code: str | None = None,
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


class D1RateLimitError(D1Error):
    """Raised when rate limit is exceeded."""

    def __init__(self, retry_after: int = 60) -> None:
        super().__init__(f"Rate limit exceeded. Retry after {retry_after}s")
        self.retry_after = retry_after


class D1StatementTooLongError(D1Error):
    """Raised when SQL statement exceeds size limit."""

    pass


class D1QueryTimeoutError(D1Error):
    """Raised when query exceeds time limit."""

    pass


class ImportStatus(str, Enum):
    """Status of a D1 import operation."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETE = "complete"
    FAILED = "failed"


@dataclass
class QueryResult:
    """Result of a D1 query."""

    success: bool
    results: list[dict[str, Any]] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    rows_read: int = 0
    rows_written: int = 0
    duration_ms: float = 0.0


@dataclass
class ImportResult:
    """Result of a D1 bulk import operation."""

    success: bool
    status: ImportStatus
    rows_written: int = 0
    error: str | None = None
    filename: str | None = None


class D1Client:
    """
    Cloudflare D1 REST API client.
    
    Provides both synchronous and async methods for interacting
    with D1 databases via the Cloudflare REST API.
    
    Example:
        client = D1Client(
            account_id="your-account-id",
            database_id="your-database-id",
            api_token="your-api-token",
        )
        
        # Execute a query
        result = await client.execute("SELECT * FROM users LIMIT 10")
        
        # Bulk import
        result = await client.import_sql(sql_content)
    """

    BASE_URL = "https://api.cloudflare.com/client/v4"

    def __init__(
        self,
        account_id: str,
        database_id: str,
        api_token: str,
        limits: Limits | None = None,
        settings: Settings | None = None,
    ) -> None:
        """
        Initialize D1 client.
        
        Args:
            account_id: Cloudflare account ID
            database_id: D1 database ID (UUID)
            api_token: Cloudflare API token
            limits: API limits (optional, uses defaults)
            settings: Full settings object (optional)
        """
        self.account_id = account_id
        self.database_id = database_id
        self.api_token = api_token
        self.limits = limits or Limits()
        self.settings = settings

        # HTTP client with retry support
        self._client: httpx.AsyncClient | None = None
        
        # Rate limiting state
        self._last_request_time: float = 0
        self._request_count: int = 0

    @property
    def database_url(self) -> str:
        """Base URL for database operations."""
        return (
            f"{self.BASE_URL}/accounts/{self.account_id}"
            f"/d1/database/{self.database_id}"
        )

    def _get_headers(self) -> dict[str, str]:
        """Get request headers with authentication."""
        return {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json",
        }

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                headers=self._get_headers(),
                timeout=httpx.Timeout(
                    connect=10.0,
                    read=self.limits.max_query_duration_seconds + 10,
                    write=30.0,
                    pool=10.0,
                ),
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "D1Client":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Make an API request with error handling and retry logic.
        
        Handles:
        - Rate limiting with exponential backoff
        - Transient errors with retry
        - D1-specific error codes
        """
        client = await self._get_client()
        max_retries = 3
        retry_delay = 1.0

        for attempt in range(max_retries):
            try:
                response = await client.request(method, url, **kwargs)

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_after)
                        continue
                    raise D1RateLimitError(retry_after)

                # Parse response
                data = response.json()

                # Check for errors
                if not data.get("success", True):
                    errors = data.get("errors", [])
                    if errors:
                        error = errors[0]
                        message = error.get("message", "Unknown error")
                        code = error.get("code", "")
                        
                        # Handle specific error codes
                        if "statement too long" in message.lower():
                            raise D1StatementTooLongError(message, code)
                        if "timeout" in message.lower():
                            raise D1QueryTimeoutError(message, code)
                        
                        raise D1Error(message, code, response.status_code)

                return data

            except httpx.TransportError as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay * (attempt + 1))
                    continue
                raise D1Error(f"Connection error: {e}")

        raise D1Error("Max retries exceeded")

    async def get_database_info(self) -> dict[str, Any]:
        """Get database metadata."""
        data = await self._request("GET", self.database_url)
        return data.get("result", {})

    async def execute(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> QueryResult:
        """
        Execute a SQL query.
        
        Args:
            sql: SQL statement
            params: Query parameters (for prepared statements)
        
        Returns:
            QueryResult with results and metadata
        """
        url = f"{self.database_url}/query"
        
        body: dict[str, Any] = {"sql": sql}
        if params:
            body["params"] = params

        start_time = time.time()
        
        try:
            data = await self._request("POST", url, json=body)
            duration = (time.time() - start_time) * 1000

            result_data = data.get("result", [{}])[0]
            
            return QueryResult(
                success=True,
                results=result_data.get("results", []),
                meta=result_data.get("meta", {}),
                rows_read=result_data.get("meta", {}).get("rows_read", 0),
                rows_written=result_data.get("meta", {}).get("rows_written", 0),
                duration_ms=duration,
            )
        except D1Error as e:
            return QueryResult(success=False, error=str(e))

    async def execute_batch(
        self,
        statements: list[dict[str, Any]],
    ) -> list[QueryResult]:
        """
        Execute multiple SQL statements in a batch.
        
        Args:
            statements: List of {"sql": str, "params": list} dicts
        
        Returns:
            List of QueryResult objects
        """
        url = f"{self.database_url}/query"
        
        start_time = time.time()
        
        try:
            data = await self._request("POST", url, json=statements)
            duration = (time.time() - start_time) * 1000

            results: list[QueryResult] = []
            for result_data in data.get("result", []):
                results.append(
                    QueryResult(
                        success=result_data.get("success", True),
                        results=result_data.get("results", []),
                        meta=result_data.get("meta", {}),
                        rows_read=result_data.get("meta", {}).get("rows_read", 0),
                        rows_written=result_data.get("meta", {}).get("rows_written", 0),
                        duration_ms=duration / len(statements),
                    )
                )
            return results
        except D1Error as e:
            return [QueryResult(success=False, error=str(e))]

    async def get_table_count(self, table: str) -> int:
        """Get row count for a table."""
        result = await self.execute(f'SELECT COUNT(*) as count FROM "{table}"')
        if result.success and result.results:
            return result.results[0].get("count", 0)
        return 0

    async def get_tables(self) -> list[str]:
        """Get list of table names."""
        result = await self.execute(
            """
            SELECT name FROM sqlite_master 
            WHERE type = 'table' 
            AND name NOT LIKE 'sqlite_%'
            AND name NOT LIKE '_cf_%'
            ORDER BY name
            """
        )
        if result.success:
            return [row.get("name", "") for row in result.results]
        return []

    # =========================================================================
    # Bulk Import Methods
    # =========================================================================

    async def import_sql(
        self,
        sql: str,
        poll_interval: float = 2.0,
        max_wait: float = 300.0,
    ) -> ImportResult:
        """
        Import SQL using the D1 bulk import API.
        
        This uses the R2 upload workflow for large SQL files:
        1. Initialize import and get upload URL
        2. Upload SQL to R2
        3. Start ingestion
        4. Poll until complete
        
        Args:
            sql: SQL statements to import
            poll_interval: Seconds between status polls
            max_wait: Maximum seconds to wait for completion
        
        Returns:
            ImportResult with status
        """
        # Step 1: Initialize import
        init_result = await self._init_import(sql)
        if not init_result.get("upload_url"):
            return ImportResult(
                success=False,
                status=ImportStatus.FAILED,
                error="Failed to get upload URL",
            )

        upload_url = init_result["upload_url"]
        filename = init_result.get("filename", "")

        # Step 2: Upload to R2
        upload_success = await self._upload_to_r2(upload_url, sql)
        if not upload_success:
            return ImportResult(
                success=False,
                status=ImportStatus.FAILED,
                error="Failed to upload to R2",
                filename=filename,
            )

        # Step 3: Start ingestion
        ingest_started = await self._start_ingestion(filename)
        if not ingest_started:
            return ImportResult(
                success=False,
                status=ImportStatus.FAILED,
                error="Failed to start ingestion",
                filename=filename,
            )

        # Step 4: Poll for completion
        return await self._poll_import_status(
            filename=filename,
            poll_interval=poll_interval,
            max_wait=max_wait,
        )

    async def _init_import(self, sql: str) -> dict[str, Any]:
        """Initialize a bulk import operation."""
        url = f"{self.database_url}/import"
        
        # Calculate MD5 hash of SQL content
        md5_hash = hashlib.md5(sql.encode("utf-8")).hexdigest()
        
        body = {
            "action": "init",
            "etag": md5_hash,
        }

        try:
            data = await self._request("POST", url, json=body)
            return data.get("result", {})
        except D1Error:
            return {}

    async def _upload_to_r2(self, upload_url: str, sql: str) -> bool:
        """Upload SQL content to the R2 presigned URL."""
        client = await self._get_client()
        
        try:
            response = await client.put(
                upload_url,
                content=sql.encode("utf-8"),
                headers={"Content-Type": "application/octet-stream"},
            )
            return response.status_code in (200, 201)
        except httpx.TransportError:
            return False

    async def _start_ingestion(self, filename: str) -> bool:
        """Start the import ingestion process."""
        url = f"{self.database_url}/import"
        
        body = {
            "action": "ingest",
            "filename": filename,
        }

        try:
            data = await self._request("POST", url, json=body)
            return data.get("success", False)
        except D1Error:
            return False

    async def _poll_import_status(
        self,
        filename: str,
        poll_interval: float,
        max_wait: float,
    ) -> ImportResult:
        """Poll import status until completion or timeout."""
        url = f"{self.database_url}/import"
        
        body = {
            "action": "poll",
            "filename": filename,
        }

        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                data = await self._request("POST", url, json=body)
                result = data.get("result", {})
                
                status_str = result.get("status", "pending")
                status = ImportStatus(status_str)
                
                if status == ImportStatus.COMPLETE:
                    return ImportResult(
                        success=True,
                        status=status,
                        rows_written=result.get("meta", {}).get("rows_written", 0),
                        filename=filename,
                    )
                
                if status == ImportStatus.FAILED:
                    return ImportResult(
                        success=False,
                        status=status,
                        error=result.get("error", "Import failed"),
                        filename=filename,
                    )
                
                await asyncio.sleep(poll_interval)
                
            except D1Error as e:
                return ImportResult(
                    success=False,
                    status=ImportStatus.FAILED,
                    error=str(e),
                    filename=filename,
                )

        return ImportResult(
            success=False,
            status=ImportStatus.FAILED,
            error="Import timed out",
            filename=filename,
        )

    # =========================================================================
    # High-Level Operations
    # =========================================================================

    async def insert_rows(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        replace: bool = False,
    ) -> QueryResult:
        """
        Insert rows into a table using parameterized queries.
        
        This is the SAFE way to insert data - no SQL escaping issues!
        
        Args:
            table: Target table
            columns: Column names
            rows: List of row tuples
            replace: Use INSERT OR REPLACE
        
        Returns:
            QueryResult with row count
        """
        if not rows:
            return QueryResult(success=True, rows_written=0)

        verb = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
        col_str = ", ".join(f'"{c}"' for c in columns)
        
        # Build multi-row INSERT with proper parameter placeholders
        value_placeholders = ", ".join(f"?{i+1}" for i in range(len(columns)))
        
        total_written = 0
        
        # Batch rows to respect statement size limits
        for row in rows:
            # Note: For bulk inserts, use import_sql instead
            sql = f'{verb} INTO "{table}" ({col_str}) VALUES ({value_placeholders})'
            result = await self.execute(sql, list(row))
            if result.success:
                total_written += result.rows_written
            else:
                return QueryResult(
                    success=False,
                    error=result.error,
                    rows_written=total_written,
                )

        return QueryResult(success=True, rows_written=total_written)

    def generate_insert_sql(
        self,
        table: str,
        columns: list[str],
        rows: list[tuple[Any, ...]],
        replace: bool = False,
    ) -> str:
        """
        Generate INSERT SQL statement for bulk import.
        
        This properly escapes values for SQL.
        
        Args:
            table: Target table
            columns: Column names
            rows: List of row tuples
            replace: Use INSERT OR REPLACE
        
        Returns:
            SQL string ready for import
        """
        if not rows:
            return ""

        verb = "INSERT OR REPLACE" if replace else "INSERT OR IGNORE"
        col_str = ", ".join(f'"{c}"' for c in columns)
        
        value_strs = []
        for row in rows:
            values = []
            for val in row:
                if val is None:
                    values.append("NULL")
                elif isinstance(val, bool):
                    values.append("1" if val else "0")
                elif isinstance(val, (int, float)):
                    values.append(str(val))
                elif isinstance(val, bytes):
                    # Hex encode binary data
                    values.append(f"X'{val.hex()}'")
                else:
                    # Escape single quotes by doubling them
                    escaped = str(val).replace("'", "''")
                    values.append(f"'{escaped}'")
            value_strs.append(f"({', '.join(values)})")

        return f'{verb} INTO "{table}" ({col_str}) VALUES\n{",\n".join(value_strs)};'


# Convenience function for creating client from settings
def create_d1_client(settings: Settings) -> D1Client:
    """Create a D1Client from settings."""
    return D1Client(
        account_id=settings.cloudflare_account_id,
        database_id=settings.database_id,
        api_token=settings.cloudflare_api_token.get_secret_value(),
        limits=settings.limits,
        settings=settings,
    )
