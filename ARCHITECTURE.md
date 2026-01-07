# D1 Sync - Technical Architecture

This document describes the technical architecture of D1 Sync for maintainers and contributors.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              D1 Sync CLI                                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   push      │  │   pull      │  │  status     │  │  verify     │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
└─────────┼────────────────┼────────────────┼────────────────┼────────────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Sync Engine (engine.py)                            │
│                                                                              │
│  • Orchestrates sync operations                                              │
│  • Coordinates all components                                                │
│  • Handles progress callbacks                                                │
└─────────┬────────────────┬────────────────┬────────────────┬────────────────┘
          │                │                │                │
    ┌─────▼─────┐    ┌─────▼─────┐    ┌─────▼─────┐    ┌─────▼─────┐
    │  SQLite   │    │ D1 Client │    │ Chunker   │    │ State Mgr │
    │ Connector │    │           │    │           │    │           │
    └───────────┘    └───────────┘    └───────────┘    └───────────┘
```

## Directory Structure

```
d1-sync/
├── src/d1_sync/
│   ├── __init__.py          # Package init, version
│   ├── __main__.py          # Entry point for python -m
│   ├── cli.py               # Typer CLI application
│   ├── config.py            # Pydantic settings & limits
│   │
│   ├── connectors/          # Database connectors
│   │   ├── __init__.py
│   │   ├── sqlite.py        # Local SQLite operations
│   │   └── d1_client.py     # Cloudflare D1 REST API
│   │
│   ├── core/                # Core sync logic
│   │   ├── __init__.py
│   │   ├── engine.py        # Main sync orchestration
│   │   ├── chunker.py       # Size-aware SQL building
│   │   ├── state.py         # Resume/recovery state
│   │   └── integrity.py     # Checksum verification
│   │
│   └── utils/               # Utilities
│       ├── __init__.py
│       ├── logger.py        # Structured logging
│       └── display.py       # Rich terminal UI
│
├── tests/                   # Test suite
├── Dockerfile               # Multi-stage build
├── docker-compose.yml       # Dev/prod compose
├── pyproject.toml          # Project config
└── README.md               # User documentation
```

## Component Details

### 1. Configuration (config.py)

**Purpose**: Type-safe configuration with Pydantic.

**Key Classes**:

- `Settings`: Main settings class with env var loading
- `Limits`: API limits (100KB SQL, 30s timeout, etc.)
- `TierLimits`: Predefined limits for free/paid tiers
- `SyncOptions`: Sync behavior options

**Settings Priority** (highest first):

1. CLI arguments
2. Environment variables (`D1_SYNC_*` prefix)
3. Config file (TOML/JSON)
4. Defaults

### 2. SQLite Connector (connectors/sqlite.py)

**Purpose**: Efficient SQLite database operations.

**Key Features**:

- Schema introspection (`get_tables()`, `get_column_info()`)
- Streaming iteration (`iter_rows()`) - memory efficient
- Batch operations (`insert_rows()`, `execute_many()`)
- Checksum calculation per batch

**Design**: Uses context manager pattern, WAL mode for performance.

### 3. D1 Client (connectors/d1_client.py)

**Purpose**: Cloudflare D1 REST API interface.

**Key Features**:

- Query execution with retry logic
- Bulk import via R2 upload workflow
- Rate limit handling
- D1-specific error codes

**Bulk Import Flow**:

```
1. Init Upload   →  Get presigned R2 URL + filename
2. Upload to R2  →  PUT SQL content to presigned URL
3. Start Ingest  →  Trigger D1 to process file
4. Poll Status   →  Wait for completion
```

### 4. SQL Chunker (core/chunker.py)

**Purpose**: Build SQL statements respecting size limits.

**Key Features**:

- Exact byte size calculation (not estimation)
- Safety margin (85% of limit by default)
- Proper SQL escaping (no injection/corruption)
- Binary data handling (hex encoding)

**Algorithm**:

```python
for each row:
    calculate exact byte size
    if current_chunk + row > limit:
        yield current_chunk
        start new chunk
    add row to chunk
yield final chunk
```

### 5. State Manager (core/state.py)

**Purpose**: Persistent state for resume/recovery.

**Key Features**:

- Per-table progress tracking
- Failed row recording with full details
- Automatic checkpoint saving
- Resume from last successful batch

**State File** (`.d1-sync-state.json`):

```json
{
  "operation": "push",
  "status": "in_progress",
  "tables": {
    "users": {
      "status": "completed",
      "processed_rows": 1000,
      "last_offset": 1000
    }
  },
  "failed_rows": []
}
```

### 6. Sync Engine (core/engine.py)

**Purpose**: Main orchestration for sync operations.

**Push Flow**:

```
1. Load/create state
2. Connect to SQLite source
3. Get tables to sync
4. For each table:
   a. Sync schema (if enabled)
   b. Get resume offset
   c. Stream rows in batches
   d. Chunk batches for D1 limits
   e. Upload via D1 client
   f. Update progress
   g. Save state
5. Verify (if enabled)
6. Mark complete
```

**Pull Flow**: Similar but reversed direction.

### 7. CLI (cli.py)

**Purpose**: User interface via Typer.

**Commands**:

- `push`: Local → D1
- `pull`: D1 → Local
- `status`: Show state
- `verify`: Check integrity
- `config`: Manage config

**Features**:

- Rich progress display
- Tab completion
- Help text with examples

## Data Integrity

### 1. SQL Escaping (chunker.py)

All values go through `escape_value()`:

- `None` → `NULL`
- `bool` → `1` or `0`
- `int/float` → string representation
- `bytes` → `X'hex'` notation
- `str` → Single quotes with `''` escaping

### 2. Checksums (integrity.py)

- Row-level MD5/SHA256
- Batch-level combined hash
- Table-level verification
- Mismatch detection

### 3. No Shell Escaping

Unlike the old JS scripts, we:

- Use parameterized queries where possible
- Generate SQL directly in Python
- No shell command interpolation

## Performance Considerations

### Batching Strategy

```
Rows/Batch = min(
    config.max_rows_per_batch,
    floor(max_sql_size * safety_margin / avg_row_size)
)
```

### Memory Efficiency

- SQLite rows streamed via generator
- Chunks built incrementally
- State saved periodically

### Concurrency

- Single-threaded by default (D1 is single-threaded)
- Paid tier can use concurrent batches (up to 6)

## Extension Points

### Adding a New Database Connector

1. Create class in `connectors/`
2. Implement interface:
   - `get_tables()`
   - `iter_rows()`
   - `insert_rows()`
3. Register in `connectors/__init__.py`

### Adding New Commands

1. Add function with `@app.command()` in `cli.py`
2. Use existing settings and engine patterns

## Testing

```bash
# All tests
pytest tests/ -v

# With coverage
pytest --cov=src/d1_sync

# Type check
mypy src/d1_sync

# Lint
ruff check src/
```

## Error Handling

### Retry Logic

- Transient errors: 3 retries with exponential backoff
- Rate limits: Wait for `Retry-After` header
- Statement too long: Split and retry

### Recovery

- State saved after each batch
- Failed rows logged to JSON
- Resume picks up from last checkpoint

## Security

### Credentials

- API token stored as `SecretStr` (never logged)
- Loaded from env vars or config file
- Not included in state files

### Docker

- Non-root user (`d1sync:1000`)
- Read-only source mounts
- No unnecessary capabilities
