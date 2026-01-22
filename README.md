# SQLite-D1 Sync

<div align="center">

![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)
![CI Status](https://github.com/parishkhan/sqlite-d1-sync/actions/workflows/test.yml/badge.svg)

**Professional bidirectional sync between local SQLite databases and Cloudflare D1.**

_Fast, reliable, and enterprise-ready database sync with zero data corruption._

</div>

---

## ⚡️ Quick Start for Beginners

### 1. Requirements

- **Python 3.12+** installed
- **Cloudflare Account** (Account ID & API Token)
- **Local SQLite Database** (e.g. `tutorials.db`)

### 2. Installation

```bash
# Clone the repository
git clone https://github.com/your-repo/d1-sync.git
cd d1-sync

# Create a virtual environment (Required on macOS!)
# This keeps your project isolated from other python tools
python3 -m venv .venv

# Activate the virtual environment
# ⚠️ You must run this command every time you open a new terminal
source .venv/bin/activate

# Install the tool
pip install -e .
```

### 3. Setup Configuration

You have **two options** to configure your credentials. Choose ONLY ONE method.

#### Option A: Using Environment Variables (Recommended for CI/CD)

Create a `.env` file or export these variables:

```bash
# notice the D1_SYNC_ prefix!
export D1_SYNC_CLOUDFLARE_ACCOUNT_ID="your-account-id"
export D1_SYNC_CLOUDFLARE_API_TOKEN="your-api-token"
export D1_SYNC_DATABASE_NAME="tutorials-db"
```

#### Option B: Using a Config File (Easier for Local Use)

Create a `config.toml` file. **Note:** Variable names here DO NOT have the `D1_SYNC_` prefix.

```toml
# config.toml
cloudflare_account_id = "your-account-id"
cloudflare_api_token = "your-api-token"  # No prefix here!
database_name = "tutorials-db"
tier = "free"
```

---

## 🚀 How to Sync

### Push: Local Computer ➡ Cloudflare D1

Uploads your local database changes to the cloud.

```bash
# 1. First, try a "Dry Run"
# This shows you what WILL happen without actually changing anything
d1-sync push --source my-database.db --dry-run --config config.toml

# 2. If it looks good, run the real sync
d1-sync push --source my-database.db --config config.toml
```

### Pull: Cloudflare D1 ➡ Local Computer

Downloads the cloud database to your computer.

```bash
d1-sync pull --destination backup.db --config config.toml
```

### Sync-Slugs: Fast Slug Fixes Sync 🆕

**What is this?**

The `sync-slugs` command is a specialized tool for syncing URL slug fixes from your local database to Cloudflare D1. It's designed to be **much faster** than a full `push` operation when you've only fixed slugs in your database.

**When should I use this?**

Use `sync-slugs` when:

- ✅ You've run a **slug fixer** that corrected bad URL slugs (e.g., converted Chinese characters to English)
- ✅ Your local database has a `slug_old` column containing the original bad slugs
- ✅ You only need to sync the **changed rows** (typically ~5,000 rows) instead of all rows (300,000+)
- ✅ You want to add the `slug_old` column to your D1 database

**How does it work?**

1. **Checks D1** - Verifies if the `slug_old` column exists in D1
2. **Adds column if needed** - Creates `slug_old` column if it doesn't exist
3. **Finds changed rows** - Only queries rows where `slug_old IS NOT NULL`
4. **Updates in batches** - Sends efficient UPDATE statements (50 rows at a time)
5. **Tracks progress** - Shows real-time progress with beautiful terminal UI

#### Basic Usage Examples

```bash
# 1. Dry Run - See what will change (RECOMMENDED FIRST STEP!)
d1-sync sync-slugs --source tutorials.db --dry-run

# 2. Real sync - After dry run looks good
d1-sync sync-slugs --source tutorials.db

# 3. With a config file
d1-sync sync-slugs --source tutorials.db --config config.toml

# 4. Quiet mode - Minimal output (useful for scripts)
d1-sync sync-slugs --source tutorials.db --quiet
```

#### All Available Options

| Option          | Short | Description                   | Required                     | Example                 |
| --------------- | ----- | ----------------------------- | ---------------------------- | ----------------------- |
| `--source`      | `-s`  | Path to local SQLite database | ✅ Yes                       | `--source tutorials.db` |
| `--table`       | `-t`  | Table name to sync            | ❌ No (default: `tutorials`) | `--table products`      |
| `--database`    | `-d`  | D1 database name              | ❌ No\*                      | `--database my-db`      |
| `--database-id` |       | D1 database UUID              | ❌ No\*                      | `--database-id abc123`  |
| `--account-id`  |       | Cloudflare account ID         | ❌ No\*                      | `--account-id xyz789`   |
| `--api-token`   |       | Cloudflare API token          | ❌ No\*                      | `--api-token token123`  |
| `--config`      | `-c`  | Path to config file           | ❌ No                        | `--config config.toml`  |
| `--dry-run`     | `-n`  | Preview without changes       | ❌ No                        | `--dry-run`             |
| `--quiet`       | `-q`  | Minimal output                | ❌ No                        | `--quiet`               |

\*These can be set in `config.toml` or environment variables instead

#### Command Variations by Use Case

**Use Case 1: First Time Sync (with a new D1 database)**

```bash
# Step 1: Dry run to verify
d1-sync sync-slugs --source ../tutorials.db --dry-run

# Step 2: Run the actual sync
d1-sync sync-slugs --source ../tutorials.db
```

**Expected Output:**

```
Slug Sync Summary
┌─────────────────┬──────────┐
│ Metric          │ Value    │
├─────────────────┼──────────┤
│ Rows to sync    │ 4,914    │
│ Rows updated    │ 4,914    │
│ Column added    │ Yes      │
│ Duration        │ 45.2s    │
└─────────────────┴──────────┘

✓ Slug sync completed successfully!
```

**Use Case 2: Using Environment Variables (CI/CD friendly)**

```bash
# Set credentials once
export D1_SYNC_CLOUDFLARE_API_TOKEN="your-token"
export D1_SYNC_CLOUDFLARE_ACCOUNT_ID="your-account-id"
export D1_SYNC_DATABASE_NAME="tutorials-db"

# Run sync (no need to specify credentials each time)
d1-sync sync-slugs --source tutorials.db
```

**Use Case 3: Using a Config File (easiest for local development)**

```bash
# Create config.toml first
cat > config.toml << EOF
cloudflare_account_id = "your-account-id"
cloudflare_api_token = "your-api-token"
database_name = "tutorials-db"
EOF

# Run sync with config
d1-sync sync-slugs --source tutorials.db --config config.toml
```

**Use Case 4: Syncing a Different Table**

```bash
# Sync a table named 'products' instead of 'tutorials'
d1-sync sync-slugs --source shop.db --table products
```

**Use Case 5: Silent/Script Mode**

```bash
# For automated scripts - minimal output, just errors if any
d1-sync sync-slugs --source tutorials.db --quiet

# Check exit code: 0 = success, 1 = failure
echo $?
```

**Use Case 6: Overriding Config File Settings**

```bash
# Use config file but override the database name
d1-sync sync-slugs \
  --source tutorials.db \
  --config config.toml \
  --database production-db
```

#### Understanding the Output

**Successful Sync:**

```
┌─────────────────┬──────────┐
│ Rows to sync    │ 4,914    │  ← Total rows where slug_old IS NOT NULL
│ Rows updated    │ 4,914    │  ← Successfully updated in D1
│ Rows failed     │ 0        │  ← No failures (good!)
│ Column added    │ Yes      │  ← slug_old column was created
│ Duration        │ 45.2s    │  ← Time taken
└─────────────────┴──────────┘
```

**Partial Success (with errors):**

```
⚠ 2 errors occurred:
  • Batch 45/100: Duplicate entry for key 'PRIMARY'
  • Batch 67/100: Constraint violation

Slug Sync Summary
┌─────────────────┬──────────┐
│ Rows to sync    │ 5,000    │
│ Rows updated    │ 4,900    │  ← Some succeeded
│ Rows failed     │ 100      │  ← Some failed (check errors above)
└─────────────────┴──────────┘
```

#### Troubleshooting

**Problem: "Source database not found"**

```bash
# Solution: Use absolute path or ensure relative path is correct
d1-sync sync-slugs --source /full/path/to/tutorials.db
```

**Problem: "No rows to sync"**

```bash
# This means your local database doesn't have the slug_old column yet
# Or all slug_old values are NULL
# Verify with SQLite:
sqlite3 tutorials.db "SELECT COUNT(*) FROM tutorials WHERE slug_old IS NOT NULL;"
```

**Problem: "column slug_old does not exist"**

```bash
# The sync-slugs command will auto-add the column to D1
# But if your local DB doesn't have it yet, you'll need to run the slug fixer first
```

#### Comparison: sync-slugs vs push

| Feature       | `sync-slugs`            | `push`                     |
| ------------- | ----------------------- | -------------------------- |
| **Speed**     | ⚡ Very Fast (~5K rows) | 🐢 Slower (all 300K+ rows) |
| **Operation** | UPDATE existing rows    | INSERT new/UPDATE existing |
| **Column**    | Adds `slug_old` column  | Creates full table schema  |
| **Use Case**  | Slug fixes only         | Full database sync         |
| **Duration**  | ~1-2 minutes            | ~10-30 minutes             |

### Check Progress

If a sync is running in the background, check its status:

```bash
d1-sync status
```

---

## ⚙️ Configuration Reference

### Confused about variable names?

Here is the simple rule:

- **In `.env` files**: Use `D1_SYNC_` prefix (e.g. `D1_SYNC_CLOUDFLARE_API_TOKEN`)
- **In `config.toml`**: No prefix (e.g. `cloudflare_api_token`)

### Full `config.toml` Example

```toml
# ==========================================
# CREDENTIALS
# ==========================================
cloudflare_account_id = "fe51..."
cloudflare_api_token = "wAL..."      # Ensure this token has "D1: Edit" permissions
database_name = "tutorials-db"
database_id = "1863..."              # Optional, but recommended

# ==========================================
# LIMITS & PERFORMANCE
# ==========================================
# "free" or "paid". Paid allows higher limits.
tier = "free"

[limits]
# Max rows to upload per batch.
# Default is 100. Increase to 500-1000 for faster sync if data is simple.
max_rows_per_batch = 500

[sync]
# If true, it uses INSERT OR REPLACE (overwrites existing rows)
# If false, it uses INSERT OR IGNORE (skips existing rows) - SAFER!
overwrite = false

# If the sync fails, resume from where it left off next time
resume = true

# Verify data integrity after sync completes
verify_after_sync = true

# Tables to exclude from sync
exclude_tables = ["sqlite_sequence", "_cf_KV"]

[logging]
# Options: DEBUG, INFO, WARNING, ERROR
level = "INFO"
# Save failed rows to this file for review
failed_rows_file = "failed_rows.json"
```

## ❓ FAQ:

**Q: My upload is slow!**
A: In `config.toml`, under `[limits]`, increase `max_rows_per_batch` to `500` or `1000`.

**Q: I got a "Virtual Environment" error?**
A: You likely forgot to run `source .venv/bin/activate`.

**Q: "Command not found: d1-sync"?**
A: Make sure you ran `pip install -e .` and activated your venv.

## 📄 License

MIT License
