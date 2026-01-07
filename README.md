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

## ❓ FAQ

**Q: My upload is slow!**
A: In `config.toml`, under `[limits]`, increase `max_rows_per_batch` to `500` or `1000`.

**Q: I got a "Virtual Environment" error?**
A: You likely forgot to run `source .venv/bin/activate`.

**Q: "Command not found: d1-sync"?**
A: Make sure you ran `pip install -e .` and activated your venv.

## 📄 License

MIT License
