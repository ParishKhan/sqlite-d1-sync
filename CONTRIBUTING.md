# Contributing to SQLite-D1 Sync

First off, thanks for taking the time to contribute! 🎉

## How to Contribute

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally:
    ```bash
    git clone https://github.com/your-username/sqlite-d1-sync.git
    cd sqlite-d1-sync
    ```
3.  **Create a virtual environment**:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate
    ```
4.  **Install development dependencies**:
    ```bash
    pip install -e ".[dev]"
    ```
5.  **Create a branch** for your feature or fix:
    ```bash
    git checkout -b feature/amazing-feature
    ```
6.  **Make your changes**.
7.  **Run tests** to ensure you haven't broken anything:
    ```bash
    pytest
    ```
8.  **Commit your changes** (we recommend [Conventional Commits](https://www.conventionalcommits.org/)):
    ```bash
    git commit -m "feat: add amazing feature"
    ```
9.  **Push to your fork**:
    ```bash
    git push origin feature/amazing-feature
    ```
10. **Open a Pull Request**.

## Coding Standards

- **Python Version**: We support Python 3.12+.
- **Type Checking**: We use `mypy` for static type checking. Run `mypy .` to check.
- **Linting**: We use `ruff` for linting. Run `ruff check .` to check.
- **Formatting**: We use `ruff` for formatting. Run `ruff format .` to format.

## Project Structure

- `src/d1_sync/`: Source code
  - `cli.py`: Command line interface (Typer)
  - `config.py`: Configuration models (Pydantic)
  - `core/`: Core logic (Chunker, Engine, Integrity)
  - `connectors/`: Database interfaces (SQLite, D1)
- `tests/`: Test suite

## Reporting Bugs

Please check existing issues first. If you find a new bug, please open a new issue with complete details, including:

- Steps to reproduce
- Expected behavior
- Actual behavior
- `d1-sync version` output
