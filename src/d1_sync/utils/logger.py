"""
Logging setup for D1 Sync.

Provides structured logging with:
- Rich colored console output
- File logging with rotation
- JSON format option
- Failed row tracking
"""

from __future__ import annotations

import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.logging import RichHandler


# Global console for rich output
console = Console()

# Package logger
logger = logging.getLogger("d1_sync")


def setup_logging(
    level: str = "INFO",
    log_file: Path | str | None = None,
    format_style: str = "rich",
    max_file_size_mb: int = 10,
    backup_count: int = 3,
) -> None:
    """
    Configure logging for the application.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file
        format_style: "rich", "json", or "simple"
        max_file_size_mb: Max log file size before rotation
        backup_count: Number of backup files to keep
    """
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    logger.setLevel(log_level)

    if format_style == "rich":
        # Rich console handler
        handler = RichHandler(
            console=console,
            rich_tracebacks=True,
            show_time=True,
            show_path=False,
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
    elif format_style == "json":
        # JSON format for structured logging
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
    else:
        # Simple format
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    handler.setLevel(log_level)
    logger.addHandler(handler)

    # Add file handler if specified
    if log_file:
        file_path = Path(log_file)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=max_file_size_mb * 1024 * 1024,
            backupCount=backup_count,
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(
            logging.Formatter(
                "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(file_handler)


class JsonFormatter(logging.Formatter):
    """JSON log formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Include extra fields
        if hasattr(record, "extra"):
            log_data.update(record.extra)  # type: ignore
        
        return json.dumps(log_data)


def get_logger(name: str = "d1_sync") -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)


# Convenience functions for quick logging
def debug(msg: str, **kwargs: Any) -> None:
    """Log debug message."""
    logger.debug(msg, extra=kwargs)


def info(msg: str, **kwargs: Any) -> None:
    """Log info message."""
    logger.info(msg, extra=kwargs)


def warning(msg: str, **kwargs: Any) -> None:
    """Log warning message."""
    logger.warning(msg, extra=kwargs)


def error(msg: str, **kwargs: Any) -> None:
    """Log error message."""
    logger.error(msg, extra=kwargs)


def critical(msg: str, **kwargs: Any) -> None:
    """Log critical message."""
    logger.critical(msg, extra=kwargs)
