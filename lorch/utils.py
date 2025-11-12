"""
Utility functions for lorch pipeline.

Includes logging, retries, validation, and file operations.
"""

import json
import logging
import os
import stat
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from rich.console import Console
from rich.logging import RichHandler


# Global console for pretty output
console = Console()


def setup_logging(log_file: Path, log_level: str = "INFO", log_format: str = "structured", console_output: bool = True) -> logging.Logger:
    """
    Set up logging for pipeline execution.

    Args:
        log_file: Path to log file
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        log_format: "structured" (JSON) or "pretty" (human-readable)
        console_output: Also log to console

    Returns:
        Configured logger
    """
    # Create log directory if needed
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Set permissions on log directory
    log_file.parent.chmod(0o700)

    # Create logger
    logger = logging.getLogger("lorch")
    logger.setLevel(getattr(logging, log_level.upper()))
    logger.handlers = []  # Clear existing handlers

    # File handler
    if log_format == "structured":
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(StructuredFormatter())
    else:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        )

    logger.addHandler(file_handler)

    # Console handler
    if console_output:
        if log_format == "pretty":
            console_handler = RichHandler(rich_tracebacks=True, show_time=False)
        else:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(
                logging.Formatter("%(levelname)s: %(message)s")
            )

        logger.addHandler(console_handler)

    return logger


class StructuredFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
        }

        # Add extra fields if present
        if hasattr(record, "stage"):
            log_data["stage"] = record.stage
        if hasattr(record, "event"):
            log_data["event"] = record.event
        if hasattr(record, "metadata"):
            log_data["metadata"] = record.metadata

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


def retry_with_backoff(
    func: Callable,
    max_attempts: int = 3,
    backoff_seconds: int = 60,
    backoff_multiplier: float = 2.0,
    logger: Optional[logging.Logger] = None,
) -> Any:
    """
    Retry a function with exponential backoff.

    Args:
        func: Function to retry
        max_attempts: Maximum number of attempts
        backoff_seconds: Initial backoff time in seconds
        backoff_multiplier: Multiplier for each retry
        logger: Logger for retry messages

    Returns:
        Result of successful function call

    Raises:
        Exception: If all retries exhausted
    """
    attempt = 1
    wait_time = backoff_seconds

    while attempt <= max_attempts:
        try:
            if logger:
                logger.info(f"Attempt {attempt}/{max_attempts}")
            return func()

        except Exception as e:
            if attempt == max_attempts:
                if logger:
                    logger.error(f"All {max_attempts} attempts failed: {e}")
                raise

            if logger:
                logger.warning(
                    f"Attempt {attempt} failed: {e}. Retrying in {wait_time}s..."
                )

            time.sleep(wait_time)
            wait_time *= backoff_multiplier
            attempt += 1


def validate_phi_permissions(path: Path, required_mode: str = "700") -> None:
    """
    Validate that PHI directory has correct permissions.

    Args:
        path: Path to PHI directory
        required_mode: Required permission mode (e.g., "700")

    Raises:
        PermissionError: If permissions are incorrect
    """
    if not path.exists():
        raise FileNotFoundError(f"PHI directory does not exist: {path}")

    stat_info = path.stat()
    current_mode = stat.S_IMODE(stat_info.st_mode)
    required_mode_int = int(required_mode, 8)

    if current_mode != required_mode_int:
        raise PermissionError(
            f"PHI directory {path} has insecure permissions {oct(current_mode)}. "
            f"Expected {oct(required_mode_int)}. Run: chmod {required_mode} {path}"
        )


def validate_file_is_jsonl(file_path: Path) -> bool:
    """
    Validate that a file is valid JSONL format.

    Args:
        file_path: Path to JSONL file

    Returns:
        True if valid, False otherwise
    """
    try:
        with open(file_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:  # Skip empty lines
                    continue
                json.loads(line)  # Will raise if invalid JSON
        return True
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False


def count_jsonl_records(file_path: Path) -> int:
    """
    Count records in a JSONL file.

    Args:
        file_path: Path to JSONL file

    Returns:
        Number of records (non-empty lines)
    """
    count = 0
    with open(file_path, "r") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def sanitize_error_message(error: Exception, max_length: int = 500) -> str:
    """
    Sanitize error message to avoid logging PHI.

    Args:
        error: Exception to sanitize
        max_length: Maximum message length

    Returns:
        Sanitized error message
    """
    message = str(error)

    # Truncate if too long
    if len(message) > max_length:
        message = message[:max_length] + "..."

    # Remove potential email addresses
    import re
    message = re.sub(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", "[EMAIL]", message)

    # Remove potential phone numbers
    message = re.sub(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", "[PHONE]", message)

    return message


def ensure_directory_permissions(directory: Path, mode: int = 0o700) -> None:
    """
    Ensure directory exists with correct permissions.

    Args:
        directory: Directory path
        mode: Permission mode (default: 0o700)
    """
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(mode)


def get_file_checksum(file_path: Path) -> str:
    """
    Calculate SHA256 checksum of file.

    Args:
        file_path: Path to file

    Returns:
        Hex digest of SHA256 checksum
    """
    import hashlib

    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "1m 23s", "45s")
    """
    if seconds < 60:
        return f"{int(seconds)}s"

    minutes = int(seconds // 60)
    remaining_seconds = int(seconds % 60)

    if minutes < 60:
        return f"{minutes}m {remaining_seconds}s"

    hours = minutes // 60
    remaining_minutes = minutes % 60
    return f"{hours}h {remaining_minutes}m {remaining_seconds}s"


def print_banner(title: str) -> None:
    """
    Print a banner to console.

    Args:
        title: Banner title
    """
    console.rule(f"[bold blue]{title}[/bold blue]")


def print_success(message: str) -> None:
    """Print success message to console."""
    console.print(f"[bold green]✓[/bold green] {message}")


def print_error(message: str) -> None:
    """Print error message to console."""
    console.print(f"[bold red]✗[/bold red] {message}")


def print_warning(message: str) -> None:
    """Print warning message to console."""
    console.print(f"[bold yellow]⚠[/bold yellow] {message}")


def print_info(message: str) -> None:
    """Print info message to console."""
    console.print(f"[bold cyan]ℹ[/bold cyan] {message}")
