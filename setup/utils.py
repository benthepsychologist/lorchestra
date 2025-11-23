"""Shared utility functions for setup scripts."""

import sys

# ANSI color codes
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color


def error(msg):
    """Print error message and exit."""
    print(f"{RED}ERROR: {msg}{NC}", file=sys.stderr)
    sys.exit(1)


def success(msg):
    """Print success message."""
    print(f"{GREEN}✓ {msg}{NC}")


def info(msg):
    """Print info message."""
    print(f"{YELLOW}→ {msg}{NC}")


def warning(msg):
    """Print warning message."""
    print(f"{BLUE}⚠ {msg}{NC}")
