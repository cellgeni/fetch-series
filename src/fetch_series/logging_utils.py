"""Logging helpers that keep credentials out of log files.

The NCBI API key is passed as a query parameter, so any log line that echoes a
request URL leaks it. That is exactly how the key ended up committed to this
repository: ``httpx``/``tenacity`` retry warnings render the full URL, including
``api_key=``, and the survey logs were committed alongside their results.

Everything here redacts the *formatted* output rather than the format string, so
it also covers URLs that only appear inside exception tracebacks.
"""

from __future__ import annotations

import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

REDACTED = "***REDACTED***"

# Credential-bearing query parameters, matched up to the next delimiter so the
# rest of the URL survives and the line stays useful for debugging.
_SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)\b(api_key|apikey|access_token|token|password)=([^&\s\"'<>\]),]+)"),
)


def redact(text: str) -> str:
    """Replace the value of any known credential parameter in ``text``."""
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub(lambda m: f"{m.group(1)}={REDACTED}", text)
    return text


class RedactingFormatter(logging.Formatter):
    """A formatter that strips credentials from the final rendered record."""

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))


DEFAULT_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"


def configure_logging(
    level: int = logging.INFO,
    stream: IO[str] | None = None,
    logfile: Path | None = None,
    fmt: str = DEFAULT_FORMAT,
) -> logging.Logger:
    """Install redacting handlers on the root logger.

    Args:
        level: threshold for the root logger.
        stream: stream to log to; defaults to stderr. Pass ``None`` with a
            ``logfile`` to keep logging to both.
        logfile: optional file to append to. Opened in append mode on purpose —
            the survey scripts used ``mode="w"``, so every re-run destroyed the
            previous run's evidence.
        fmt: logging format string.

    Returns:
        The configured root logger.
    """
    root = logging.getLogger()
    root.setLevel(level)
    for existing in list(root.handlers):
        root.removeHandler(existing)

    formatter = RedactingFormatter(fmt)

    stream_handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    stream_handler.setFormatter(formatter)
    root.addHandler(stream_handler)

    if logfile is not None:
        logfile.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(logfile, mode="a", encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    return root


def run_logfile(route: str, root: Path = Path("data/query/logs")) -> Path:
    """Return a timestamped, per-run log path: ``data/query/logs/<route>/<ts>.log``.

    One file per run so a re-run never overwrites the log that documents the
    results sitting next to it.
    """
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return root / route / f"{stamp}.log"
