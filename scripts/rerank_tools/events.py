"""Shared filesystem helpers for relevance-feedback event files."""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Dict

try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None


def _get_events_dir() -> Path:
    return Path(os.environ.get("RERANK_EVENTS_DIR", "/tmp/rerank_events"))


def _ensure_events_dir() -> Path:
    events_dir = _get_events_dir()
    events_dir.mkdir(parents=True, exist_ok=True)
    return events_dir


_WRITE_LOCKS: Dict[str, threading.Lock] = {}
_LOCKS_LOCK = threading.Lock()


def _get_write_lock(file_key: str) -> threading.Lock:
    """Return the process-local lock for one event file."""
    with _LOCKS_LOCK:
        if file_key not in _WRITE_LOCKS:
            _WRITE_LOCKS[file_key] = threading.Lock()
        return _WRITE_LOCKS[file_key]


def append_event_line(events_file: Path, line: str) -> None:
    """Append one NDJSON event safely across threads and worker processes."""
    events_file = Path(events_file)
    events_file.parent.mkdir(parents=True, exist_ok=True)
    lock = _get_write_lock(str(events_file))
    with lock:
        with events_file.open("a", encoding="utf-8") as event_file:
            if fcntl is not None:
                fcntl.flock(event_file.fileno(), fcntl.LOCK_EX)
            try:
                event_file.write(line.rstrip("\n") + "\n")
                event_file.flush()
            finally:
                if fcntl is not None:
                    fcntl.flock(event_file.fileno(), fcntl.LOCK_UN)
