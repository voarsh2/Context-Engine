"""Shared configuration helpers for pseudo/tags generation.

This keeps env semantics consistent across:
- watcher (watch_index / watch_index_core)
- indexing CLI (scripts/ingest/cli.py)

Policy:
- PSEUDO_BACKFILL_ENABLED controls whether the async backfill worker is enabled.
- PSEUDO_DEFER_TO_WORKER controls *foreground vs background* behavior only.
  Deferral is only effective when the worker is enabled; otherwise we keep inline
  pseudo generation ON to avoid silently dropping pseudo/tags.
"""

from __future__ import annotations

import os
from typing import Optional


def _parse_env_bool(value: Optional[str], *, default: bool = False) -> bool:
    if value is None:
        return default
    v = str(value).strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "on"}


def env_bool(key: str, *, default: bool = False) -> bool:
    """Read a boolean env var using consistent truthy parsing."""
    return _parse_env_bool(os.environ.get(key), default=default)


def effective_defer_to_worker(*, defer_to_worker: bool, backfill_enabled: bool) -> bool:
    """Whether we should disable inline pseudo/tags generation."""
    return bool(defer_to_worker and backfill_enabled)


def effective_pseudo_mode(*, defer_to_worker: bool, backfill_enabled: bool) -> str:
    """Return pseudo_mode ('off'|'full') for indexing pipeline."""
    return "off" if effective_defer_to_worker(
        defer_to_worker=defer_to_worker,
        backfill_enabled=backfill_enabled,
    ) else "full"


__all__ = [
    "env_bool",
    "effective_defer_to_worker",
    "effective_pseudo_mode",
]

