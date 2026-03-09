"""Path classification helpers shared by watcher components."""

from __future__ import annotations

from pathlib import Path

from scripts.workspace_state import (
    _get_global_state_dir,
    INTERNAL_STATE_TOP_LEVEL_DIRS,
)


def is_internal_metadata_path(path: Path) -> bool:
    """Return True when path points into watcher/internal metadata trees."""
    try:
        if any(part in INTERNAL_STATE_TOP_LEVEL_DIRS for part in path.parts):
            return True
        global_state_dir = _get_global_state_dir()
        if global_state_dir is not None and path.is_relative_to(global_state_dir):
            return True
    except (OSError, ValueError):
        return False
    return False


def is_internal_top_level_path(path: Path, root: Path) -> bool:
    """Return True when path's top-level segment under root is internal metadata."""
    try:
        rel = path.resolve().relative_to(root.resolve())
    except Exception:
        return False
    if not rel.parts:
        return False
    return rel.parts[0] in INTERNAL_STATE_TOP_LEVEL_DIRS
