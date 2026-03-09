"""Background pseudo backfill worker for the watcher."""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional

import scripts.ingest_code as idx
from . import config as watch_config
from .utils import get_boolean_env
from scripts.workspace_state import (
    _cross_process_lock,
    _get_global_state_dir,
    _get_repo_state_dir,
    get_collection_mappings,
    is_multi_repo_mode,
)

logger = logging.getLogger(__name__)


def _start_pseudo_backfill_worker(
    client,
    default_collection: str,
    model_dim: int,
    vector_name: str,
) -> Optional[threading.Event]:
    """Start a daemon thread that periodically backfills pseudo/tags.
    
    Returns a threading.Event that can be set to signal shutdown,
    or None if the worker was not started (disabled via env).
    """
    
    if not get_boolean_env("PSEUDO_DEFER_TO_WORKER"):
        return None

    try:
        interval = float(os.environ.get("PSEUDO_BACKFILL_TICK_SECS", "60") or 60.0)
    except Exception:
        interval = 60.0
    if interval <= 0:
        return None
    try:
        max_points = int(os.environ.get("PSEUDO_BACKFILL_MAX_POINTS", "256") or 256)
    except Exception:
        max_points = 256
    if max_points <= 0:
        max_points = 1
    try:
        graph_max_files = int(
            os.environ.get("GRAPH_EDGES_BACKFILL_MAX_FILES", "128") or 128
        )
    except Exception:
        graph_max_files = 128
    if graph_max_files <= 0:
        graph_max_files = 1

    shutdown_event = threading.Event()

    def _worker() -> None:
        while not shutdown_event.is_set():
            try:
                graph_backfill_enabled = get_boolean_env("GRAPH_EDGES_BACKFILL")
                try:
                    mappings = get_collection_mappings(search_root=str(watch_config.ROOT))
                except Exception:
                    mappings = []
                if not mappings:
                    mappings = [
                        {"repo_name": None, "collection_name": default_collection},
                    ]
                for mapping in mappings:
                    if shutdown_event.is_set():
                        break
                    coll = mapping.get("collection_name") or default_collection
                    repo_name: Optional[str] = mapping.get("repo_name")
                    if not coll:
                        continue
                    try:
                        if is_multi_repo_mode() and repo_name:
                            state_dir = _get_repo_state_dir(repo_name)
                        else:
                            state_dir = _get_global_state_dir(str(watch_config.ROOT))
                        lock_path = state_dir / "pseudo.lock"
                        with _cross_process_lock(lock_path):
                            processed = idx.pseudo_backfill_tick(
                                client,
                                coll,
                                repo_name=repo_name,
                                max_points=max_points,
                                dim=model_dim,
                                vector_name=vector_name,
                            )
                            if processed:
                                logger.info(
                                    "[pseudo_backfill] repo=%s collection=%s processed=%d",
                                    repo_name or "default", coll, processed,
                                )
                        # Optional: backfill graph edge collection from main points.
                        # Controlled separately because it may scan large collections over time.
                        # Run under its own lock to avoid blocking pseudo/tag backfill workers.
                        if graph_backfill_enabled:
                            try:
                                graph_lock_path = state_dir / "graph_edges.lock"
                                with _cross_process_lock(graph_lock_path):
                                    files_done = idx.graph_edges_backfill_tick(
                                        client,
                                        coll,
                                        repo_name=repo_name,
                                        max_files=graph_max_files,
                                    )
                                if files_done:
                                    logger.info(
                                        "[graph_backfill] repo=%s collection=%s files=%d",
                                        repo_name or "default",
                                        coll,
                                        files_done,
                                    )
                            except Exception as exc:
                                logger.error(
                                    "[graph_backfill] error repo=%s collection=%s: %s",
                                    repo_name or "default",
                                    coll,
                                    exc,
                                    exc_info=True,
                                )
                    except Exception as exc:
                        logger.error(
                            "[pseudo_backfill] error repo=%s collection=%s: %s",
                            repo_name or "default", coll, exc,
                            exc_info=True,
                        )
            except Exception:
                logger.error(
                    "[pseudo_backfill] unexpected error in worker loop",
                    exc_info=True,
                )
            # Use event.wait for interruptible sleep
            shutdown_event.wait(timeout=interval)

    thread = threading.Thread(target=_worker, name="pseudo-backfill", daemon=True)
    thread.start()
    return shutdown_event


__all__ = ["_start_pseudo_backfill_worker"]
