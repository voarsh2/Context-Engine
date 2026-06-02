#!/usr/bin/env python3
from __future__ import annotations

import os
import time
from collections import Counter
from pathlib import Path
from typing import Optional

from qdrant_client import QdrantClient
from watchdog.observers import Observer

from scripts.watch_index_core import config as watch_config
from scripts.watch_index_core.config import LOGGER, MODEL, QDRANT_URL, default_collection_name
from scripts.watch_index_core.utils import (
    get_boolean_env,
    resolve_vector_name_config,
    create_observer,
)
from scripts.watch_index_core.handler import IndexHandler
from scripts.watch_index_core.init_maintenance import start_init_maintenance_worker
from scripts.watch_index_core.pseudo import _start_pseudo_backfill_worker
from scripts.watch_index_core.processor import _process_paths
from scripts.watch_index_core.queue import ChangeQueue
from scripts.watch_index_core.consistency import (
    run_consistency_audit,
    run_empty_dir_sweep_maintenance,
)
from scripts.workspace_state import (
    compute_indexing_config_hash,
    get_indexing_config_snapshot,
    list_pending_index_journal_entries,
    is_multi_repo_mode,
    persist_indexing_config,
    update_indexing_status,
    initialize_watcher_state,
)

_sleep = time.sleep

import scripts.ingest_code as idx

logger = LOGGER
ROOT = watch_config.ROOT
# Back-compat: legacy modules/tests expect a module-level COLLECTION constant.
# We use a sentinel and a getter to ensure the resolved value is returned.
_COLLECTION: Optional[str] = None
_JOURNAL_DRAIN_LAST_LOG = 0.0
_JOURNAL_DRAIN_LAST_TOTAL = 0


def get_collection() -> str:
    """Return the resolved collection name or the env default."""
    if _COLLECTION is not None:
        return _COLLECTION
    return default_collection_name()


def _set_runtime_root() -> None:
    global ROOT
    runtime_root = Path(
        os.environ.get("WATCH_ROOT")
        or os.environ.get("WORKSPACE_PATH")
        or str(ROOT)
    )
    try:
        runtime_root = runtime_root.resolve()
    except Exception:
        pass

    ROOT = runtime_root
    watch_config.ROOT = runtime_root


def _journal_log_interval_secs() -> float:
    try:
        return max(0.0, float(os.environ.get("WATCH_JOURNAL_LOG_INTERVAL_SECS", "120") or 120.0))
    except Exception:
        return 120.0


def _maybe_log_journal_drain(
    *,
    total: int,
    queued: int,
    op_counts: Counter[str],
    queue: ChangeQueue,
) -> None:
    global _JOURNAL_DRAIN_LAST_LOG, _JOURNAL_DRAIN_LAST_TOTAL
    now = time.time()
    interval = _journal_log_interval_secs()
    should_log = False
    if total <= 0 and _JOURNAL_DRAIN_LAST_TOTAL > 0:
        should_log = True
    elif total > 0 and (_JOURNAL_DRAIN_LAST_LOG <= 0 or (now - _JOURNAL_DRAIN_LAST_LOG) >= interval):
        should_log = True
    if not should_log:
        _JOURNAL_DRAIN_LAST_TOTAL = total
        return

    queue_stats = {}
    try:
        queue_stats = queue.stats()
    except Exception:
        queue_stats = {}
    logger.info(
        "watch_index::journal_drain backlog=%d queued=%d ops=%s queue=%s",
        total,
        queued,
        dict(op_counts),
        queue_stats,
        extra={
            "root": str(ROOT),
            "backlog": total,
            "queued": queued,
            "op_counts": dict(op_counts),
            "queue_stats": queue_stats,
        },
    )
    _JOURNAL_DRAIN_LAST_LOG = now
    _JOURNAL_DRAIN_LAST_TOTAL = total


def _drain_pending_journal(queue: ChangeQueue) -> None:
    pending_path: Optional[str] = None
    try:
        pending_entries = list_pending_index_journal_entries(str(ROOT))
        queued = 0
        op_counts: Counter[str] = Counter()
        for pending_entry in pending_entries:
            op_type = str(pending_entry.get("op_type") or "unknown").strip() or "unknown"
            op_counts[op_type] += 1
            pending_path = str(pending_entry.get("path") or "").strip()
            if pending_path:
                queue.add(Path(pending_path), force=True)
                queued += 1
        _maybe_log_journal_drain(
            total=len(pending_entries),
            queued=queued,
            op_counts=op_counts,
            queue=queue,
        )
    except Exception as exc:
        logger.exception(
            "watch_index::pending_journal_drain_failed",
            extra={"root": str(ROOT), "pending_path": pending_path, "error": str(exc)},
        )


def _run_periodic_maintenance(client: QdrantClient) -> None:
    try:
        run_consistency_audit(client, ROOT)
    except Exception as exc:
        logger.exception(
            "watch_index::consistency_audit_failed",
            extra={"root": str(ROOT), "error": str(exc)},
        )
    try:
        run_empty_dir_sweep_maintenance(ROOT)
    except Exception as exc:
        logger.exception(
            "watch_index::empty_dir_sweep_failed",
            extra={"root": str(ROOT), "error": str(exc)},
        )


def _maintenance_interval_secs() -> float:
    try:
        return max(0.0, float(os.environ.get("WATCH_MAINTENANCE_INTERVAL_SECS", "300") or 300.0))
    except Exception:
        return 300.0


def main() -> None:
    _set_runtime_root()

    # Resolve collection name from workspace state before any client/state ops
    try:
        from scripts.workspace_state import get_collection_name_with_staging as _get_coll
    except Exception:
        _get_coll = None

    multi_repo_enabled = False
    try:
        multi_repo_enabled = bool(is_multi_repo_mode())
    except Exception:
        multi_repo_enabled = False

    default_collection = default_collection_name()
    # In multi-repo mode, per-repo collections are resolved via _get_collection_for_file
    # and workspace_state; avoid deriving a root-level collection like "/work-<hash>".
    if _get_coll and not multi_repo_enabled:
        try:
            resolved = _get_coll(str(ROOT))
            if resolved:
                default_collection = resolved
        except Exception:
            pass
    if multi_repo_enabled:
        print("[multi_repo] Multi-repo mode enabled - per-repo collections in use")
    else:
        print("[single_repo] Single-repo mode enabled - using single collection")

    global _COLLECTION, COLLECTION
    _COLLECTION = default_collection
    COLLECTION = _COLLECTION

    print(
        f"Watch mode: root={ROOT} qdrant={QDRANT_URL} collection={default_collection} model={MODEL}"
    )

    # Guardrail: deferring pseudo to a worker only makes sense if the worker is enabled.
    # Otherwise you'd silently disable pseudo generation (old behavior).
    pseudo_defer = get_boolean_env("PSEUDO_DEFER_TO_WORKER")
    pseudo_backfill_enabled = get_boolean_env("PSEUDO_BACKFILL_ENABLED")
    if pseudo_defer and not pseudo_backfill_enabled:
        print(
            "[pseudo] Warning: PSEUDO_DEFER_TO_WORKER=1 but PSEUDO_BACKFILL_ENABLED=0; "
            "inline pseudo will remain enabled (no deferral)."
        )

    # Health check: detect and auto-heal cache/collection sync issues.
    # In multi-repo mode this can be expensive and may duplicate external init checks,
    # so default it OFF unless explicitly enabled.
    run_health_check = get_boolean_env(
        "WATCH_COLLECTION_HEALTHCHECK",
        default=(False if multi_repo_enabled else True),
    )
    if run_health_check:
        try:
            from scripts.collection_health import auto_heal_if_needed, auto_heal_multi_repo

            print("[health_check] Checking collection health...")
            if multi_repo_enabled:
                # Multi-repo: check each repo's collection
                heal_result = auto_heal_multi_repo(str(ROOT), QDRANT_URL, dry_run=False)
                if heal_result.get("repos_healed", 0) > 0:
                    print(
                        f"[health_check] Cleared cache for {heal_result['repos_healed']} repos with empty collections"
                    )
                elif heal_result.get("repos_checked", 0) > 0:
                    print(f"[health_check] Checked {heal_result['repos_checked']} repos - all OK")
                else:
                    print("[health_check] No repos with cached state to check")
            else:
                # Single-repo mode
                heal_result = auto_heal_if_needed(
                    str(ROOT), default_collection, QDRANT_URL, dry_run=False
                )
                if heal_result.get("action_taken") == "cleared_cache":
                    print("[health_check] Cache cleared due to sync issue - files will be reindexed")
                elif not heal_result.get("health_check", {}).get("healthy", True):
                    print(
                        f"[health_check] Issue detected: {heal_result['health_check'].get('issue', 'unknown')}"
                    )
                else:
                    print("[health_check] Collection health OK")
        except Exception as e:
            print(f"[health_check] Warning: health check failed: {e}")

    client = QdrantClient(
        url=QDRANT_URL, timeout=int(os.environ.get("QDRANT_TIMEOUT", "20") or 20)
    )

    from scripts.embedder import get_embedding_model, get_model_dimension

    model = get_embedding_model(MODEL)
    model_dim = get_model_dimension(MODEL)

    vector_name = resolve_vector_name_config(client, default_collection, model_dim, MODEL)

    # In multi-repo mode we index into per-repo collections; don't eagerly touch/create
    # the default collection unless explicitly enabled.
    ensure_default_collection = get_boolean_env(
        "WATCH_ENSURE_DEFAULT_COLLECTION",
        default=(False if multi_repo_enabled else True),
    )
    if ensure_default_collection:
        try:
            idx.ensure_collection_and_indexes_once(
                client, default_collection, model_dim, vector_name
            )
        except Exception:
            pass

    # Start backfill worker even in multi-repo mode; it uses workspace mappings and
    # will no-op if disabled. Only allow a single-repo fallback to the default
    # collection when startup was explicitly permitted to touch that collection.
    _start_pseudo_backfill_worker(
        client,
        default_collection,
        model_dim,
        vector_name,
        allow_default_collection_fallback=ensure_default_collection,
    )
    init_maintenance_shutdown = start_init_maintenance_worker()

    try:
        initialize_watcher_state(str(ROOT), multi_repo_enabled, default_collection)
    except Exception as e:
        print(f"[workspace_state] Error initializing workspace state: {e}")

    q = ChangeQueue(
        lambda paths: _process_paths(
            paths, client, model, vector_name, model_dim, str(ROOT)
        )
    )
    handler = IndexHandler(ROOT, q, client, default_collection)

    use_polling = get_boolean_env("WATCH_USE_POLLING")
    obs = create_observer(use_polling, observer_cls=Observer)
    obs.schedule(handler, str(ROOT), recursive=True)
    obs.start()

    maintenance_interval = _maintenance_interval_secs()
    last_maintenance: Optional[float] = None

    try:
        while True:
            # Watcher is the sole durable journal consumer in v1. Upload/apply
            # records upsert/delete intent here so missed filesystem events can
            # still be replayed after watcher/container restarts.
            _drain_pending_journal(q)
            now = time.time()
            if last_maintenance is None or (now - last_maintenance) >= maintenance_interval:
                _run_periodic_maintenance(client)
                last_maintenance = now
            _sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        if init_maintenance_shutdown is not None:
            init_maintenance_shutdown.set()
        obs.stop()
        obs.join()


if __name__ == "__main__":
    main()

# For legacy compatibility, provide a global COLLECTION variable. 
# Due to import binding, this will reflect the state at the end of the module execution.
COLLECTION = get_collection()
