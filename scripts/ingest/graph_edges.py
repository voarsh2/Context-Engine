#!/usr/bin/env python3
"""
ingest/graph_edges.py - Materialized graph edges in Qdrant.

This is a small, MIT-safe reimplementation of the "graph edges collection" idea:
- Maintain a dedicated Qdrant collection named `<base_collection>_graph`
- Store payload-only edge docs for fast lookups:
  - callers/importers queries become simple keyword filters on an indexed payload field

Design goals for this branch:
- Keep this as an *accelerator* (symbol_graph still works without it)
- Avoid Neo4j/PageRank/GraphRAG complexity
- Avoid CLI flags; watcher can backfill opportunistically
"""

from __future__ import annotations

import hashlib
import logging
import os
from typing import Any, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

GRAPH_COLLECTION_SUFFIX = "_graph"

EDGE_TYPE_CALLS = "calls"
EDGE_TYPE_IMPORTS = "imports"

GRAPH_INDEX_FIELDS: Tuple[str, ...] = (
    "caller_path",
    "callee_symbol",
    "edge_type",
    "repo",
)

_ENSURED_GRAPH_COLLECTIONS: set[str] = set()
_GRAPH_VECTOR_MODE: dict[str, str] = {}
_MISSING_GRAPH_COLLECTIONS: set[str] = set()
_BACKFILL_OFFSETS: dict[tuple[str, Optional[str]], Any] = {}

_EDGE_VECTOR_NAME = "_edge"
_EDGE_VECTOR_VALUE = [0.0]


def _normalize_path(path: str) -> str:
    if not path:
        return ""
    try:
        normalized = os.path.normpath(str(path))
    except Exception:
        normalized = str(path)
    return normalized.replace("\\", "/")


def normalize_caller_path(path: str) -> str:
    """Normalize a caller path exactly as graph edge payloads do.

    This is used by both the writer (upsert/delete) and any readers/verifiers so
    cross-platform path separators (Windows vs POSIX) do not cause mismatches.
    """

    return _normalize_path(path)


def get_graph_collection_name(base_collection: str) -> str:
    return f"{base_collection}{GRAPH_COLLECTION_SUFFIX}"


def _edge_vector_for_upsert(graph_collection: str) -> dict:
    mode = _GRAPH_VECTOR_MODE.get(graph_collection)
    if mode == "named":
        return {_EDGE_VECTOR_NAME: _EDGE_VECTOR_VALUE}
    return {}


def ensure_graph_collection(client: Any, base_collection: str) -> Optional[str]:
    """Ensure `<base_collection>_graph` exists and has payload indexes."""
    from qdrant_client import models as qmodels
    from qdrant_client.http.exceptions import UnexpectedResponse

    if not base_collection:
        return None
    graph_coll = get_graph_collection_name(base_collection)
    if graph_coll in _ENSURED_GRAPH_COLLECTIONS:
        return graph_coll

    def _detect_vector_mode(info: Any) -> str:
        try:
            vectors = getattr(
                getattr(getattr(info, "config", None), "params", None), "vectors", None
            )
            if isinstance(vectors, dict):
                return "none" if not vectors else "named"
            return "none" if vectors is None else "named"
        except Exception:
            return "named"

    try:
        info = client.get_collection(graph_coll)
        _GRAPH_VECTOR_MODE[graph_coll] = _detect_vector_mode(info)
        _ENSURED_GRAPH_COLLECTIONS.add(graph_coll)
        _MISSING_GRAPH_COLLECTIONS.discard(graph_coll)
        return graph_coll
    except UnexpectedResponse as e:
        # Only a 404 means "missing"; any other HTTP failure should be visible.
        if getattr(e, "status_code", None) != 404:
            logger.exception(
                "Failed to get graph collection %s (status=%s): %s",
                graph_coll,
                getattr(e, "status_code", None),
                e,
            )
            return None
    except Exception as e:
        logger.exception("Failed to get graph collection %s: %s", graph_coll, e)
        return None

    try:
        # Prefer vector-less collection when supported by server/client.
        try:
            client.create_collection(
                collection_name=graph_coll,
                vectors_config={},
            )
            _GRAPH_VECTOR_MODE[graph_coll] = "none"
        except Exception as vec_exc:
            logger.debug(
                "Vector-less creation failed for %s, trying named vector: %s",
                graph_coll,
                vec_exc,
            )
            client.create_collection(
                collection_name=graph_coll,
                vectors_config={
                    _EDGE_VECTOR_NAME: qmodels.VectorParams(
                        size=1, distance=qmodels.Distance.COSINE
                    )
                },
            )
            _GRAPH_VECTOR_MODE[graph_coll] = "named"

        # Create payload indexes (best-effort).
        for field in GRAPH_INDEX_FIELDS:
            try:
                client.create_payload_index(
                    collection_name=graph_coll,
                    field_name=field,
                    field_schema=qmodels.PayloadSchemaType.KEYWORD,
                )
            except Exception as e:
                logger.debug(
                    "Failed to create graph payload index '%s' for %s: %s",
                    field,
                    graph_coll,
                    e,
                    exc_info=True,
                )

        _ENSURED_GRAPH_COLLECTIONS.add(graph_coll)
        _MISSING_GRAPH_COLLECTIONS.discard(graph_coll)
        return graph_coll
    except Exception as e:
        logger.debug("Failed to ensure graph collection %s: %s", graph_coll, e)
        return None


def _edge_id(edge_type: str, repo: str, caller_path: str, callee_symbol: str) -> str:
    key = f"{edge_type}\x00{repo}\x00{caller_path}\x00{callee_symbol}"
    return hashlib.sha256(key.encode("utf-8", errors="ignore")).hexdigest()[:32]


def _iter_edges(
    *,
    caller_path: str,
    repo: str,
    calls: Iterable[str] = (),
    imports: Iterable[str] = (),
) -> List[Dict[str, Any]]:
    norm_path = _normalize_path(caller_path)
    repo_s = (repo or "").strip() or "default"

    edges: List[Dict[str, Any]] = []
    for sym in calls or []:
        s = str(sym).strip()
        if not s:
            continue
        edges.append(
            {
                "id": _edge_id(EDGE_TYPE_CALLS, repo_s, norm_path, s),
                "payload": {
                    "caller_path": norm_path,
                    "callee_symbol": s,
                    "edge_type": EDGE_TYPE_CALLS,
                    "repo": repo_s,
                },
            }
        )
    for sym in imports or []:
        s = str(sym).strip()
        if not s:
            continue
        edges.append(
            {
                "id": _edge_id(EDGE_TYPE_IMPORTS, repo_s, norm_path, s),
                "payload": {
                    "caller_path": norm_path,
                    "callee_symbol": s,
                    "edge_type": EDGE_TYPE_IMPORTS,
                    "repo": repo_s,
                },
            }
        )
    return edges


def upsert_file_edges(
    client: Any,
    base_collection: str,
    *,
    caller_path: str,
    repo: str | None,
    calls: List[str] | None = None,
    imports: List[str] | None = None,
) -> int:
    graph_coll = ensure_graph_collection(client, base_collection)
    if not graph_coll:
        return 0
    edges = _iter_edges(
        caller_path=caller_path,
        repo=repo or "default",
        calls=calls or [],
        imports=imports or [],
    )
    if not edges:
        return 0

    from qdrant_client import models as qmodels

    points = [
        qmodels.PointStruct(
            id=e["id"],
            vector=_edge_vector_for_upsert(graph_coll),
            payload=e["payload"],
        )
        for e in edges
    ]
    try:
        client.upsert(collection_name=graph_coll, points=points, wait=True)
        return len(points)
    except Exception as e:
        logger.debug("Graph edge upsert failed for %s: %s", caller_path, e)
        return 0


def delete_edges_by_path(
    client: Any,
    base_collection: str,
    *,
    caller_path: str,
    repo: str | None = None,
) -> int:
    from qdrant_client.http.exceptions import UnexpectedResponse
    graph_coll = get_graph_collection_name(base_collection)
    if graph_coll in _MISSING_GRAPH_COLLECTIONS:
        return 0

    from qdrant_client import models as qmodels

    norm_path = _normalize_path(caller_path)
    must: list[Any] = [
        qmodels.FieldCondition(
            key="caller_path", match=qmodels.MatchValue(value=norm_path)
        )
    ]
    if repo:
        r = str(repo).strip()
        if r and r != "*":
            must.append(
                qmodels.FieldCondition(key="repo", match=qmodels.MatchValue(value=r))
            )
    flt = qmodels.Filter(must=must)

    # Probe first so callers can distinguish "no matching rows" (0) from a real delete.
    # This is important for fallback logic (e.g., retry path-only delete when repo tag drifted).
    try:
        existing, _ = client.scroll(
            collection_name=graph_coll,
            scroll_filter=flt,
            limit=1,
            with_payload=False,
            with_vectors=False,
        )
        if not existing:
            return 0
    except UnexpectedResponse as e:
        if getattr(e, "status_code", None) == 404:
            _MISSING_GRAPH_COLLECTIONS.add(graph_coll)
            return 0
        logger.debug(
            "Graph edge probe failed for %s in %s (status=%s): %s",
            norm_path,
            graph_coll,
            getattr(e, "status_code", None),
            e,
            exc_info=True,
        )
        return 0
    except Exception as e:
        logger.debug(
            "Graph edge probe failed for %s in %s: %s",
            norm_path,
            graph_coll,
            e,
            exc_info=True,
        )
        return 0

    try:
        resp = client.delete(
            collection_name=graph_coll,
            points_selector=qmodels.FilterSelector(filter=flt),
        )
        result_status = getattr(getattr(resp, "result", None), "status", None)
        if result_status is None:
            result_status = getattr(resp, "status", None)
        if result_status is None:
            return 1
        status_s = str(result_status).strip().lower()
        return 1 if status_s in {"acknowledged", "completed", "ok", "success"} else 0
    except UnexpectedResponse as e:
        if getattr(e, "status_code", None) == 404:
            _MISSING_GRAPH_COLLECTIONS.add(graph_coll)
            return 0
        logger.debug(
            "Graph edge delete failed for %s in %s (status=%s): %s",
            norm_path,
            graph_coll,
            getattr(e, "status_code", None),
            e,
            exc_info=True,
        )
        return 0
    except Exception as e:
        logger.debug(
            "Graph edge delete failed for %s in %s: %s",
            norm_path,
            graph_coll,
            e,
            exc_info=True,
        )
        return 0


def graph_edges_backfill_tick(
    client: Any,
    base_collection: str,
    *,
    repo_name: str | None = None,
    max_files: int = 128,
) -> int:
    """Best-effort incremental backfill from `<base_collection>` into `<base_collection>_graph`.

    This scans the main collection and upserts file-level edges into the graph collection.
    It's idempotent (deterministic IDs) and safe to run continuously in a watcher worker.
    """
    from qdrant_client import models as qmodels

    if not base_collection or max_files <= 0:
        return 0

    graph_coll = ensure_graph_collection(client, base_collection)
    if not graph_coll:
        return 0

    must: list[Any] = []
    if repo_name:
        must.append(
            qmodels.FieldCondition(
                key="metadata.repo", match=qmodels.MatchValue(value=repo_name)
            )
        )
    flt = qmodels.Filter(must=must or None)

    processed_files = 0
    seen_paths: set[str] = set()

    key = (base_collection, repo_name)
    next_offset = _BACKFILL_OFFSETS.get(key)

    # We may need to overscan because the main collection is chunked.
    overscan = max_files * 8
    while processed_files < max_files:
        attempts = 0
        while True:
            try:
                points, next_offset = client.scroll(
                    collection_name=base_collection,
                    scroll_filter=flt,
                    limit=min(64, overscan),
                    with_payload=True,
                    with_vectors=False,
                    offset=next_offset,
                )
                break
            except Exception as e:
                attempts += 1
                logger.exception(
                    "Graph edge backfill scroll failed (collection=%s repo=%s offset=%s attempt=%d): %s",
                    base_collection,
                    repo_name or "default",
                    next_offset,
                    attempts,
                    e,
                )
                # Retry a couple times for transient errors, then raise so failures are not silent.
                if attempts >= 3:
                    raise
                import time

                time.sleep(0.25 * (2 ** (attempts - 1)))

        if not points:
            break

        for rec in points:
            if processed_files >= max_files:
                break
            payload = getattr(rec, "payload", None) or {}
            md = payload.get("metadata") or {}
            path = md.get("path") or ""
            if not path:
                continue
            norm_path = _normalize_path(str(path))
            if norm_path in seen_paths:
                continue
            seen_paths.add(norm_path)

            repo = md.get("repo") or repo_name or "default"
            calls = md.get("calls") or []
            imports = md.get("imports") or []
            if not isinstance(calls, list):
                calls = []
            if not isinstance(imports, list):
                imports = []

            upsert_file_edges(
                client,
                base_collection,
                caller_path=norm_path,
                repo=str(repo),
                calls=[str(x) for x in calls if x],
                imports=[str(x) for x in imports if x],
            )
            processed_files += 1

        if next_offset is None:
            break

    _BACKFILL_OFFSETS[key] = next_offset
    return processed_files


__all__ = [
    "GRAPH_COLLECTION_SUFFIX",
    "EDGE_TYPE_CALLS",
    "EDGE_TYPE_IMPORTS",
    "get_graph_collection_name",
    "ensure_graph_collection",
    "upsert_file_edges",
    "delete_edges_by_path",
    "graph_edges_backfill_tick",
]
