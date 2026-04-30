#!/usr/bin/env python3
import os
import hashlib
from pathlib import Path
from typing import Tuple, Any

from qdrant_client import QdrantClient, models
try:
    from scripts.ingest.graph_edges import (
        delete_edges_by_path as _shared_delete_graph_edges_by_path,
        get_graph_collection_name as _shared_graph_collection_name,
    )
except Exception:
    _shared_delete_graph_edges_by_path = None  # type: ignore[assignment]
    _shared_graph_collection_name = None  # type: ignore[assignment]

COLLECTION = os.environ.get("COLLECTION_NAME", "codebase")
QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6333")
API_KEY = os.environ.get("QDRANT_API_KEY")
ROOT = Path(os.environ.get("PRUNE_ROOT", ".")).resolve()
GRAPH_COLLECTION = (
    _shared_graph_collection_name(COLLECTION)
    if _shared_graph_collection_name is not None
    else f"{COLLECTION}_graph"
)


def _norm_path(path_str: Any) -> str:
    if not path_str:
        return ""
    try:
        normalized = os.path.normpath(str(path_str))
    except Exception:
        normalized = str(path_str)
    return normalized.replace("\\", "/")


def sha1_file(path: Path) -> str:
    try:
        data = path.read_bytes()
    except Exception:
        return ""
    return hashlib.sha1(data).hexdigest()


def delete_by_path(client: QdrantClient, path_str: str) -> int:
    flt = models.Filter(
        must=[
            models.FieldCondition(
                key="metadata.path", match=models.MatchValue(value=path_str)
            )
        ]
    )
    try:
        res = client.delete(
            collection_name=COLLECTION,
            points_selector=models.FilterSelector(filter=flt),
        )
        return 1
    except Exception:
        return 0


def delete_graph_edges_by_path(client: QdrantClient, path_str: str, repo: str | None = None) -> int:
    """Best-effort deletion for graph-edge collections (if present).

    Some deployments store symbol-graph edges in a separate Qdrant collection
    (commonly `${COLLECTION}_graph`). On this branch, edge docs are file-level and
    reference a file path as `caller_path`.
    """
    if not path_str:
        return 0
    path_str = _norm_path(path_str)

    # Canonical path: shared graph-edge deleter against <collection>_graph.
    if _shared_delete_graph_edges_by_path is None:
        return 0
    try:
        return int(
            _shared_delete_graph_edges_by_path(
                client,
                COLLECTION,
                caller_path=path_str,
                repo=repo,
            )
            or 0
        )
    except Exception:
        return 0


def _graph_collection_exists(client: QdrantClient) -> bool:
    try:
        client.get_collection(collection_name=GRAPH_COLLECTION)
        return True
    except Exception:
        return False


def _delete_graph_points_by_ids(client: QdrantClient, ids: list[Any]) -> int:
    if not ids:
        return 0
    try:
        from qdrant_client import models as qmodels
        client.delete(
            collection_name=GRAPH_COLLECTION,
            points_selector=qmodels.PointIdsList(points=ids),
        )
        return len(ids)
    except Exception:
        return 0


def delete_orphan_graph_edges(client: QdrantClient, valid_paths: set[str]) -> int:
    """Delete graph-edge points whose `caller_path` no longer exists in base collection."""
    if not _graph_collection_exists(client):
        return 0

    removed = 0
    next_page = None
    pending_ids: list[Any] = []
    batch_size = 256

    while True:
        try:
            points, next_page = client.scroll(
                collection_name=GRAPH_COLLECTION,
                with_payload=True,
                with_vectors=False,
                limit=512,
                offset=next_page,
                scroll_filter=None,
            )
        except Exception:
            break

        if not points:
            break

        for p in points:
            payload = p.payload or {}
            caller_path = _norm_path(payload.get("caller_path"))
            if not caller_path:
                continue
            if caller_path in valid_paths:
                continue
            pending_ids.append(p.id)
            if len(pending_ids) >= batch_size:
                removed += _delete_graph_points_by_ids(client, pending_ids)
                pending_ids = []

        if next_page is None:
            break

    if pending_ids:
        removed += _delete_graph_points_by_ids(client, pending_ids)

    return removed


def main():
    client = QdrantClient(url=QDRANT_URL, api_key=API_KEY or None)

    seen = set()
    removed_missing = 0
    removed_mismatch = 0
    removed_graph_edges = 0
    removed_orphan_graph_edges = 0

    next_page = None
    while True:
        points, next_page = client.scroll(
            collection_name=COLLECTION,
            with_payload=True,
            limit=256,
            offset=next_page,
            scroll_filter=None,
        )
        if not points:
            break
        for p in points:
            md = (p.payload or {}).get("metadata") or {}
            path_str = md.get("path")
            file_hash = md.get("file_hash")
            norm_path = _norm_path(path_str)
            if not norm_path or norm_path in seen:
                continue
            abs_path = (
                ROOT / Path(path_str).relative_to("/work")
                if path_str.startswith("/work/")
                else ROOT / path_str
            )
            if not abs_path.exists():
                removed_missing += delete_by_path(client, path_str)
                deleted = delete_graph_edges_by_path(client, path_str, md.get("repo"))
                if deleted == 0:
                    # Repo tags can drift across ingestion modes; fall back to path-only delete.
                    deleted = delete_graph_edges_by_path(client, path_str, None)
                removed_graph_edges += deleted
                print(f"[prune] removed missing file points: {path_str}")
                continue
            current_hash = sha1_file(abs_path)
            if file_hash and current_hash and current_hash != file_hash:
                removed_mismatch += delete_by_path(client, path_str)
                deleted = delete_graph_edges_by_path(client, path_str, md.get("repo"))
                if deleted == 0:
                    deleted = delete_graph_edges_by_path(client, path_str, None)
                removed_graph_edges += deleted
                print(f"[prune] removed outdated points (hash mismatch): {path_str}")
                continue

            seen.add(norm_path)

        if next_page is None:
            break

    # Secondary pass: if base points were manually deleted, remove orphan `_graph` edges.
    removed_orphan_graph_edges = delete_orphan_graph_edges(client, seen)

    print(
        "Prune complete. "
        f"removed_missing={removed_missing}, "
        f"removed_mismatch={removed_mismatch}, "
        f"removed_graph_edges={removed_graph_edges}, "
        f"removed_orphan_graph_edges={removed_orphan_graph_edges}"
    )


if __name__ == "__main__":
    main()
