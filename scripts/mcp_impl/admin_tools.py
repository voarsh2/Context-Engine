#!/usr/bin/env python3
"""
mcp/admin_tools.py - Admin tool helper functions for MCP indexer server.

Extracted from mcp_indexer_server.py for better modularity.
Contains:
- Subprocess runner (_run_async)
- Embedding model cache (_get_embedding_model)
- Repo detection (_detect_current_repo)

Note: The @mcp.tool() decorated functions remain in mcp_indexer_server.py
as thin wrappers that call these helpers.
"""

from __future__ import annotations

__all__ = [
    # Constants
    "_EMBED_MODEL_CACHE",
    "_EMBED_MODEL_LOCKS",
    # Functions
    "_run_async",
    "_get_embedding_model",
    "_detect_current_repo",
    "_collection_map_impl",
]

import asyncio
import logging
import os
import subprocess
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Embedding model cache
# ---------------------------------------------------------------------------
_EMBED_MODEL_CACHE: Dict[str, Any] = {}
_EMBED_MODEL_LOCKS: Dict[str, threading.Lock] = {}


def _get_embedding_model(model_name: str):
    """Get cached embedding model via the centralized embedder factory."""
    from scripts.embedder import get_embedding_model

    return get_embedding_model(model_name)


# ---------------------------------------------------------------------------
# Subprocess runner
# ---------------------------------------------------------------------------
async def _run_async(
    cmd: list[str],
    env: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Run subprocess with proper resource management using SubprocessManager."""
    from scripts.async_subprocess_manager import run_subprocess_async

    # Default timeout from env if not provided
    if timeout is None:
        timeout = float(os.environ.get("MCP_TOOL_TIMEOUT_SECS", "600"))

    return await run_subprocess_async(cmd, timeout=timeout, env=env)



# ---------------------------------------------------------------------------
# Repo detection
# ---------------------------------------------------------------------------
def _detect_current_repo() -> Optional[str]:
    """Detect the current repository name from workspace/env.

    Priority:
    1. CURRENT_REPO env var (explicitly set)
    2. REPO_NAME env var
    3. Bindmount git detection when CTXCE_BINDMOUNT_REPO_DETECTION=1

    Returns: repo name or None if detection fails
    """
    # Check explicit env vars first
    for env_key in ("CURRENT_REPO", "REPO_NAME"):
        val = os.environ.get(env_key, "").strip()
        if val:
            return val

    try:
        from scripts.workspace_state import bindmount_repo_detection_enabled

        allow_git_detection = bindmount_repo_detection_enabled()
    except Exception:
        allow_git_detection = False

    if not allow_git_detection:
        return None

    # Bindmount detection from /work. Do not guess from invalid/internal
    # metadata: a leaked /work/.git must not become repo "work".
    work_path = Path("/work")
    if work_path.exists():
        try:
            if (work_path / ".git").exists():
                result = subprocess.run(
                    ["git", "-C", str(work_path), "config", "--get", "remote.origin.url"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                if result.returncode == 0 and result.stdout.strip():
                    url = result.stdout.strip()
                    name = url.rstrip("/").rsplit("/", 1)[-1]
                    if name.endswith(".git"):
                        name = name[:-4]
                    if name:
                        return name

            internal_dirs = {".codebase", ".git", "__pycache__"}
            for subdir in work_path.iterdir():
                if subdir.name in internal_dirs:
                    continue
                if subdir.is_dir() and (subdir / ".git").exists():
                    return subdir.name
        except Exception:
            pass

    return None


# ---------------------------------------------------------------------------
# Collection map implementation
# ---------------------------------------------------------------------------
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")


async def _collection_map_impl(
    search_root: Optional[str] = None,
    collection: Optional[str] = None,
    repo_name: Optional[str] = None,
    include_samples: Optional[bool] = None,
    limit: Optional[int] = None,
    coerce_bool_fn=None,
) -> Dict[str, Any]:
    """Return collection↔repo mappings with optional Qdrant payload samples.

    Implementation extracted from mcp_indexer_server.py for modularity.
    """
    from scripts.mcp_impl.utils import _coerce_bool

    _coerce = coerce_bool_fn or _coerce_bool

    def _norm_str(val: Any) -> Optional[str]:
        if val is None:
            return None
        try:
            s = str(val).strip()
        except Exception:
            return None
        return s or None

    collection_filter = _norm_str(collection)
    repo_filter = _norm_str(repo_name)
    sample_flag = _coerce(include_samples, False)

    max_entries: Optional[int] = None
    if limit is not None:
        try:
            max_entries = max(1, int(limit))
        except Exception:
            max_entries = None

    state_entries: List[Dict[str, Any]] = []
    state_error: Optional[str] = None

    try:
        from scripts.workspace_state import get_collection_mappings as _get_collection_mappings  # type: ignore

        try:
            state_entries = await asyncio.to_thread(
                lambda: _get_collection_mappings(search_root)
            )
        except Exception as exc:
            state_error = str(exc)
            state_entries = []
    except Exception as exc:  # pragma: no cover
        state_error = f"workspace_state unavailable: {exc}"
        state_entries = []

    if repo_filter:
        state_entries = [
            entry for entry in state_entries if _norm_str(entry.get("repo_name")) == repo_filter
        ]
    if collection_filter:
        state_entries = [
            entry
            for entry in state_entries
            if _norm_str(entry.get("collection_name")) == collection_filter
        ]

    results: List[Dict[str, Any]] = []
    seen_collections: set[str] = set()

    for entry in state_entries:
        item = dict(entry)
        item["source"] = "state"
        results.append(item)
        coll = _norm_str(entry.get("collection_name"))
        if coll:
            seen_collections.add(coll)

    # Qdrant helpers -----------------------------------------------------
    sample_cache: Dict[str, Tuple[Optional[Dict[str, Any]], Optional[str]]] = {}
    qdrant_error: Optional[str] = None
    qdrant_used = False
    client = None

    def _ensure_qdrant_client():
        nonlocal client, qdrant_error, qdrant_used
        if client is not None or qdrant_error:
            return client
        try:
            from qdrant_client import QdrantClient  # type: ignore
        except Exception as exc:  # pragma: no cover
            qdrant_error = f"qdrant_client unavailable: {exc}"
            return None

        try:
            qdrant_used = True
            return QdrantClient(
                url=QDRANT_URL,
                api_key=os.environ.get("QDRANT_API_KEY"),
                timeout=float(os.environ.get("QDRANT_TIMEOUT", "20") or 20),
            )
        except Exception as exc:  # pragma: no cover
            qdrant_error = str(exc)
            return None

    async def _sample_payload(coll_name: Optional[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        key = _norm_str(coll_name) or ""
        if not key:
            return None, "missing_collection"
        if key in sample_cache:
            return sample_cache[key]

        cli = _ensure_qdrant_client()
        if cli is None:
            sample_cache[key] = (None, qdrant_error)
            return sample_cache[key]

        def _scroll_one():
            try:
                points, _ = cli.scroll(
                    collection_name=key,
                    limit=1,
                    with_payload=True,
                    with_vectors=False,
                )
                return points
            except Exception as exc:  # pragma: no cover
                raise exc

        try:
            points = await asyncio.to_thread(_scroll_one)
        except Exception as exc:  # pragma: no cover
            err = str(exc)
            sample_cache[key] = (None, err)
            return sample_cache[key]

        if not points:
            sample_cache[key] = (None, None)
            return sample_cache[key]

        payload = points[0].payload or {}
        metadata = payload.get("metadata") or {}
        sample = {
            "host_path": metadata.get("host_path"),
            "container_path": metadata.get("container_path"),
            "path": metadata.get("path") or payload.get("path"),
            "start_line": metadata.get("start_line"),
            "end_line": metadata.get("end_line"),
        }
        sample_cache[key] = (sample, None)
        return sample_cache[key]

    # Attach samples to state-backed entries when requested
    if sample_flag and results:
        for entry in results:
            coll_name = entry.get("collection_name")
            sample, err = await _sample_payload(coll_name)
            if sample:
                entry["sample"] = sample
            if err:
                entry.setdefault("warnings", []).append(err)

    # If no state entries (or explicit collection filtered out), fall back to Qdrant listings
    fallback_entries: List[Dict[str, Any]] = []
    need_qdrant_listing = not results

    if need_qdrant_listing:
        cli = _ensure_qdrant_client()
        if cli is not None:
            def _list_collections():
                info = cli.get_collections()
                return [c.name for c in info.collections]

            try:
                collection_names = await asyncio.to_thread(_list_collections)
            except Exception as exc:  # pragma: no cover
                qdrant_error = str(exc)
                collection_names = []

            if collection_filter:
                collection_names = [
                    name for name in collection_names if _norm_str(name) == collection_filter
                ]

            count = 0
            for name in collection_names:
                if name in seen_collections:
                    continue
                entry: Dict[str, Any] = {
                    "collection_name": name,
                    "source": "qdrant",
                }
                sample, err = await _sample_payload(name) if sample_flag else (None, None)
                if sample:
                    entry["sample"] = sample
                if err:
                    entry.setdefault("warnings", []).append(err)
                fallback_entries.append(entry)
                count += 1
                if max_entries is not None and count >= max_entries:
                    break

    entries = results + fallback_entries

    return {
        "results": entries,
        "counts": {
            "state": len(state_entries),
            "returned": len(entries),
            "fallback": len(fallback_entries),
        },
        "errors": {
            "state": state_error,
            "qdrant": qdrant_error,
        },
        "qdrant_used": qdrant_used,
        "filters": {
            "collection": collection_filter,
            "repo_name": repo_filter,
            "search_root": search_root,
            "include_samples": sample_flag,
            "limit": max_entries,
        },
    }
