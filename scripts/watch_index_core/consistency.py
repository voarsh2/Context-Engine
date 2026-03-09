from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from qdrant_client import QdrantClient

import scripts.ingest_code as idx
from scripts.workspace_state import (
    _extract_repo_name_from_path,
    _normalize_cache_key_path,
    get_collection_state_snapshot,
    get_workspace_state,
    list_workspaces,
    update_workspace_state,
    upsert_index_journal_entries,
)

from .config import LOGGER
from .utils import get_boolean_env
from .paths import is_internal_metadata_path

logger = LOGGER
_DEFAULT_EMPTY_DIR_SWEEP_INTERVAL_SECONDS = 7 * 24 * 60 * 60


def _consistency_audit_enabled() -> bool:
    return get_boolean_env("WATCH_CONSISTENCY_AUDIT_ENABLED", default=True)


def _consistency_audit_interval_secs() -> int:
    try:
        return max(60, int(os.environ.get("WATCH_CONSISTENCY_AUDIT_INTERVAL_SECS", "86400") or 86400))
    except Exception:
        return 86400


def _consistency_audit_max_paths() -> int:
    try:
        return max(0, int(os.environ.get("WATCH_CONSISTENCY_AUDIT_MAX_PATHS", "200000") or 200000))
    except Exception:
        return 200000


def _consistency_repair_enabled() -> bool:
    return get_boolean_env("WATCH_CONSISTENCY_REPAIR_ENABLED", default=True)


def _consistency_repair_max_ops() -> int:
    try:
        return max(0, int(os.environ.get("WATCH_CONSISTENCY_REPAIR_MAX_OPS", "5000") or 5000))
    except Exception:
        return 5000


def _empty_dir_sweep_enabled() -> bool:
    if "WATCH_EMPTY_DIR_SWEEP_ENABLED" in os.environ:
        return get_boolean_env("WATCH_EMPTY_DIR_SWEEP_ENABLED", default=True)
    return get_boolean_env("CTXCE_UPLOAD_EMPTY_DIR_SWEEP", default=True)


def _empty_dir_sweep_interval_secs() -> int:
    raw = os.environ.get("WATCH_EMPTY_DIR_SWEEP_INTERVAL_SECONDS")
    if raw is None:
        raw = os.environ.get(
            "CTXCE_UPLOAD_EMPTY_DIR_SWEEP_INTERVAL_SECONDS",
            str(_DEFAULT_EMPTY_DIR_SWEEP_INTERVAL_SECONDS),
        )
    try:
        return max(0, int(raw or _DEFAULT_EMPTY_DIR_SWEEP_INTERVAL_SECONDS))
    except Exception:
        return _DEFAULT_EMPTY_DIR_SWEEP_INTERVAL_SECONDS


def _parse_ts(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _should_run_consistency_audit(workspace_path: str, repo_name: Optional[str]) -> bool:
    if not _consistency_audit_enabled():
        return False
    interval = _consistency_audit_interval_secs()
    try:
        state = get_workspace_state(workspace_path=workspace_path, repo_name=repo_name) or {}
    except Exception:
        return True
    maintenance = dict(state.get("maintenance") or {})
    last = _parse_ts(maintenance.get("last_consistency_audit_at"))
    if last is None:
        return True
    age = (datetime.now(timezone.utc) - last).total_seconds()
    return age >= interval


def _sweep_empty_workspace_dirs(workspace_root: Path) -> None:
    protected_top_level = {".codebase", ".remote-git"}
    try:
        workspace_root = workspace_root.resolve()
    except Exception:
        pass
    try:
        for root, _dirnames, _filenames in os.walk(workspace_root, topdown=False):
            current = Path(root)
            if current == workspace_root:
                continue
            if current.parent == workspace_root and current.name in protected_top_level:
                continue
            try:
                rel = current.relative_to(workspace_root)
            except Exception:
                continue
            if rel.parts and rel.parts[0] in protected_top_level:
                continue
            try:
                if any(current.iterdir()):
                    continue
                current.rmdir()
            except Exception:
                continue
    except Exception:
        pass


def _should_run_empty_dir_sweep(workspace_path: str, repo_name: Optional[str]) -> bool:
    if not _empty_dir_sweep_enabled():
        return False
    interval_seconds = _empty_dir_sweep_interval_secs()
    if interval_seconds == 0:
        return True
    try:
        state = get_workspace_state(workspace_path=workspace_path, repo_name=repo_name) or {}
    except Exception:
        return True
    maintenance = state.get("maintenance") or {}
    last_sweep_at = _parse_ts(maintenance.get("last_empty_dir_sweep_at"))
    if last_sweep_at is None:
        return True
    age_seconds = (datetime.now(timezone.utc) - last_sweep_at).total_seconds()
    return age_seconds >= interval_seconds


def _record_empty_dir_sweep(workspace_path: str, repo_name: Optional[str]) -> None:
    try:
        state = get_workspace_state(workspace_path=workspace_path, repo_name=repo_name) or {}
        maintenance = dict(state.get("maintenance") or {})
        maintenance["last_empty_dir_sweep_at"] = datetime.now(timezone.utc).isoformat()
        update_workspace_state(
            workspace_path=workspace_path,
            repo_name=repo_name,
            updates={"maintenance": maintenance},
        )
    except Exception:
        pass


def _load_cached_hashes(
    workspace_path: str,
    repo_name: Optional[str],
    *,
    metadata_root: Optional[Path] = None,
) -> Dict[str, str]:
    workspace_norm = _normalize_cache_key_path(workspace_path)
    workspace_prefix = f"{workspace_norm.rstrip('/')}/"
    candidates: list[Path] = []
    seen: set[str] = set()

    def _append_candidate(path: Path) -> None:
        key = str(path)
        if key in seen:
            return
        seen.add(key)
        candidates.append(path)

    root = Path(metadata_root or workspace_path)
    if repo_name:
        _append_candidate(root / ".codebase" / "repos" / repo_name / "cache.json")
    else:
        _append_candidate(root / ".codebase" / "cache.json")

    for cache_path in candidates:
        if not cache_path.exists():
            continue
        try:
            with cache_path.open("r", encoding="utf-8-sig") as f:
                data = json.load(f)
            hashes = data.get("file_hashes", {})
            if not isinstance(hashes, dict):
                return {}
            normalized: Dict[str, str] = {}
            for path_key, value in hashes.items():
                norm = _normalize_cache_key_path(str(path_key))
                if not norm:
                    continue
                if workspace_norm and not (
                    norm == workspace_norm or norm.startswith(workspace_prefix)
                ):
                    continue
                if isinstance(value, dict):
                    digest = str(value.get("hash") or "").strip()
                else:
                    digest = str(value or "").strip()
                normalized[norm] = digest
            return normalized
        except Exception:
            return {}
    return {}


def _is_index_eligible_path(path_str: str, workspace_root: Path, excluder) -> bool:
    try:
        p = Path(path_str).resolve()
    except Exception:
        p = Path(path_str)
    try:
        rel = p.resolve().relative_to(workspace_root.resolve())
    except Exception:
        return False

    if not rel.parts:
        return False
    if not p.exists() or p.is_dir():
        return False
    try:
        if int(p.stat().st_size) == 0:
            # Empty files (e.g. many __init__.py stubs) produce no vectors; do not
            # enqueue consistency upserts for them.
            return False
    except Exception:
        return False
    if is_internal_metadata_path(p):
        return False

    # .remote-git manifests are control files and must not be treated as indexable.
    if _is_remote_git_manifest(p.as_posix()):
        return False

    try:
        rel_dir = "/" + str(rel.parent).replace(os.sep, "/")
        if rel_dir == "/.":
            rel_dir = "/"
        if excluder.exclude_dir(rel_dir):
            return False
    except Exception:
        return False

    if not idx.is_indexable_file(p):
        return False

    try:
        relf = (rel_dir.rstrip("/") + "/" + p.name).replace("//", "/")
        if excluder.exclude_file(relf):
            return False
    except Exception:
        return False
    return True


def _scan_indexable_fs_paths(workspace_root: Path, *, max_paths: int) -> Tuple[Set[str], bool]:
    paths: Set[str] = set()
    excluder = idx._Excluder(workspace_root)
    try:
        workspace_root = workspace_root.resolve()
    except Exception:
        pass

    for root_str, dirnames, filenames in os.walk(workspace_root):
        current = Path(root_str)
        pruned_dirnames = []
        for dirname in dirnames:
            child = current / dirname
            if is_internal_metadata_path(child):
                continue
            pruned_dirnames.append(dirname)
        dirnames[:] = pruned_dirnames

        for filename in filenames:
            file_path = current / filename
            normalized = _normalize_cache_key_path(str(file_path))
            if not normalized:
                continue
            if not _is_index_eligible_path(normalized, workspace_root, excluder):
                continue
            paths.add(normalized)
            if max_paths > 0 and len(paths) >= max_paths:
                return paths, True
    return paths, False


def _load_indexed_paths_for_collection(
    client: QdrantClient,
    collection: str,
    workspace_path: str,
    *,
    max_paths: int,
) -> Tuple[Set[str], bool]:
    paths: Set[str] = set()
    workspace_norm = _normalize_cache_key_path(workspace_path)
    workspace_prefix = f"{workspace_norm.rstrip('/')}/"
    offset = None
    while True:
        points, next_offset = client.scroll(
            collection_name=collection,
            limit=1000,
            with_payload=True,
            with_vectors=False,
            offset=offset,
        )
        for pt in points or []:
            payload = getattr(pt, "payload", {}) or {}
            metadata = payload.get("metadata", {}) or {}
            path = _normalize_cache_key_path(str(metadata.get("path") or ""))
            if path:
                if workspace_norm and not (
                    path == workspace_norm or path.startswith(workspace_prefix)
                ):
                    continue
                paths.add(path)
                if max_paths > 0 and len(paths) >= max_paths:
                    return paths, True
        if next_offset is None:
            break
        offset = next_offset
    return paths, False


def _record_consistency_audit(
    workspace_path: str,
    repo_name: Optional[str],
    summary: Dict[str, Any],
) -> None:
    try:
        state = get_workspace_state(workspace_path=workspace_path, repo_name=repo_name) or {}
        maintenance = dict(state.get("maintenance") or {})
        maintenance["last_consistency_audit_at"] = datetime.now(timezone.utc).isoformat()
        maintenance["last_consistency_audit_summary"] = summary
        update_workspace_state(
            workspace_path=workspace_path,
            repo_name=repo_name,
            updates={"maintenance": maintenance},
        )
    except Exception:
        pass


def _is_remote_git_manifest(path: str) -> bool:
    """Check if path is a .remote-git git history manifest file (control file, not indexable content)."""
    try:
        p = Path(path)
        return any(part == ".remote-git" for part in p.parts) and p.suffix.lower() == ".json"
    except Exception:
        return False


def _enqueue_consistency_repairs(
    workspace_root: Path,
    workspace_path: str,
    repo_name: Optional[str],
    stale_paths: list[str],
    missing_paths: list[str],
    cached_hashes: Dict[str, str],
) -> Tuple[int, int]:
    if not _consistency_repair_enabled():
        return 0, 0
    max_ops = _consistency_repair_max_ops()
    if max_ops <= 0:
        return 0, 0

    entries: list[Dict[str, Any]] = []
    enqueued_stale = 0
    enqueued_missing = 0
    missing_set = set(missing_paths)
    excluder = idx._Excluder(workspace_root)

    for path in stale_paths:
        if len(entries) >= max_ops:
            break
        # Skip .remote-git git history manifests - they are control files, not indexable content
        if _is_remote_git_manifest(path):
            continue
        # Cache can lag after state resets/rebuilds; if the path still exists and is
        # index-eligible, treat it as missing/upsert instead of stale/delete.
        if _is_index_eligible_path(path, workspace_root, excluder):
            missing_set.add(path)
            continue
        entries.append({"path": path, "op_type": "delete"})
        enqueued_stale += 1
    for path in sorted(missing_set):
        if len(entries) >= max_ops:
            break
        # Skip .remote-git git history manifests - they are control files, not indexable content
        if _is_remote_git_manifest(path):
            continue
        entries.append(
            {
                "path": path,
                "op_type": "upsert",
                "content_hash": cached_hashes.get(path) or None,
            }
        )
        enqueued_missing += 1

    if not entries:
        return 0, 0
    try:
        upsert_index_journal_entries(
            entries,
            workspace_path=workspace_path,
            repo_name=repo_name,
        )
    except Exception as exc:
        logger.debug(
            "[consistency_audit] failed to enqueue repairs workspace=%s repo=%s: %s",
            workspace_path,
            repo_name,
            exc,
        )
        return 0, 0
    return enqueued_stale, enqueued_missing


def run_consistency_audit(client: QdrantClient, root: Path) -> None:
    if not _consistency_audit_enabled():
        return
    max_paths = _consistency_audit_max_paths()
    try:
        candidates = list_workspaces(search_root=str(root), use_qdrant_fallback=False)
    except Exception:
        candidates = []
    for ws in candidates:
        workspace_path = str(ws.get("workspace_path") or "").strip()
        if not workspace_path:
            continue
        repo_name = _extract_repo_name_from_path(workspace_path)
        if not _should_run_consistency_audit(workspace_path, repo_name):
            continue
        try:
            snapshot = get_collection_state_snapshot(
                workspace_path=workspace_path,
                repo_name=repo_name,
            )
            collection = str(snapshot.get("active_collection") or "").strip()
            if not collection:
                continue
            cached_hashes = _load_cached_hashes(
                workspace_path,
                repo_name,
                metadata_root=root,
            )
            workspace_root = Path(workspace_path)
            fs_paths, fs_truncated = _scan_indexable_fs_paths(
                workspace_root,
                max_paths=max_paths,
            )
            excluder = idx._Excluder(workspace_root)
            cached_paths = {
                path
                for path in cached_hashes.keys()
                if _is_index_eligible_path(path, workspace_root, excluder)
            }
            indexed_paths, indexed_truncated = _load_indexed_paths_for_collection(
                client,
                collection,
                workspace_path,
                max_paths=max_paths,
            )
            if fs_truncated or indexed_truncated:
                stale = []
                missing = []
                enq_stale = 0
                enq_missing = 0
            else:
                stale = sorted(indexed_paths - fs_paths)
                missing = sorted(fs_paths - indexed_paths)
                enq_stale, enq_missing = _enqueue_consistency_repairs(
                    workspace_root,
                    workspace_path,
                    repo_name,
                    stale,
                    missing,
                    cached_hashes,
                )
            summary = {
                "fs_count": len(fs_paths),
                "cache_count": len(cached_paths),
                "qdrant_count": len(indexed_paths),
                "fs_scan_truncated": fs_truncated,
                "qdrant_scan_truncated": indexed_truncated,
                "repair_skipped_due_to_truncation": bool(fs_truncated or indexed_truncated),
                "stale_in_qdrant_count": len(stale),
                "missing_in_qdrant_count": len(missing),
                "repair_enqueued_stale_count": int(enq_stale),
                "repair_enqueued_missing_count": int(enq_missing),
                "sample_stale": stale[:20],
                "sample_missing": missing[:20],
            }
            _record_consistency_audit(workspace_path, repo_name, summary)
            logger.info(
                "[consistency_audit] repo=%s collection=%s fs=%d cache=%d qdrant=%d stale=%d missing=%d repair_stale=%d repair_missing=%d",
                repo_name or "<none>",
                collection,
                len(fs_paths),
                len(cached_paths),
                len(indexed_paths),
                len(stale),
                len(missing),
                int(enq_stale),
                int(enq_missing),
            )
        except Exception as exc:
            logger.debug(
                "[consistency_audit] failed workspace=%s repo=%s: %s",
                workspace_path,
                repo_name,
                exc,
            )


def run_empty_dir_sweep_maintenance(root: Path) -> None:
    if not _empty_dir_sweep_enabled():
        return
    try:
        candidates = list_workspaces(search_root=str(root), use_qdrant_fallback=False)
    except Exception:
        candidates = []
    for ws in candidates:
        workspace_path = str(ws.get("workspace_path") or "").strip()
        if not workspace_path:
            continue
        repo_name = _extract_repo_name_from_path(workspace_path)
        if not _should_run_empty_dir_sweep(workspace_path, repo_name):
            continue
        try:
            logger.info("[empty_dir_sweep] Sweeping empty directories under %s", workspace_path)
            _sweep_empty_workspace_dirs(Path(workspace_path))
            _record_empty_dir_sweep(workspace_path, repo_name)
        except Exception as exc:
            logger.debug(
                "[empty_dir_sweep] failed workspace=%s repo=%s: %s",
                workspace_path,
                repo_name,
                exc,
            )
