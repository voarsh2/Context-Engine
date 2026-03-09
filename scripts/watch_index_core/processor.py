"""Batch processing and ingest orchestration for the watcher."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import atexit
import threading
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from qdrant_client import models

import scripts.ingest_code as idx
from scripts.workspace_state import (
    _normalize_cache_key_path,
    _extract_repo_name_from_path,
    get_cached_file_hash,
    list_pending_index_journal_entries,
    get_workspace_state,
    is_staging_enabled,
    log_watcher_activity as _log_activity,
    persist_indexing_config,
    remove_cached_file,
    set_cached_file_hash,
    set_indexing_progress as _update_progress,
    set_indexing_started as _set_status_indexing,
    update_index_journal_entry_status,
    update_indexing_status,
)
from . import config as watch_config
from .rename import _rename_in_store
from .paths import is_internal_top_level_path

from .utils import (
    _detect_repo_for_file, 
    _get_collection_for_file,
    get_boolean_env,
    safe_print,
    safe_log_error,
)

logger = watch_config.LOGGER


class _SkipUnchanged(Exception):
    """Sentinel exception to skip unchanged files in the watch loop."""

    def __init__(self, *, text: Optional[str] = None, file_hash: str = "") -> None:
        super().__init__("unchanged")
        self.text = text
        self.file_hash = file_hash


def _is_internal_ignored_path(path: Path) -> bool:
    return is_internal_top_level_path(path, watch_config.ROOT)


def _staging_requires_subprocess(state: Optional[Dict[str, object]]) -> bool:
    """Return True only when dual-root staging is actually active for this repo."""
    if not (is_staging_enabled() and isinstance(state, dict)):
        return False

    staging = state.get("staging")
    if isinstance(staging, dict) and staging:
        return True

    active_slug = str(state.get("active_repo_slug") or "").strip()
    serving_slug = str(state.get("serving_repo_slug") or "").strip()
    if serving_slug.endswith("_old"):
        return True
    if active_slug and serving_slug and active_slug != serving_slug:
        return True
    return False


def _env_int(name: str, default: int) -> int:
    try:
        raw = str(os.environ.get(name, str(default))).strip()
        val = int(raw)
        return val if val > 0 else default
    except Exception:
        return default


_GIT_HISTORY_MAX_WORKERS = _env_int("WATCH_GIT_HISTORY_MAX_WORKERS", 1)
_GIT_HISTORY_TIMEOUT_SECONDS = _env_int("WATCH_GIT_HISTORY_TIMEOUT_SECONDS", 0)
_GIT_HISTORY_EXECUTOR = ThreadPoolExecutor(
    max_workers=_GIT_HISTORY_MAX_WORKERS,
    thread_name_prefix="git-history",
)


def _shutdown_git_history_executor() -> None:
    try:
        _GIT_HISTORY_EXECUTOR.shutdown(wait=False)
    except Exception:
        pass


atexit.register(_shutdown_git_history_executor)
_GIT_HISTORY_INFLIGHT: set[str] = set()
_GIT_HISTORY_INFLIGHT_LOCK = threading.Lock()


def _manifest_key(p: Path) -> str:
    try:
        return str(p.resolve())
    except Exception:
        return str(p)


def _manifest_stats(p: Path) -> tuple[str, int]:
    run_id = "unknown"
    commit_count = -1
    try:
        with p.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            commits = data.get("commits") or []
            if isinstance(commits, list):
                commit_count = len(commits)
            name = p.name
            run_id = name[:-5] if name.endswith(".json") else name
    except Exception:
        pass
    return run_id, commit_count


def _run_git_history_ingest(
    p: Path,
    collection: str,
    repo_name: Optional[str],
    env_snapshot: Optional[Dict[str, str]] = None,
) -> None:
    script = watch_config.ROOT_DIR / "scripts" / "ingest_history.py"
    if not script.exists():
        raise RuntimeError(f"[git_history_manifest] ingest script missing: {script}")

    cmd = [sys.executable or "python3", str(script), "--manifest-json", str(p)]
    env = _build_subprocess_env(collection, repo_name, env_snapshot)
    started = time.monotonic()
    timeout = _GIT_HISTORY_TIMEOUT_SECONDS if _GIT_HISTORY_TIMEOUT_SECONDS > 0 else None
    stdout_tail: deque[str] = deque(maxlen=20)
    stderr_tail: deque[str] = deque(maxlen=20)
    tail_lock = threading.Lock()

    def _tail_snapshot(tail: deque[str], limit: int = 5) -> str:
        with tail_lock:
            return " | ".join(list(tail)[-limit:])

    def _stream_pipe(pipe, label: str, tail: deque[str], lock: threading.Lock) -> None:
        try:
            for raw in iter(pipe.readline, ""):
                line = (raw or "").rstrip()
                if not line:
                    continue
                with lock:
                    tail.append(line)
                logger.info("[git_history_manifest][%s] %s", label, line)
        except Exception:
            pass
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    proc: Optional[subprocess.Popen] = None
    try:
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        t_out = threading.Thread(
            target=_stream_pipe,
            args=(proc.stdout, "stdout", stdout_tail, tail_lock),
            daemon=True,
        )
        t_err = threading.Thread(
            target=_stream_pipe,
            args=(proc.stderr, "stderr", stderr_tail, tail_lock),
            daemon=True,
        )
        t_out.start()
        t_err.start()

        deadline = (started + timeout) if timeout else None
        timed_out = False
        while True:
            code = proc.poll()
            if code is not None:
                break
            if deadline and time.monotonic() >= deadline:
                timed_out = True
                try:
                    proc.kill()
                except Exception:
                    pass
                break
            time.sleep(0.2)

        # Ensure threads flush trailing output after process exit/kill.
        t_out.join(timeout=1.0)
        t_err.join(timeout=1.0)

        if timed_out:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            error_msg = (
                f"[git_history_manifest] ingest_history.py timeout for {p} after {elapsed_ms}ms "
                f"(timeout={_GIT_HISTORY_TIMEOUT_SECONDS}s)"
            )
            if stderr_tail:
                error_msg += f" stderr={_tail_snapshot(stderr_tail)}"
            logger.warning(error_msg)
            raise RuntimeError(error_msg)

        returncode = proc.wait(timeout=1.0)
    except Exception as e:
        logger.warning("[git_history_manifest] subprocess error for %s: %s", p, e)
        try:
            if proc and proc.poll() is None:
                proc.kill()
        except Exception:
            pass
        raise RuntimeError(f"[git_history_manifest] subprocess error for {p}: {e}") from e

    elapsed_ms = int((time.monotonic() - started) * 1000)
    if returncode != 0:
        error_msg = (
            f"[git_history_manifest] ingest_history.py failed for {p}: exit={returncode} "
            f"elapsed_ms={elapsed_ms} stderr={_tail_snapshot(stderr_tail)}"
        )
        logger.warning(error_msg)
        raise RuntimeError(error_msg)

    logger.info(
        "[git_history_manifest] completed for %s: exit=0 elapsed_ms=%d",
        p,
        elapsed_ms,
    )
    if stdout_tail:
        logger.info(
            "[git_history_manifest] stdout tail for %s: %s",
            p,
            _tail_snapshot(stdout_tail),
        )
    if stderr_tail:
        logger.warning(
            "[git_history_manifest] stderr tail for %s: %s",
            p,
            _tail_snapshot(stderr_tail),
        )


def _on_git_history_done(manifest_path: Path, collection: str, repo_name: Optional[str], future: Future) -> None:
    manifest_key = _manifest_key(manifest_path)
    with _GIT_HISTORY_INFLIGHT_LOCK:
        _GIT_HISTORY_INFLIGHT.discard(manifest_key)
        remaining = len(_GIT_HISTORY_INFLIGHT)
    logger.info("[git_history_manifest] in-flight remaining=%d", remaining)
    try:
        future.result()
        # Mark journal as done after successful completion
        repo_path = _detect_repo_for_file(manifest_path)
        if repo_path:
            repo_key = str(repo_path)
            _mark_journal_done(manifest_path, repo_key, repo_name)
            logger.info("[git_history_manifest] marked journal as done: %s", manifest_path)
    except Exception as e:
        repo_path = _detect_repo_for_file(manifest_path)
        repo_key = str(repo_path) if repo_path else ""
        if repo_key:
            _mark_journal_failed(
                manifest_path,
                repo_key,
                repo_name,
                f"git history worker failed for collection '{collection}': {e}",
            )
        logger.warning(
            "[git_history_manifest] worker crashed for %s (collection=%s, repo_key=%s): %s",
            manifest_key,
            collection,
            repo_key or "<unknown>",
            e,
            exc_info=True,
        )


def _process_git_history_manifest(
    p: Path,
    collection: str,
    repo_name: Optional[str],
    env_snapshot: Optional[Dict[str, str]] = None,
) -> None:
    key = _manifest_key(p)
    run_id, commit_count = _manifest_stats(p)
    queued = 0
    with _GIT_HISTORY_INFLIGHT_LOCK:
        if key in _GIT_HISTORY_INFLIGHT:
            logger.info(
                "[git_history_manifest] skip duplicate in-flight manifest: %s run_id=%s",
                p,
                run_id,
            )
            return
        _GIT_HISTORY_INFLIGHT.add(key)
        queued = len(_GIT_HISTORY_INFLIGHT)
    logger.info(
        "[git_history_manifest] queued ingest_history.py for %s run_id=%s commits=%d collection=%s repo=%s in_flight=%d",
        p,
        run_id,
        commit_count,
        collection,
        repo_name,
        queued,
    )
    future = _GIT_HISTORY_EXECUTOR.submit(
        _run_git_history_ingest,
        p,
        collection,
        repo_name,
        env_snapshot,
    )
    future.add_done_callback(lambda fut, manifest_path=p, coll=collection, rn=repo_name: _on_git_history_done(manifest_path, coll, rn, fut))


def _advance_progress(
    repo_progress: Dict[str, int],
    repo_key: str,
    repo_files: List[Path],
    started_at: str,
    current_file: Path,
) -> None:
    repo_progress[repo_key] = repo_progress.get(repo_key, 0) + 1
    try:
        _update_progress(
            repo_key,
            started_at,
            repo_progress[repo_key],
            len(repo_files),
            current_file,
        )
    except Exception:
        pass


def _mark_journal_done(path: Path, repo_key: str, repo_name: Optional[str]) -> None:
    try:
        update_index_journal_entry_status(
            str(path),
            status="done",
            workspace_path=repo_key,
            repo_name=repo_name,
        )
    except Exception:
        pass


def _mark_journal_failed(
    path: Path,
    repo_key: str,
    repo_name: Optional[str],
    error: str,
) -> None:
    try:
        update_index_journal_entry_status(
            str(path),
            status="failed",
            error=error,
            workspace_path=repo_key,
            repo_name=repo_name,
            remove_on_done=False,
        )
    except Exception:
        pass


def _path_has_indexed_points(client, collection: str, path: Path) -> Optional[bool]:
    try:
        filt = models.Filter(
            must=[
                models.FieldCondition(
                    key="metadata.path", match=models.MatchValue(value=str(path))
                )
            ]
        )
        points, _ = client.scroll(
            collection_name=collection,
            scroll_filter=filt,
            with_payload=False,
            with_vectors=False,
            limit=1,
        )
        return bool(points)
    except Exception:
        return None


def _verify_delete_committed(client, collection: str, path: Path) -> bool:
    has_points = _path_has_indexed_points(client, collection, path)
    return has_points is False


def _verify_upsert_committed(
    client,
    collection: str,
    path: Path,
    repo_name: Optional[str],
    expected_file_hash: Optional[str],
    source_text: Optional[str] = None,
) -> bool:
    indexed_hash = str(
        idx.get_indexed_file_hash(client, collection, str(path)) or ""
    ).strip()
    expected_hash = str(expected_file_hash or "").strip()
    if expected_hash:
        if bool(indexed_hash) and indexed_hash == expected_hash:
            return True
        # Empty/whitespace-only files can legitimately have no indexed points/hash.
        try:
            if source_text is not None and not source_text.strip():
                has_points = _path_has_indexed_points(client, collection, path)
                return has_points is False
        except Exception:
            pass
        return False
    has_points = _path_has_indexed_points(client, collection, path)
    return has_points is True


def _verify_and_update_journal_for_upsert(
    p: Path,
    client,
    collection: str,
    repo_key: str,
    repo_name: Optional[str],
    journal_content_hash: str,
    *,
    text: Optional[str] = None,
    file_hash: Optional[str] = None,
) -> None:
    source_text = text
    expected_hash = str(file_hash or "").strip()
    if source_text is None or not expected_hash:
        read_text, read_hash = _read_text_and_sha1(p)
        if source_text is None:
            source_text = read_text
        if not expected_hash:
            expected_hash = read_hash
    expected_hash = expected_hash or journal_content_hash
    if _verify_upsert_committed(
        client,
        collection,
        p,
        repo_name,
        expected_hash or None,
        source_text=source_text,
    ):
        _mark_journal_done(p, repo_key, repo_name)
    else:
        _mark_journal_failed(
            p,
            repo_key,
            repo_name,
            "upsert_verification_failed",
        )


def _finalize_journal_after_index_attempt(
    path: Path,
    client,
    collection: str | None,
    repo_key: str,
    repo_name: Optional[str],
    *,
    force_upsert: bool,
    journal_content_hash: str,
    text: Optional[str] = None,
    file_hash: Optional[str] = None,
    default_error: Optional[str] = None,
) -> None:
    if force_upsert and client is not None and collection is not None:
        _verify_and_update_journal_for_upsert(
            path,
            client,
            collection,
            repo_key,
            repo_name,
            journal_content_hash,
            text=text,
            file_hash=file_hash,
        )
    elif default_error:
        _mark_journal_failed(path, repo_key, repo_name, default_error)
    else:
        _mark_journal_done(path, repo_key, repo_name)


def _build_subprocess_env(
    collection: str | None,
    repo_name: str | None,
    env_snapshot: Optional[Dict[str, str]],
) -> Dict[str, str]:
    env = os.environ.copy()
    try:
        if env_snapshot:
            env.update({str(k): str(v) for k, v in env_snapshot.items() if k})
    except Exception:
        pass
    if collection:
        env["COLLECTION_NAME"] = collection
    if watch_config.QDRANT_URL:
        env["QDRANT_URL"] = watch_config.QDRANT_URL
    if repo_name:
        env["REPO_NAME"] = repo_name
    return env


def _maybe_handle_staging_file(
    path: Path,
    client,
    collection: str | None,
    repo_name: str | None,
    repo_key: str,
    repo_files: List[Path],
    state_env: Optional[Dict[str, str]],
    repo_progress: Dict[str, int],
    started_at: str,
    *,
    force_upsert: bool = False,
    journal_content_hash: str = "",
) -> bool:
    if not (state_env and collection):
        return False

    source_text, file_hash = _read_text_and_sha1(path)
    if file_hash:
        try:
            cached_hash = get_cached_file_hash(str(path), repo_name) if repo_name else None
        except Exception:
            cached_hash = None
        if cached_hash and cached_hash == file_hash:
            if force_upsert and client is not None:
                if _verify_upsert_committed(
                    client,
                    collection,
                    path,
                    repo_name,
                    file_hash or journal_content_hash or None,
                    source_text=source_text,
                ):
                    safe_print(f"[skip_unchanged] {path} (hash match)")
                    _log_activity(repo_key, "skipped", path, {"reason": "hash_unchanged"})
                    _mark_journal_done(path, repo_key, repo_name)
                    _advance_progress(repo_progress, repo_key, repo_files, started_at, path)
                    return True
            # Fast path: skip if content hash matches cached hash (file unchanged)
            # Safety: startup health check clears stale cache per-repo
            if not force_upsert:
                safe_print(f"[skip_unchanged] {path} (hash match)")
                _log_activity(repo_key, "skipped", path, {"reason": "hash_unchanged"})
                _advance_progress(repo_progress, repo_key, repo_files, started_at, path)
                return True

    cmd = [
        sys.executable or "python3",
        str(watch_config.ROOT_DIR / "scripts" / "ingest_code.py"),
        "--root",
        str(path),
        "--no-skip-unchanged",
    ]
    env = _build_subprocess_env(collection, repo_name, state_env)
    try:
        # If a repo-specific indexing_env is present (staging), avoid mutating os.environ
        # process-wide. Instead, run ingest_code in a subprocess with an explicit env dict.
        # Cheap pre-flight hash check so we can skip unchanged files without spawning a subprocess.
        result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    except Exception:
        return False
    if result.returncode != 0:
        # TODO: Instead of launching one subprocess per file, queue changes and run a 
        # single ingest_code.py --root <repo> pass with --no-skip-unchanged. That 
        # reuses ingest’s own skip logic, but requires more plumbing (collect paths, 
        # pass via manifest/CLI, etc.).
        try:
            logger.error(
                "watch_index::subprocess_index_failed",
                extra={
                    "repo_key": repo_key,
                    "collection": collection,
                    "file": str(path),
                    "returncode": result.returncode,
                    "stdout": (result.stdout or "").strip(),
                    "stderr": (result.stderr or "").strip(),
                },
            )
        except Exception:
            safe_print(
                f"[indexed_subprocess_error] {path} -> {collection} "
                f"returncode={result.returncode}"
            )
    else:
        safe_print(f"[indexed_subprocess] {path} -> {collection}")
        _finalize_journal_after_index_attempt(
            path,
            client,
            collection,
            repo_key,
            repo_name,
            force_upsert=force_upsert,
            journal_content_hash=journal_content_hash,
            text=source_text,
            file_hash=file_hash,
        )
    if result.returncode != 0 and force_upsert:
        _mark_journal_failed(path, repo_key, repo_name, "subprocess_index_failed")
    _advance_progress(repo_progress, repo_key, repo_files, started_at, path)
    return True


def _process_paths(
    paths,
    client,
    model,
    vector_name: str,
    model_dim: int,
    workspace_path: str,
) -> None:
    unique_paths = sorted(set(Path(x) for x in paths))
    if not unique_paths:
        return

    started_at = datetime.now().isoformat()

    repo_groups: Dict[str, List[Path]] = {}
    for p in unique_paths:
        repo_path = _detect_repo_for_file(p) or Path(workspace_path)
        repo_groups.setdefault(str(repo_path), []).append(p)

    for repo_path, repo_files in repo_groups.items():
        try:
            repo_name = _extract_repo_name_from_path(repo_path)
            try:
                if persist_indexing_config:
                    persist_indexing_config(
                        workspace_path=repo_path,
                        repo_name=repo_name,
                        pending=True,
                    )
            except Exception:
                pass
            _set_status_indexing(str(repo_path), len(repo_files))
        except Exception:
            pass

    repo_progress: Dict[str, int] = {key: 0 for key in repo_groups.keys()}
    repo_pending_journal_ops: Dict[str, Dict[str, Dict[str, str]]] = {}
    repo_move_source_for_dest: Dict[str, Dict[str, str]] = {}
    move_dest_keys: set[str] = set()
    move_source_keys: set[str] = set()
    for repo_path in repo_groups.keys():
        try:
            repo_name = _extract_repo_name_from_path(repo_path)
            entries = list_pending_index_journal_entries(repo_path, repo_name)
            repo_pending_journal_ops[repo_path] = {}
            upserts_by_hash: Dict[str, List[str]] = {}
            deletes_by_hash: Dict[str, List[str]] = {}
            for rec in entries:
                path_key = _normalize_cache_key_path(str(rec.get("path") or ""))
                op_type = str(rec.get("op_type") or "").strip().lower()
                content_hash = str(rec.get("content_hash") or "").strip().lower()
                if not path_key:
                    continue
                repo_pending_journal_ops[repo_path][path_key] = {
                    "op_type": op_type,
                    "content_hash": content_hash,
                }
                if not content_hash:
                    continue
                if op_type == "upsert":
                    upserts_by_hash.setdefault(content_hash, []).append(path_key)
                elif op_type == "delete":
                    deletes_by_hash.setdefault(content_hash, []).append(path_key)
            pairs: Dict[str, str] = {}
            for content_hash, dest_paths in upserts_by_hash.items():
                src_paths = deletes_by_hash.get(content_hash) or []
                if not src_paths:
                    continue
                src_idx = 0
                for dest_key in dest_paths:
                    while src_idx < len(src_paths) and src_paths[src_idx] == dest_key:
                        src_idx += 1
                    if src_idx >= len(src_paths):
                        break
                    src_key = src_paths[src_idx]
                    src_idx += 1
                    pairs[dest_key] = src_key
                    move_dest_keys.add(dest_key)
                    move_source_keys.add(src_key)
            repo_move_source_for_dest[repo_path] = pairs
        except Exception:
            repo_pending_journal_ops[repo_path] = {}
            repo_move_source_for_dest[repo_path] = {}

    unique_paths = sorted(
        unique_paths,
        key=lambda p: (
            0
            if _normalize_cache_key_path(str(p)) in move_dest_keys
            else (2 if _normalize_cache_key_path(str(p)) in move_source_keys else 1),
            str(p),
        ),
    )
    completed_move_sources: set[str] = set()

    for p in unique_paths:
        repo_path = _detect_repo_for_file(p) or Path(workspace_path)
        repo_key = str(repo_path)
        repo_files = repo_groups.get(repo_key, [])
        repo_name = _extract_repo_name_from_path(repo_key)
        path_key = _normalize_cache_key_path(str(p))
        if path_key in completed_move_sources:
            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
            continue
        journal_rec = repo_pending_journal_ops.get(repo_key, {}).get(path_key, {})
        journal_op = str(journal_rec.get("op_type") or "").strip().lower()
        force_delete = journal_op == "delete"
        force_upsert = journal_op == "upsert"
        journal_content_hash = str(journal_rec.get("content_hash") or "").strip().lower()
        if _is_internal_ignored_path(p):
            _log_activity(repo_key, "skipped", p, {"reason": "internal_ignored_path"})
            # Internal metadata paths should never drive indexing or collection creation.
            # If they entered the journal via drift repair, mark done and drop.
            _mark_journal_done(p, repo_key, repo_name)
            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
            continue
        collection = _get_collection_for_file(p)
        state_env: Optional[Dict[str, str]] = None
        try:
            st = get_workspace_state(repo_key, repo_name) if get_workspace_state else None
            if isinstance(st, dict):
                if _staging_requires_subprocess(st):
                    state_env = st.get("indexing_env")
        except Exception:
            state_env = None

        if ".remote-git" in p.parts and p.suffix.lower() == ".json":
            try:
                _process_git_history_manifest(
                    p,
                    collection,
                    repo_name,
                    env_snapshot=state_env,
                )
            except Exception as exc:
                safe_print(f"[commit_ingest_error] {p}: {exc}")
            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
            continue

        if force_upsert and not p.exists():
            _log_activity(repo_key, "skipped", p, {"reason": "upsert_missing_file"})
            _mark_journal_failed(
                p,
                repo_key,
                repo_name,
                "upsert_missing_file",
            )
            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
            continue

        if force_upsert and client is not None and collection is not None:
            move_src_key = repo_move_source_for_dest.get(repo_key, {}).get(path_key)
            if move_src_key:
                move_src_path = Path(move_src_key)
                src_collection = _get_collection_for_file(move_src_path)
                try:
                    moved_count, renamed_hash = _rename_in_store(
                        client,
                        src_collection,
                        move_src_path,
                        p,
                        collection,
                    )
                except Exception:
                    moved_count, renamed_hash = -1, None
                if moved_count and moved_count > 0:
                    try:
                        if repo_name:
                            remove_cached_file(str(move_src_path), repo_name)
                    except Exception:
                        pass
                    final_hash = renamed_hash or journal_content_hash
                    try:
                        if repo_name and final_hash:
                            set_cached_file_hash(str(p), final_hash, repo_name)
                    except Exception:
                        pass
                    _log_activity(
                        repo_key,
                        "moved",
                        p,
                        {"from": str(move_src_path), "chunks": int(moved_count)},
                    )
                    _mark_journal_done(p, repo_key, repo_name)
                    _mark_journal_done(move_src_path, repo_key, repo_name)
                    completed_move_sources.add(move_src_key)
                    _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
                    continue

        if force_delete or not p.exists():
            deleted_ok = False
            if client is not None:
                try:
                    idx.delete_points_by_path(client, collection, str(p))
                    try:
                        idx.delete_graph_edges_by_path(
                            client,
                            collection,
                            caller_path=str(p),
                            repo=repo_name,
                        )
                    except Exception as graph_exc:
                        safe_print(f"[deleted:graph_failed] {p} -> {collection}: {graph_exc}")
                        # Don't mark as deleted_ok if graph cleanup fails
                        deleted_ok = False
                        raise
                    safe_print(f"[deleted] {p} -> {collection}")
                    deleted_ok = True
                except Exception:
                    deleted_ok = False
            if deleted_ok and client is not None and collection is not None:
                deleted_ok = _verify_delete_committed(client, collection, p)
            try:
                if repo_name:
                    remove_cached_file(str(p), repo_name)
            except Exception:
                pass
            _log_activity(repo_key, "deleted", p)
            if deleted_ok:
                _mark_journal_done(p, repo_key, repo_name)
            else:
                _mark_journal_failed(
                    p,
                    repo_key,
                    repo_name,
                    "delete_points_failed",
                )
            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
            continue

        if _maybe_handle_staging_file(
            p,
            client,
            collection,
            repo_name,
            repo_key,
            repo_files,
            state_env,
            repo_progress,
            started_at,
            force_upsert=force_upsert,
            journal_content_hash=journal_content_hash,
        ):
            continue
        if client is not None and model is not None:
            try:
                verify_context: Dict[str, Optional[str]] = {}
                ok = _run_indexing_strategy(
                    p,
                    client,
                    model,
                    collection,
                    vector_name,
                    model_dim,
                    repo_name,
                    verify_context=verify_context if force_upsert else None,
                )
            except _SkipUnchanged as exc:
                status = "skipped"
                safe_print(f"[{status}] {p} -> {collection}")
                _log_activity(repo_key, "skipped", p, {"reason": "hash_unchanged"})
                _finalize_journal_after_index_attempt(
                    p,
                    client,
                    collection,
                    repo_key,
                    repo_name,
                    force_upsert=force_upsert,
                    journal_content_hash=journal_content_hash,
                    text=exc.text,
                    file_hash=exc.file_hash,
                )
                _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
                continue
            except Exception:
                safe_log_error(
                    logger,
                    "watch_index::_process_paths error",
                    extra={
                        "repo_key": repo_key,
                        "collection": collection,
                        "file": str(p),
                    },
                )
                _mark_journal_failed(p, repo_key, repo_name, "indexing_error")
                _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
                continue

            status = "indexed" if ok else "skipped"
            safe_print(f"[{status}] {p} -> {collection}")
            if ok:
                try:
                    size = int(p.stat().st_size)
                except Exception:
                    size = None
                _log_activity(repo_key, "indexed", p, {"file_size": size})
                _finalize_journal_after_index_attempt(
                    p,
                    client,
                    collection,
                    repo_key,
                    repo_name,
                    force_upsert=force_upsert,
                    journal_content_hash=journal_content_hash,
                    text=verify_context.get("text"),
                    file_hash=verify_context.get("file_hash"),
                )
            else:
                _log_activity(
                    repo_key, "skipped", p, {"reason": "no-change-or-error"}
                )
                _finalize_journal_after_index_attempt(
                    p,
                    client,
                    collection,
                    repo_key,
                    repo_name,
                    force_upsert=force_upsert,
                    journal_content_hash=journal_content_hash,
                    text=verify_context.get("text"),
                    file_hash=verify_context.get("file_hash"),
                    default_error="no_change_or_error",
                )
            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
        else:
            safe_print(f"Not processing locally: {p}")
            _log_activity(repo_key, "skipped", p, {"reason": "remote-mode"})

            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)

    for repo_path in repo_groups.keys():
        try:
            repo_name = _extract_repo_name_from_path(repo_path)
            update_indexing_status(
                repo_name=repo_name,
                status={"state": "watching"},
            )
        except Exception:
            pass


def _read_text_and_sha1(path: Path) -> tuple[Optional[str], str]:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        text = None
    if text is None:
        return text, ""
    try:
        file_hash = hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()
    except Exception:
        file_hash = ""
    return text, file_hash


def _run_indexing_strategy(
    path: Path,
    client,
    model,
    collection: str | None,
    vector_name: str,
    model_dim: int,
    repo_name: str | None,
    *,
    verify_context: Optional[Dict[str, Optional[str]]] = None,
) -> bool:
    if collection is None:
        return False

    text, file_hash = _read_text_and_sha1(path)
    if verify_context is not None:
        verify_context["text"] = text
        verify_context["file_hash"] = file_hash
    ok = False
    if text is not None:
        try:
            language = idx.detect_language(path)
        except Exception:
            language = ""
        try:
            is_text_like = bool(idx.is_text_like_language(language))
        except Exception:
            is_text_like = False
        if file_hash:
            try:
                cached_hash = get_cached_file_hash(str(path), repo_name) if repo_name else None
            except Exception:
                cached_hash = None
            if cached_hash and cached_hash == file_hash:
                ok = True
                raise _SkipUnchanged(text=text, file_hash=file_hash)
            if not is_text_like:
                try:
                    use_smart, smart_reason = idx.should_use_smart_reindexing(str(path), file_hash)
                except Exception:
                    use_smart, smart_reason = False, "smart_check_failed"
                # Bootstrap: if we have no symbol cache yet, still run smart path once
                bootstrap = smart_reason == "no_cached_symbols"
                if use_smart or bootstrap:
                    msg_kind = (
                        "smart reindexing"
                        if use_smart
                        else "bootstrap (no_cached_symbols) for smart reindex"
                    )
                    safe_print(
                        f"[SMART_REINDEX][watcher] Using {msg_kind} for {path} ({smart_reason})"
                    )
                    try:
                        status = idx.process_file_with_smart_reindexing(
                            path,
                            text,
                            language,
                            client,
                            collection,
                            repo_name,
                            model,
                            vector_name,
                            model_dim=model_dim,
                        )
                        ok = status in ("success", "skipped")
                    except Exception as exc:
                        safe_print(
                            f"[SMART_REINDEX][watcher] Smart reindexing failed for {path}: {exc}"
                        )
                        ok = False
                else:
                    safe_print(
                        f"[SMART_REINDEX][watcher] Using full reindexing for {path} ({smart_reason})"
                    )
                    # Fallback: full single-file reindex. Pseudo/tags are inlined by default;
                    # when PSEUDO_DEFER_TO_WORKER=1 we run base-only and rely on backfill.
    if not ok:
        try:
            idx.ensure_collection_and_indexes_once(
                client, collection, model_dim, vector_name
            )
        except Exception:
            pass
        pseudo_mode = "off" if get_boolean_env("PSEUDO_DEFER_TO_WORKER") else "full"
        ok = idx.index_single_file(
            client,
            model,
            collection,
            vector_name,
            path,
            dedupe=True,
            skip_unchanged=False,
            pseudo_mode=pseudo_mode,
            repo_name_for_cache=repo_name,
            preloaded_text=text,
            preloaded_file_hash=file_hash,
            preloaded_language=language if text is not None else None,
        )
    return ok


__all__ = [
    "_SkipUnchanged",
    "_process_git_history_manifest",
    "_process_paths",
]
