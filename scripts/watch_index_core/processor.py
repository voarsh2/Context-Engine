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

import scripts.ingest_code as idx
from scripts.workspace_state import (
    _extract_repo_name_from_path,
    get_cached_file_hash,
    get_workspace_state,
    is_staging_enabled,
    log_watcher_activity as _log_activity,
    persist_indexing_config,
    remove_cached_file,
    set_indexing_progress as _update_progress,
    set_indexing_started as _set_status_indexing,
    update_indexing_status,
)

from .config import QDRANT_URL, ROOT, ROOT_DIR, LOGGER as logger
from .utils import (
    _detect_repo_for_file, 
    _get_collection_for_file,
    get_boolean_env,
    safe_print,
    safe_log_error,
)


class _SkipUnchanged(Exception):
    """Sentinel exception to skip unchanged files in the watch loop."""


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
    script = ROOT_DIR / "scripts" / "ingest_history.py"
    if not script.exists():
        logger.warning("[git_history_manifest] ingest script missing: %s", script)
        return

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
            logger.warning(
                "[git_history_manifest] ingest_history.py timeout for %s after %dms (timeout=%ss)",
                p,
                elapsed_ms,
                _GIT_HISTORY_TIMEOUT_SECONDS,
            )
            if stderr_tail:
                logger.warning(
                    "[git_history_manifest] timeout stderr tail for %s: %s",
                    p,
                    _tail_snapshot(stderr_tail),
                )
            return

        returncode = proc.wait(timeout=1.0)
    except Exception as e:
        logger.warning("[git_history_manifest] subprocess error for %s: %s", p, e)
        try:
            if proc and proc.poll() is None:
                proc.kill()
        except Exception:
            pass
        return

    elapsed_ms = int((time.monotonic() - started) * 1000)
    if returncode != 0:
        logger.warning(
            "[git_history_manifest] ingest_history.py failed for %s: exit=%d elapsed_ms=%d stderr=%s",
            p,
            returncode,
            elapsed_ms,
            _tail_snapshot(stderr_tail),
        )
        return

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


def _on_git_history_done(manifest_key: str, future: Future) -> None:
    with _GIT_HISTORY_INFLIGHT_LOCK:
        _GIT_HISTORY_INFLIGHT.discard(manifest_key)
        remaining = len(_GIT_HISTORY_INFLIGHT)
    logger.info("[git_history_manifest] in-flight remaining=%d", remaining)
    try:
        future.result()
    except Exception as e:
        logger.warning("[git_history_manifest] worker crashed for %s: %s", manifest_key, e)


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
    future.add_done_callback(lambda fut, manifest_key=key: _on_git_history_done(manifest_key, fut))


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
    if QDRANT_URL:
        env["QDRANT_URL"] = QDRANT_URL
    if repo_name:
        env["REPO_NAME"] = repo_name
    return env


def _maybe_handle_staging_file(
    path: Path,
    collection: str | None,
    repo_name: str | None,
    repo_key: str,
    repo_files: List[Path],
    state_env: Optional[Dict[str, str]],
    repo_progress: Dict[str, int],
    started_at: str,
) -> bool:
    if not (is_staging_enabled() and state_env and collection):
        return False

    _text, file_hash = _read_text_and_sha1(path)
    if file_hash:
        try:
            cached_hash = get_cached_file_hash(str(path), repo_name) if repo_name else None
        except Exception:
            cached_hash = None
        if cached_hash and cached_hash == file_hash:
            # Fast path: skip if content hash matches cached hash (file unchanged)
            # Safety: startup health check clears stale cache per-repo
            safe_print(f"[skip_unchanged] {path} (hash match)")
            _log_activity(repo_key, "skipped", path, {"reason": "hash_unchanged"})
            _advance_progress(repo_progress, repo_key, repo_files, started_at, path)
            return True

    cmd = [
        sys.executable or "python3",
        str(ROOT_DIR / "scripts" / "ingest_code.py"),
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

    for p in unique_paths:
        repo_path = _detect_repo_for_file(p) or Path(workspace_path)
        repo_key = str(repo_path)
        repo_files = repo_groups.get(repo_key, [])
        repo_name = _extract_repo_name_from_path(repo_key)
        collection = _get_collection_for_file(p)
        state_env: Optional[Dict[str, str]] = None
        try:
            st = get_workspace_state(repo_key, repo_name) if get_workspace_state else None
            if isinstance(st, dict):
                if is_staging_enabled():
                    state_env = st.get("indexing_env")
        except Exception:
            state_env = None

        if ".remote-git" in p.parts and p.suffix.lower() == ".json":
            try:
                _process_git_history_manifest(
                    p,
                    collection,
                    repo_name,
                    env_snapshot=(state_env if is_staging_enabled() else None),
                )
            except Exception as exc:
                safe_print(f"[commit_ingest_error] {p}: {exc}")
            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
            continue

        if not p.exists():
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
                    except Exception:
                        pass
                    safe_print(f"[deleted] {p} -> {collection}")
                except Exception:
                    pass
            try:
                if repo_name:
                    remove_cached_file(str(p), repo_name)
            except Exception:
                pass
            _log_activity(repo_key, "deleted", p)
            _advance_progress(repo_progress, repo_key, repo_files, started_at, p)
            continue

        if _maybe_handle_staging_file(
            p,
            collection,
            repo_name,
            repo_key,
            repo_files,
            state_env,
            repo_progress,
            started_at,
        ):
            continue
        if client is not None and model is not None:
            try:
                ok = _run_indexing_strategy(
                    p, client, model, collection, vector_name, model_dim, repo_name
                )
            except _SkipUnchanged:
                status = "skipped"
                safe_print(f"[{status}] {p} -> {collection}")
                _log_activity(repo_key, "skipped", p, {"reason": "hash_unchanged"})
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
            else:
                _log_activity(
                    repo_key, "skipped", p, {"reason": "no-change-or-error"}
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
    if not text:
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
) -> bool:
    if collection is None:
        return False
    try:
        idx.ensure_collection_and_indexes_once(client, collection, model_dim, vector_name)
    except Exception:
        pass

    text, file_hash = _read_text_and_sha1(path)
    ok = False
    if text is not None:
        try:
            language = idx.detect_language(path)
        except Exception:
            language = ""
        if file_hash:
            try:
                cached_hash = get_cached_file_hash(str(path), repo_name) if repo_name else None
            except Exception:
                cached_hash = None
            if cached_hash and cached_hash == file_hash:
                ok = True
                raise _SkipUnchanged()
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
