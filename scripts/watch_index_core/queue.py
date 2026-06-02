"""Debounced change queue used by the watcher."""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Callable, Iterable, List, Set

from .config import DELAY_SECS, LOGGER, RECENT_FINGERPRINT_TTL_SECS


class ChangeQueue:
    """Collects file paths and flushes them after a debounce interval."""

    def __init__(self, process_cb: Callable[[List[Path]], None]):
        self._lock = threading.Lock()
        self._paths: Set[Path] = set()
        self._pending: Set[Path] = set()
        self._forced_paths: Set[Path] = set()
        self._pending_forced: Set[Path] = set()
        self._timer: threading.Timer | None = None
        self._process_cb = process_cb
        # Serialize processing to avoid concurrent use of TextEmbedding/QdrantClient
        self._processing_lock = threading.Lock()
        self._recent_fingerprints: dict[Path, tuple[tuple[int, int], float]] = {}

    def add(self, p: Path, *, force: bool = False) -> None:
        with self._lock:
            already_queued = p in self._paths
            self._paths.add(p)
            if force:
                self._forced_paths.add(p)
            if self._timer is not None and not already_queued:
                try:
                    self._timer.cancel()
                except Exception as exc:
                    LOGGER.error(
                        "Failed to cancel timer in ChangeQueue.add",
                        extra={"error": str(exc)},
                    )
            if self._timer is None or not already_queued:
                self._timer = threading.Timer(DELAY_SECS, self._flush)
                self._timer.daemon = True
                self._timer.start()

    def stats(self) -> dict[str, int | bool]:
        with self._lock:
            return {
                "queued": len(self._paths),
                "pending": len(self._pending),
                "forced": len(self._forced_paths),
                "pending_forced": len(self._pending_forced),
                "processing": self._processing_lock.locked(),
            }

    def _fingerprint_path(self, p: Path) -> tuple[int, int] | None:
        try:
            st = p.stat()
            return (
                int(getattr(st, "st_size", 0)),
                int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))),
            )
        except Exception:
            return None

    def _filter_recent_paths(
        self,
        paths: Iterable[Path],
        *,
        forced_paths: Iterable[Path] | None = None,
    ) -> list[Path]:
        ttl = float(RECENT_FINGERPRINT_TTL_SECS)
        forced = set(forced_paths or [])
        if ttl <= 0:
            return list(paths)

        now = time.time()
        keep: list[Path] = []
        for p in paths:
            if p in forced:
                keep.append(p)
                continue
            fp = self._fingerprint_path(p)
            if fp is None:
                keep.append(p)
                continue
            prev = self._recent_fingerprints.get(p)
            if prev is not None:
                prev_fp, prev_ts = prev
                if prev_fp == fp and (now - prev_ts) < ttl:
                    continue
            keep.append(p)
        return keep

    def _mark_recent_paths(self, paths: Iterable[Path]) -> None:
        ttl = float(RECENT_FINGERPRINT_TTL_SECS)
        if ttl <= 0:
            return
        now = time.time()
        for p in paths:
            fp = self._fingerprint_path(p)
            if fp is None:
                continue
            self._recent_fingerprints[p] = (fp, now)
        # Keep at least a 1s grace for small TTLs while using a proportional
        # buffer for larger TTLs so stale handled fingerprints age out cleanly.
        cutoff = now - max(ttl * 2.0, ttl + 1.0)
        stale = [p for p, (_fp, ts) in self._recent_fingerprints.items() if ts < cutoff]
        for p in stale:
            self._recent_fingerprints.pop(p, None)

    def _drain_pending(self) -> tuple[list[Path], Set[Path]] | None:
        with self._lock:
            if not self._pending:
                return None
            todo = list(self._pending)
            todo_forced = {p for p in todo if p in self._pending_forced}
            self._pending.clear()
            self._pending_forced.clear()
            return todo, todo_forced

    def _flush(self) -> None:
        # Grab current batch
        with self._lock:
            paths = list(self._paths)
            forced_paths = {p for p in paths if p in self._forced_paths}
            self._paths.clear()
            self._forced_paths.difference_update(paths)
            self._timer = None

        # Try to run the processor exclusively; if busy, queue and return
        if not self._processing_lock.acquire(blocking=False):
            with self._lock:
                self._pending.update(paths)
                self._pending_forced.update(forced_paths)
                if self._timer is None:
                    # schedule a follow-up flush to pick up pending when free
                    self._timer = threading.Timer(DELAY_SECS, self._flush)
                    self._timer.daemon = True
                    self._timer.start()
            return
        try:
            # Per-file locking in index_single_file handles indexer/watcher coordination
            todo: Iterable[Path] = paths
            todo_forced: Set[Path] = set(forced_paths)
            while True:
                filtered_todo = self._filter_recent_paths(todo, forced_paths=todo_forced)
                if not filtered_todo:
                    pending = self._drain_pending()
                    if pending is None:
                        break
                    todo, todo_forced = pending
                    continue
                try:
                    self._process_cb(list(filtered_todo))
                    self._mark_recent_paths(filtered_todo)
                except Exception as exc:
                    # Log processing error via structured logging
                    try:
                        LOGGER.error(
                            "Processing batch failed in ChangeQueue._flush",
                            extra={"error": str(exc), "batch_size": len(filtered_todo)},
                            exc_info=True,
                        )
                    except Exception as inner_exc:  # pragma: no cover - logging fallback
                        # If logging fails, ensure we don't lose both errors
                        import sys
                        try:
                            print(
                                f"[watcher_error] logging failed: {inner_exc}, "
                                f"original error: {exc}",
                                file=sys.stderr,
                            )
                        except Exception:
                            pass  # Last resort: can't even print
                # drain any pending accumulated during processing
                pending = self._drain_pending()
                if pending is None:
                    break
                todo, todo_forced = pending
        finally:
            self._processing_lock.release()


__all__ = ["ChangeQueue"]
