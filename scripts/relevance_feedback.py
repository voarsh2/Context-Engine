"""Shared storage and reindex reconciliation for relevance feedback."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

try:
    import fcntl  # type: ignore
except Exception:  # pragma: no cover
    fcntl = None


RECENT_META_TTL = 3600
RECENT_META_MAX = 4096
RECENT_META_KEYS = (
    "result_id",
    "target_id",
    "impression_id",
    "path",
    "host_path",
    "container_path",
    "symbol",
    "kind",
    "repo",
    "file_hash",
    "symbol_content_hash",
)


def _weights_dir() -> Path:
    return Path(os.environ.get("RERANKER_WEIGHTS_DIR", "/tmp/rerank_weights"))


def _safe_name(value: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(value))


def stable_target_id(*, repo: str = "", kind: str = "", symbol: str = "", path: str = "") -> str:
    repo = str(repo or "").strip()
    kind = str(kind or "").strip()
    symbol = str(symbol or "").strip()
    path = str(path or "").strip()
    if repo and symbol:
        key = f"symbol\x00{repo}\x00{kind}\x00{symbol}"
    elif symbol:
        key = f"symbol\x00{kind}\x00{symbol}"
    elif repo and path:
        key = f"file\x00{repo}\x00{path}"
    elif path:
        key = f"file\x00{path}"
    else:
        return ""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]


@contextmanager
def _locked_file(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with open(lock_path, "a+") as lock_file:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _read_json(path: Path) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        tmp.replace(path)
    finally:
        tmp.unlink(missing_ok=True)


def _recent_path(collection: str) -> Path:
    return _weights_dir() / f"{_safe_name(collection)}_recent_results.json"


def remember_recent_results(collection: str, results: list[dict]) -> None:
    """Persist recent result metadata for cross-process hands-off ratings."""
    if not collection or not results:
        return
    try:
        path = _recent_path(collection)
        now = time.time()
        expired_before = now - RECENT_META_TTL
        with _locked_file(path):
            data = _read_json(path)
            entries = data.get("results") if isinstance(data.get("results"), dict) else {}
            entries = {
                rid: entry
                for rid, entry in entries.items()
                if isinstance(entry, dict) and float(entry.get("ts", 0) or 0) >= expired_before
            }
            for result in results:
                rid = str(result.get("result_id") or "").strip()
                if not rid:
                    continue
                meta = {}
                for key in RECENT_META_KEYS:
                    val = result.get(key)
                    if val is not None and str(val).strip():
                        meta[key] = str(val).strip()
                if meta:
                    entries[rid] = {"ts": now, "meta": meta}
            if len(entries) > RECENT_META_MAX:
                ranked = sorted(
                    entries.items(),
                    key=lambda item: float((item[1] or {}).get("ts", 0) or 0),
                    reverse=True,
                )
                entries = dict(ranked[:RECENT_META_MAX])
            _write_json(path, {"updated_at": now, "results": entries})
    except OSError:
        return


def enrich_recent_rating(collection: str, rating: dict) -> dict:
    """Fill rating metadata from shared recent search results."""
    if not isinstance(rating, dict):
        return {}
    out = dict(rating)
    rid = str(out.get("result_id") or "").strip()
    if not collection or not rid:
        return out
    try:
        data = _read_json(_recent_path(collection))
    except OSError:
        return out
    entry = (data.get("results") or {}).get(rid)
    if not isinstance(entry, dict):
        return out
    if float(entry.get("ts", 0) or 0) < time.time() - RECENT_META_TTL:
        return out
    meta = entry.get("meta") if isinstance(entry.get("meta"), dict) else {}
    for key, val in meta.items():
        out.setdefault(key, val)
    return out


def _symbol_tokens(info: dict) -> set[str]:
    content = str(info.get("content") or "")
    return set(re.findall(r"[A-Za-z_][A-Za-z0-9_]{2,}", content))


def _symbol_target(info: dict, *, repo: str, path: str) -> dict:
    symbol = str(info.get("name") or "").strip()
    kind = str(info.get("type") or "").strip()
    target_id = stable_target_id(repo=repo, kind=kind, symbol=symbol, path=path)
    return {
        "target_id": target_id,
        "path": path,
        "container_path": path,
        "repo": str(repo or ""),
        "kind": kind,
        "symbol": symbol,
        "symbol_content_hash": str(info.get("content_hash") or ""),
    }


def build_symbol_reconciliations(
    old_symbols: dict,
    new_symbols: dict,
    *,
    repo: str,
    path: str,
    split_min_overlap: float = 0.45,
    split_min_coverage: float = 0.75,
) -> dict[str, list[dict]]:
    """Map removed feedback targets to conservative rename/split successors."""
    old_symbols = old_symbols or {}
    new_symbols = new_symbols or {}
    new_by_name_kind: dict[tuple[str, str], list[dict]] = {}
    new_by_hash_kind: dict[tuple[str, str], list[dict]] = {}
    for info in new_symbols.values():
        kind = str(info.get("type") or "")
        name = str(info.get("name") or "")
        content_hash = str(info.get("content_hash") or "")
        new_by_name_kind.setdefault((kind, name), []).append(info)
        if kind and content_hash:
            new_by_hash_kind.setdefault((kind, content_hash), []).append(info)

    mappings: dict[str, list[dict]] = {}
    for old_info in old_symbols.values():
        kind = str(old_info.get("type") or "")
        name = str(old_info.get("name") or "")
        old_hash = str(old_info.get("content_hash") or "")
        old_id = stable_target_id(repo=repo, kind=kind, symbol=name, path=path)
        if not old_id:
            continue

        # Same logical symbol persists; its stable target ID already survives edits.
        if new_by_name_kind.get((kind, name)):
            continue

        exact = new_by_hash_kind.get((kind, old_hash)) or []
        if len(exact) == 1:
            mappings[old_id] = [
                {
                    "reason": "rename_exact_content",
                    "inheritance_weight": 1.0,
                    "target": _symbol_target(exact[0], repo=repo, path=path),
                }
            ]
            continue

        old_tokens = _symbol_tokens(old_info)
        if len(old_tokens) < 4:
            continue
        candidates = []
        covered: set[str] = set()
        for new_info in new_symbols.values():
            if str(new_info.get("type") or "") != kind:
                continue
            overlap_tokens = old_tokens & _symbol_tokens(new_info)
            overlap = len(overlap_tokens) / len(old_tokens)
            if overlap >= split_min_overlap:
                candidates.append((new_info, overlap, overlap_tokens))
                covered.update(overlap_tokens)
        coverage = len(covered) / len(old_tokens)
        if len(candidates) < 2 or coverage < split_min_coverage:
            continue
        total_overlap = sum(item[1] for item in candidates) or 1.0
        mappings[old_id] = [
            {
                "reason": "split_token_coverage",
                "inheritance_weight": round(overlap / total_overlap, 6),
                "target": _symbol_target(info, repo=repo, path=path),
            }
            for info, overlap, _ in candidates
        ]
    return mappings


def reconcile_collection_weights(
    collection: str,
    mappings: dict[str, list[dict]],
) -> int:
    """Migrate existing feedback weights to reconciled symbol targets."""
    if not collection or not mappings:
        return 0
    path = _weights_dir() / f"{collection}_relevance.json"
    if not path.exists():
        return 0
    migrated = 0
    with _locked_file(path):
        data = _read_json(path)
        results = data.get("results") if isinstance(data.get("results"), dict) else {}
        for old_id, successors in mappings.items():
            old_entry = results.get(old_id)
            if not isinstance(old_entry, dict):
                continue
            lineage = list(old_entry.get("lineage") or [])
            for successor in successors:
                target = successor.get("target") if isinstance(successor.get("target"), dict) else {}
                new_id = str(target.get("target_id") or "")
                inheritance = float(successor.get("inheritance_weight", 1.0) or 0)
                if not new_id or inheritance <= 0:
                    continue
                new_entry = dict(old_entry)
                new_entry["target"] = target
                new_entry["inheritance_weight"] = inheritance
                new_entry["lineage"] = lineage + [
                    {
                        "from_target_id": old_id,
                        "reason": successor.get("reason") or "reindex",
                        "at": time.time(),
                    }
                ]
                current = results.get(new_id)
                if not isinstance(current, dict) or float(current.get("count", 0) or 0) <= float(
                    new_entry.get("count", 0) or 0
                ):
                    results[new_id] = new_entry
                    migrated += 1
            old_entry["superseded_by"] = [
                str((successor.get("target") or {}).get("target_id") or "")
                for successor in successors
            ]
        if migrated:
            data["updated_at"] = time.time()
            data["results"] = results
            _write_json(path, data)
    return migrated
