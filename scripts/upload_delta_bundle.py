import os
import json
import shutil
import tarfile
import hashlib
import re
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from scripts.workspace_state import (
    _normalize_cache_key_path,
    _extract_repo_name_from_path,
    get_staging_targets,
    get_collection_state_snapshot,
    is_staging_enabled,
    upsert_index_journal_entries,
)


logger = logging.getLogger(__name__)

WORK_DIR = os.environ.get("WORK_DIR") or os.environ.get("WORKDIR") or "/work"
_SLUGGED_REPO_RE = re.compile(r"^.+-[0-9a-f]{16}(?:_old)?$")


def _normalize_hash_value(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if ":" in raw:
        _, _, digest = raw.partition(":")
        if digest.strip():
            return digest.strip().lower()
    return raw.lower()


def _file_matches_hash(path: Path, expected_hash: str) -> bool:
    """Verify a destination is already the requested content for idempotent retries."""
    expected = _normalize_hash_value(expected_hash)
    if not expected or not path.is_file():
        return False
    try:
        return hashlib.sha1(path.read_bytes()).hexdigest() == expected
    except OSError:
        return False


def _build_upsert_journal_entry(path: Path | str, content_hash: Optional[str]) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "path": str(path),
        "op_type": "upsert",
    }
    if content_hash:
        entry["content_hash"] = content_hash
    return entry


def _build_delete_journal_entry(path: Path | str, content_hash: Optional[str] = None) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "path": str(path),
        "op_type": "delete",
    }
    if content_hash:
        entry["content_hash"] = content_hash
    return entry


def _load_cache_hashes(cache_path: Path) -> Dict[str, str]:
    try:
        with cache_path.open("r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except (OSError, ValueError, json.JSONDecodeError):
        return {}

    file_hashes = data.get("file_hashes", {})
    if not isinstance(file_hashes, dict):
        return {}

    normalized: Dict[str, str] = {}
    for path_key, value in file_hashes.items():
        if isinstance(value, dict):
            hash_value = value.get("hash")
        else:
            hash_value = value
        digest = _normalize_hash_value(hash_value)
        if digest:
            normalized[_normalize_cache_key_path(str(path_key))] = digest
    return normalized


def _load_replica_cache_hashes(workspace_root: Path, slug: str) -> Dict[str, str]:
    merged: Dict[str, str] = {}
    cache_paths = (
        Path(WORK_DIR) / ".codebase" / "repos" / slug / "cache.json",
        workspace_root / ".codebase" / "cache.json",
    )
    for cache_path in cache_paths:
        if not cache_path.exists():
            continue
        merged.update(_load_cache_hashes(cache_path))
    return merged


def _flush_replica_cache_hashes(workspace_root: Path, slug: str, hashes: Dict[str, str]) -> None:
    """Flush replica hashes to workspace cache.json."""
    try:
        cache_path = workspace_root / ".codebase" / "cache.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)

        # Read existing cache to preserve other entries
        existing_data = {}
        if cache_path.exists():
            try:
                with cache_path.open("r", encoding="utf-8-sig") as f:
                    existing_data = json.load(f)
            except (OSError, ValueError, json.JSONDecodeError):
                existing_data = {}

        # Update file_hashes section
        if not isinstance(existing_data, dict):
            existing_data = {}
        existing_data["file_hashes"] = hashes

        # Write back atomically
        temp_path = cache_path.with_suffix(".tmp")
        with temp_path.open("w", encoding="utf-8") as f:
            json.dump(existing_data, f, indent=2)
        temp_path.replace(cache_path)
    except Exception as e:
        logger.debug(f"[upload_service] Failed to flush cache for {slug}: {e}")


def get_workspace_key(workspace_path: str) -> str:
    """Generate 16-char hash for collision avoidance in remote uploads.

    Remote uploads may have identical folder names from different users,
    so uses longer hash than local indexing (8-chars) to ensure uniqueness.

    Both host paths (/home/user/project/repo) and container paths (/work/repo)
    should generate the same key for the same repository.
    """
    repo_name = Path(workspace_path).name
    if _SLUGGED_REPO_RE.match(repo_name):
        return repo_name[-16:]
    return hashlib.sha256(repo_name.encode("utf-8")).hexdigest()[:16]


def _cleanup_empty_dirs(path: Path, stop_at: Path) -> None:
    """Recursively remove empty directories up to stop_at (exclusive)."""
    try:
        path = path.resolve()
        stop_at = stop_at.resolve()
    except Exception:
        pass
    while True:
        try:
            if path == stop_at or not path.exists() or not path.is_dir():
                break
            if any(path.iterdir()):
                break
            path.rmdir()
            path = path.parent
        except Exception:
            break


def _resolve_replica_roots(workspace_path: str, *, create_missing: bool = True) -> Dict[str, Path]:
    workspace_leaf = Path(workspace_path).name

    repo_name_for_state: Optional[str] = None
    serving_slug: Optional[str] = None
    active_slug: Optional[str] = None
    try:
        repo_name_for_state = _extract_repo_name_from_path(workspace_path)
        if repo_name_for_state:
            snapshot = get_collection_state_snapshot(
                workspace_path=None,
                repo_name=repo_name_for_state,
            )  # type: ignore[arg-type]
            serving_slug = snapshot.get("serving_repo_slug")
            active_slug = snapshot.get("active_repo_slug")
    except Exception:
        serving_slug = None
        active_slug = None

    slug_order: list[str] = []
    serving_candidate: Optional[str] = None
    if serving_slug and _SLUGGED_REPO_RE.match(serving_slug):
        serving_candidate = serving_slug
    if active_slug and _SLUGGED_REPO_RE.match(active_slug) and active_slug not in slug_order:
        slug_order.append(active_slug)

    staging_active = False
    staging_gate = bool(is_staging_enabled())
    try:
        if serving_slug and str(serving_slug).endswith("_old"):
            staging_active = True
    except Exception:
        staging_active = False

    if not staging_gate:
        staging_active = False

    def _append_slug(slug: Optional[str]) -> None:
        if slug and _SLUGGED_REPO_RE.match(slug) and slug not in slug_order:
            slug_order.append(slug)

    if repo_name_for_state and _SLUGGED_REPO_RE.match(repo_name_for_state):
        canonical_slug = (
            repo_name_for_state[:-4]
            if repo_name_for_state.endswith("_old")
            else repo_name_for_state
        )
        old_slug_candidate = (
            repo_name_for_state
            if repo_name_for_state.endswith("_old")
            else f"{canonical_slug}_old"
        )
        if staging_active:
            slug_order = []
            _append_slug(canonical_slug)
            _append_slug(old_slug_candidate)
        elif not slug_order:
            _append_slug(canonical_slug)
            old_slug_path = Path(WORK_DIR) / old_slug_candidate
            if old_slug_path.exists():
                _append_slug(old_slug_candidate)

    if not slug_order:
        if _SLUGGED_REPO_RE.match(workspace_leaf):
            slug_order.append(workspace_leaf)
        else:
            repo_name = _extract_repo_name_from_path(workspace_path) or workspace_leaf
            workspace_key = get_workspace_key(workspace_path)
            slug_order.append(f"{repo_name}-{workspace_key}")

    if staging_gate and not staging_active:
        try:
            repo_name_for_staging = _extract_repo_name_from_path(workspace_path) or slug_order[0]
            targets = get_staging_targets(
                workspace_path=workspace_path,
                repo_name=repo_name_for_staging,
            )
            if isinstance(targets, dict) and targets.get("staging"):
                staging_active = True
        except Exception as staging_err:
            logger.debug("[upload_service] Failed to detect staging: %s", staging_err)

    def _slug_exists(slug: str) -> bool:
        try:
            return (
                (Path(WORK_DIR) / slug).exists()
                or (Path(WORK_DIR) / ".codebase" / "repos" / slug).exists()
            )
        except Exception:
            return False

    if staging_gate and (not staging_active) and slug_order:
        primary = slug_order[0]
        if _SLUGGED_REPO_RE.match(primary):
            canonical = primary[:-4] if primary.endswith("_old") else primary
            inferred_old = primary if primary.endswith("_old") else f"{canonical}_old"
            if _slug_exists(inferred_old):
                staging_active = True

    if staging_gate and staging_active and slug_order:
        primary = slug_order[0]
        if _SLUGGED_REPO_RE.match(primary):
            canonical = primary[:-4] if primary.endswith("_old") else primary
            old_slug = primary if primary.endswith("_old") else f"{canonical}_old"
            desired = [canonical, old_slug]
            slug_order = [s for s in desired if _SLUGGED_REPO_RE.match(s)]
    elif staging_gate and not staging_active and serving_candidate:
        # Keep the canonical replica when serving and active are the same.
        # A serving-only target should be removed only when a distinct active
        # replica is available.
        if (
            active_slug
            and active_slug != serving_candidate
            and serving_candidate in slug_order
        ):
            slug_order = [s for s in slug_order if s != serving_candidate]

    if staging_gate:
        try:
            logger.info("[upload_service] Delta bundle targets (staging=%s): %s", staging_active, slug_order)
        except Exception:
            pass

    replica_roots: Dict[str, Path] = {}
    for slug in slug_order:
        path = Path(WORK_DIR) / slug
        if create_missing:
            path.mkdir(parents=True, exist_ok=True)
            try:
                marker_dir = Path(WORK_DIR) / ".codebase" / "repos" / slug
                marker_dir.mkdir(parents=True, exist_ok=True)
                (marker_dir / ".ctxce_managed_upload").write_text("1\n")
            except Exception:
                pass
        replica_roots[slug] = path.resolve()
    return replica_roots


def _enqueue_replica_journal_entries(
    *,
    workspace_root: Path,
    slug: str,
    entries: list[Dict[str, Any]],
) -> None:
    if not entries:
        return
    try:
        upsert_index_journal_entries(
            entries,
            workspace_path=str(workspace_root),
            repo_name=slug,
        )
    except Exception as exc:
        logger.debug(
            "[upload_service] Failed to enqueue index journal entries for %s: %s",
            workspace_root,
            exc,
        )


def _safe_join(base: Path, rel: str) -> Path:
    rp = Path(str(rel))
    if str(rp) in {".", ""}:
        raise ValueError("Invalid operation path")
    if rp.is_absolute():
        raise ValueError(f"Absolute paths are not allowed: {rel}")
    base_resolved = base.resolve()
    candidate = (base_resolved / rp).resolve()
    try:
        ok = candidate.is_relative_to(base_resolved)
    except Exception:
        ok = os.path.commonpath([str(base_resolved), str(candidate)]) == str(base_resolved)
    if not ok:
        raise ValueError(f"Path escapes workspace: {rel}")
    return candidate


def _sanitize_operation_path(rel_path: str, replica_roots: Dict[str, Path]) -> Optional[str]:
    sanitized_path = rel_path
    skipped_due_to_exact_slug = False
    for slug in replica_roots.keys():
        if sanitized_path == slug:
            skipped_due_to_exact_slug = True
            break
        prefix = f"{slug}/"
        if sanitized_path.startswith(prefix):
            sanitized_path = sanitized_path[len(prefix):]
            break
    if skipped_due_to_exact_slug or not sanitized_path:
        return None
    return sanitized_path


def plan_delta_upload(
    workspace_path: str,
    operations: list[Dict[str, Any]],
    file_hashes: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    needed_files = {
        "created": [],
        "updated": [],
        "moved": [],
    }
    operations_count = {
        "created": 0,
        "updated": 0,
        "deleted": 0,
        "moved": 0,
        "skipped": 0,
        "skipped_hash_match": 0,
        "failed": 0,
    }
    needed_size_bytes = 0
    replica_roots = _resolve_replica_roots(workspace_path, create_missing=False)
    replica_cache_hashes = {
        slug: _load_replica_cache_hashes(root, slug)
        for slug, root in replica_roots.items()
    }
    diagnostics = {
        "candidate_operations": len(operations or []),
        "cache_entries": sum(len(hashes) for hashes in replica_cache_hashes.values()),
        "cache_hash_matches": 0,
        "hash_mismatches": 0,
        "missing_targets": 0,
    }
    normalized_hashes = {
        str(rel_path): _normalize_hash_value(hash_value)
        for rel_path, hash_value in (file_hashes or {}).items()
        if _normalize_hash_value(hash_value)
    }

    for operation in operations:
        op_type = str(operation.get("operation") or "")
        rel_path = operation.get("path")
        if not rel_path:
            operations_count["skipped"] += 1
            continue

        sanitized = _sanitize_operation_path(str(rel_path), replica_roots)
        if not sanitized:
            operations_count["skipped"] += 1
            continue

        if op_type == "deleted":
            operations_count["deleted"] += 1
            continue
        if op_type == "moved":
            operations_count["moved"] += 1
            source_rel_path = operation.get("source_path") or operation.get("source_relative_path")
            if not source_rel_path:
                needed_files["moved"].append(sanitized)
                needed_size_bytes += int(operation.get("size_bytes") or 0)
                continue

            move_needs_content = False
            for _slug, root in replica_roots.items():
                try:
                    safe_source_path = _safe_join(root, str(source_rel_path))
                except ValueError:
                    logger.warning(
                        "[upload_service] Invalid move source path during plan: %s (root=%s)",
                        source_rel_path,
                        root,
                    )
                    move_needs_content = True
                    break
                if not safe_source_path.exists():
                    move_needs_content = True
                    break
            if move_needs_content:
                needed_files["moved"].append(sanitized)
                needed_size_bytes += int(operation.get("size_bytes") or 0)
            continue
        if op_type not in {"created", "updated"}:
            operations_count["failed"] += 1
            continue

        op_content_hash = _normalize_hash_value(
            operation.get("content_hash") or normalized_hashes.get(sanitized)
        )
        if not op_content_hash:
            needed_files[op_type].append(sanitized)
            operations_count[op_type] += 1
            needed_size_bytes += int(operation.get("size_bytes") or 0)
            continue

        needs_content = False
        for slug, root in replica_roots.items():
            try:
                target_path = _safe_join(root, sanitized)
            except ValueError:
                logger.warning(
                    "[upload_service] Invalid %s path during plan: %s (root=%s)",
                    op_type,
                    sanitized,
                    root,
                )
                continue
            target_key = _normalize_cache_key_path(str(target_path))
            cached_hash = replica_cache_hashes.get(slug, {}).get(target_key)
            # A cache hit is only authoritative while the indexed replica file
            # still exists. Otherwise a stale cache entry can suppress repair.
            if cached_hash == op_content_hash and target_path.is_file():
                continue

            needs_content = True
            if not target_path.is_file():
                diagnostics["missing_targets"] += 1
            else:
                diagnostics["hash_mismatches"] += 1
            break

        if needs_content:
            needed_files[op_type].append(sanitized)
            operations_count[op_type] += 1
            needed_size_bytes += int(operation.get("size_bytes") or 0)
        else:
            operations_count["skipped"] += 1
            operations_count["skipped_hash_match"] += 1
            diagnostics["cache_hash_matches"] += 1

    diagnostics["needed_content_operations"] = sum(
        operations_count[op_type] for op_type in ("created", "updated", "moved")
    )
    logger.info(
        "[upload_service] Delta plan workspace=%s targets=%s candidates=%d "
        "needed=%d skipped_hash_match=%d cache_entries=%d cache_matches=%d",
        workspace_path,
        list(replica_roots.keys()),
        diagnostics["candidate_operations"],
        diagnostics["needed_content_operations"],
        operations_count["skipped_hash_match"],
        diagnostics["cache_entries"],
        diagnostics["cache_hash_matches"],
    )

    return {
        "needed_files": needed_files,
        "operation_counts_preview": operations_count,
        "needed_size_bytes": needed_size_bytes,
        "replica_targets": list(replica_roots.keys()),
        "diagnostics": diagnostics,
    }


def apply_delta_operations(
    workspace_path: str,
    operations: list[Dict[str, Any]],
    file_hashes: Optional[Dict[str, str]] = None,
) -> Dict[str, int]:
    """Apply metadata-only delta operations without requiring a tar bundle."""
    operations_count = {
        "created": 0,
        "updated": 0,
        "deleted": 0,
        "moved": 0,
        "skipped": 0,
        "skipped_hash_match": 0,
        "failed": 0,
    }

    try:
        replica_roots = _resolve_replica_roots(workspace_path)
        if not replica_roots:
            raise ValueError(f"No replica roots available for workspace: {workspace_path}")
        replica_cache_hashes = {
            slug: _load_replica_cache_hashes(root, slug)
            for slug, root in replica_roots.items()
        }
        journal_entries_by_slug: Dict[str, list[Dict[str, Any]]] = {
            slug: [] for slug in replica_roots.keys()
        }
        normalized_hashes = {
            str(rel_path): _normalize_hash_value(hash_value)
            for rel_path, hash_value in (file_hashes or {}).items()
            if _normalize_hash_value(hash_value)
        }

        for operation in operations:
            op_type = str(operation.get("operation") or "")
            rel_path = operation.get("path")

            if not rel_path:
                operations_count["skipped"] += 1
                continue

            sanitized_path = _sanitize_operation_path(str(rel_path), replica_roots)
            if not sanitized_path:
                operations_count["skipped"] += 1
                continue

            rel_path = sanitized_path

            if op_type not in {"deleted", "moved"}:
                operations_count["failed"] += 1
                continue

            source_rel_path = None
            if op_type == "moved":
                raw_source = operation.get("source_path") or operation.get("source_relative_path")
                if not raw_source:
                    operations_count["failed"] += 1
                    continue
                source_rel_path = _sanitize_operation_path(str(raw_source), replica_roots)
                if not source_rel_path:
                    operations_count["failed"] += 1
                    continue

            replica_results: Dict[str, str] = {}
            for slug, root in replica_roots.items():
                target_path = _safe_join(root, rel_path)
                target_key = _normalize_cache_key_path(str(target_path))
                replica_hashes = replica_cache_hashes.setdefault(slug, {})
                op_content_hash = _normalize_hash_value(
                    operation.get("content_hash") or normalized_hashes.get(rel_path)
                )

                try:
                    if op_type == "deleted":
                        if target_path.exists():
                            target_path.unlink(missing_ok=True)
                        _cleanup_empty_dirs(target_path.parent, root)
                        replica_hashes.pop(target_key, None)
                        journal_entries_by_slug.setdefault(slug, []).append(
                            _build_delete_journal_entry(target_path)
                        )
                        replica_results[slug] = "applied"
                        continue

                    safe_source_path = _safe_join(root, source_rel_path or "")
                    if not safe_source_path.exists():
                        if _file_matches_hash(target_path, op_content_hash):
                            replica_hashes.pop(
                                _normalize_cache_key_path(str(safe_source_path)), None
                            )
                            replica_hashes[target_key] = op_content_hash
                            journal_entries_by_slug.setdefault(slug, []).extend(
                                [
                                    _build_delete_journal_entry(
                                        safe_source_path, op_content_hash
                                    ),
                                    _build_upsert_journal_entry(
                                        target_path, op_content_hash
                                    ),
                                ]
                            )
                            replica_results[slug] = "skipped_hash_match"
                            continue
                        replica_results[slug] = "failed"
                        continue

                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    if target_path.exists():
                        if target_path.is_dir():
                            raise IsADirectoryError(
                                f"[upload_delta_bundle] move target is a directory: {target_path}"
                            )
                        else:
                            target_path.unlink()
                    shutil.move(str(safe_source_path), str(target_path))
                    _cleanup_empty_dirs(safe_source_path.parent, root)
                    source_key = _normalize_cache_key_path(str(safe_source_path))
                    moved_hash = replica_hashes.pop(source_key, None)
                    if op_content_hash:
                        replica_hashes[target_key] = op_content_hash
                    elif moved_hash:
                        replica_hashes[target_key] = moved_hash
                    move_entry_hash = op_content_hash or moved_hash
                    journal_entries_by_slug.setdefault(slug, []).extend(
                        [
                            _build_delete_journal_entry(safe_source_path, move_entry_hash),
                            _build_upsert_journal_entry(target_path, move_entry_hash),
                        ]
                    )
                    replica_results[slug] = "applied"
                except Exception as exc:
                    logger.debug(
                        "[upload_service] Failed to apply metadata-only %s to %s in %s: %s",
                        op_type,
                        rel_path,
                        root,
                        exc,
                    )
                    replica_results[slug] = "failed"

            applied_any = any(result == "applied" for result in replica_results.values())
            skipped_hash_match = bool(replica_results) and all(
                result in {"applied", "skipped_hash_match"}
                for result in replica_results.values()
            )
            success_all = skipped_hash_match
            if applied_any:
                operations_count[op_type] += 1
                if not success_all:
                    # Keep the operation count as applied for reporting, but
                    # surface the replica failure so the sequence is retried.
                    operations_count["failed"] += 1
                    logger.debug(
                        "[upload_service] Partial metadata-only success for %s %s: %s",
                        op_type,
                        rel_path,
                        replica_results,
                    )
            elif skipped_hash_match:
                operations_count["skipped"] += 1
                operations_count["skipped_hash_match"] += 1
            else:
                operations_count["failed"] += 1

        for slug, root in replica_roots.items():
            _enqueue_replica_journal_entries(
                workspace_root=root,
                slug=slug,
                entries=journal_entries_by_slug.get(slug, []),
            )
            # Flush updated replica hashes to disk (including empty caches)
            replica_hashes = replica_cache_hashes.get(slug, {})
            _flush_replica_cache_hashes(root, slug, replica_hashes)

        return operations_count
    except Exception as e:
        logger.error(f"Error applying metadata-only delta operations: {e}")
        raise


def process_delta_bundle(workspace_path: str, bundle_path: Path, manifest: Dict[str, Any]) -> Dict[str, int]:
    """Process delta bundle and return operation counts."""
    operations_count = {
        "created": 0,
        "updated": 0,
        "deleted": 0,
        "moved": 0,
        "skipped": 0,
        "skipped_hash_match": 0,
        "failed": 0,
    }

    try:
        replica_roots = _resolve_replica_roots(workspace_path)
        if not replica_roots:
            raise ValueError(f"No replica roots available for workspace: {workspace_path}")
        primary_slug = next(iter(replica_roots))
        workspace_root = replica_roots[primary_slug]

        def _member_suffix(name: str, marker: str) -> Optional[str]:
            idx = name.find(marker)
            if idx < 0:
                return None
            suffix = name[idx + len(marker):]
            return suffix or None

        with tarfile.open(bundle_path, "r:gz") as tar:
            ops_member = None
            hashes_member = None
            git_member = None
            created_members: Dict[str, tarfile.TarInfo] = {}
            updated_members: Dict[str, tarfile.TarInfo] = {}
            moved_members: Dict[str, tarfile.TarInfo] = {}
            for member in tar.getmembers():
                name = member.name
                if name.endswith("metadata/operations.json"):
                    ops_member = member
                    continue
                if name.endswith("metadata/hashes.json"):
                    hashes_member = member
                    continue
                if name.endswith("metadata/git_history.json"):
                    git_member = member
                    continue
                created_rel = _member_suffix(name, "files/created/")
                if created_rel:
                    created_members[created_rel] = member
                    continue
                updated_rel = _member_suffix(name, "files/updated/")
                if updated_rel:
                    updated_members[updated_rel] = member
                    continue
                moved_rel = _member_suffix(name, "files/moved/")
                if moved_rel:
                    moved_members[moved_rel] = member

            if not ops_member:
                raise ValueError("operations.json not found in bundle")

            ops_file = tar.extractfile(ops_member)
            if not ops_file:
                raise ValueError("Cannot extract operations.json")

            operations_data = json.loads(ops_file.read().decode("utf-8"))
            operations = operations_data.get("operations", [])
            bundle_hashes: Dict[str, str] = {}
            if hashes_member:
                hashes_file = tar.extractfile(hashes_member)
                if hashes_file:
                    hashes_data = json.loads(hashes_file.read().decode("utf-8"))
                    raw_hashes = hashes_data.get("file_hashes", {})
                    if isinstance(raw_hashes, dict):
                        for rel_path, hash_value in raw_hashes.items():
                            digest = _normalize_hash_value(hash_value)
                            if digest:
                                bundle_hashes[str(rel_path)] = digest

            replica_cache_hashes = {
                slug: _load_replica_cache_hashes(root, slug)
                for slug, root in replica_roots.items()
            }
            journal_entries_by_slug: Dict[str, list[Dict[str, Any]]] = {
                slug: [] for slug in replica_roots.keys()
            }

            # Best-effort: extract git history metadata for watcher to ingest
            try:
                if git_member:
                    git_file = tar.extractfile(git_member)
                    if git_file:
                        history_bytes = git_file.read()
                        bundle_id = manifest.get("bundle_id") or "unknown"
                        for root in replica_roots.values():
                            try:
                                history_dir = root / ".remote-git"
                                history_dir.mkdir(parents=True, exist_ok=True)
                                history_path = history_dir / f"git_history_{bundle_id}.json"
                                history_path.write_bytes(history_bytes)
                            except Exception as write_err:
                                logger.debug(
                                    f"[upload_service] Failed to write git history manifest for {root}: {write_err}",
                                )
            except Exception as git_err:
                logger.debug(f"[upload_service] Error extracting git history metadata: {git_err}")

            def _apply_operation_to_workspace(
                slug: str,
                workspace_root: Path,
                op_type: str,
                rel_path: str,
                operation: Dict[str, Any],
            ) -> str:
                """Apply a single file operation to a workspace."""
                target_path = _safe_join(workspace_root, rel_path)
                target_key = _normalize_cache_key_path(str(target_path))
                replica_hashes = replica_cache_hashes.setdefault(slug, {})
                op_content_hash = _normalize_hash_value(
                    operation.get("content_hash") or bundle_hashes.get(rel_path)
                )

                safe_source_path = None
                source_rel_path = None
                if op_type == "moved":
                    source_rel_path = operation.get("source_path") or operation.get("source_relative_path")
                    if source_rel_path:
                        safe_source_path = _safe_join(workspace_root, source_rel_path)

                try:
                    if op_type == "created":
                        if op_content_hash and target_path.is_file():
                            cached_hash = replica_hashes.get(target_key)
                            if cached_hash and cached_hash == op_content_hash:
                                return "skipped_hash_match"
                        file_member = created_members.get(rel_path)
                        if file_member:
                            file_content = tar.extractfile(file_member)
                            if file_content:
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                target_path.write_bytes(file_content.read())
                                if op_content_hash:
                                    replica_hashes[target_key] = op_content_hash
                                journal_entries_by_slug.setdefault(slug, []).append(
                                    _build_upsert_journal_entry(target_path, op_content_hash)
                                )
                                return "applied"
                            else:
                                return "failed"
                        else:
                            return "failed"

                    elif op_type == "updated":
                        if op_content_hash and target_path.is_file():
                            cached_hash = replica_hashes.get(target_key)
                            if cached_hash and cached_hash == op_content_hash:
                                return "skipped_hash_match"
                        file_member = updated_members.get(rel_path)
                        if file_member:
                            file_content = tar.extractfile(file_member)
                            if file_content:
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                target_path.write_bytes(file_content.read())
                                if op_content_hash:
                                    replica_hashes[target_key] = op_content_hash
                                journal_entries_by_slug.setdefault(slug, []).append(
                                    _build_upsert_journal_entry(target_path, op_content_hash)
                                )
                                return "applied"
                            else:
                                return "failed"
                        else:
                            return "failed"

                    elif op_type == "deleted":
                        if target_path.exists():
                            target_path.unlink(missing_ok=True)
                        _cleanup_empty_dirs(target_path.parent, workspace_root)
                        replica_hashes.pop(target_key, None)
                        journal_entries_by_slug.setdefault(slug, []).append(
                            _build_delete_journal_entry(target_path)
                        )
                        return "applied"

                    elif op_type == "moved":
                        if safe_source_path and safe_source_path.exists():
                            target_path.parent.mkdir(parents=True, exist_ok=True)
                            if target_path.exists():
                                if target_path.is_dir():
                                    raise IsADirectoryError(
                                        f"[upload_service] move target is a directory: {target_path}"
                                    )
                                else:
                                    target_path.unlink()
                            shutil.move(str(safe_source_path), str(target_path))
                            _cleanup_empty_dirs(safe_source_path.parent, workspace_root)
                            source_key = _normalize_cache_key_path(str(safe_source_path))
                            moved_hash = replica_hashes.pop(source_key, None)
                            if op_content_hash:
                                replica_hashes[target_key] = op_content_hash
                            elif moved_hash:
                                replica_hashes[target_key] = moved_hash
                            move_entry_hash = op_content_hash or moved_hash
                            journal_entries_by_slug.setdefault(slug, []).extend(
                                [
                                    _build_delete_journal_entry(safe_source_path, move_entry_hash),
                                    _build_upsert_journal_entry(target_path, move_entry_hash),
                                ]
                            )
                            return "applied"
                        if _file_matches_hash(target_path, op_content_hash):
                            replica_hashes[target_key] = op_content_hash
                            if safe_source_path:
                                replica_hashes.pop(
                                    _normalize_cache_key_path(str(safe_source_path)), None
                                )
                                journal_entries_by_slug.setdefault(slug, []).append(
                                    _build_delete_journal_entry(
                                        safe_source_path, op_content_hash
                                    )
                                )
                            journal_entries_by_slug.setdefault(slug, []).append(
                                _build_upsert_journal_entry(target_path, op_content_hash)
                            )
                            return "skipped_hash_match"
                        # Remote uploads may not have the source file on the server (e.g. staging
                        # mirrors). In that case, clients can embed the destination content under
                        # files/moved/<dest>.
                        file_member = moved_members.get(rel_path)
                        if file_member:
                            file_content = tar.extractfile(file_member)
                            if file_content:
                                target_path.parent.mkdir(parents=True, exist_ok=True)
                                target_path.write_bytes(file_content.read())
                                if op_content_hash:
                                    replica_hashes[target_key] = op_content_hash
                                if safe_source_path:
                                    journal_entries_by_slug.setdefault(slug, []).append(
                                        _build_delete_journal_entry(safe_source_path, op_content_hash)
                                    )
                                journal_entries_by_slug.setdefault(slug, []).append(
                                    _build_upsert_journal_entry(target_path, op_content_hash)
                                )
                                return "applied"
                            return "failed"
                        return "failed"

                    else:
                        logger.warning(f"[upload_service] Unknown operation type: {op_type}")
                        return "failed"
                except Exception as e:
                    logger.debug(f"[upload_service] Failed to apply {op_type} to {rel_path} in {workspace_root}: {e}")
                    return "failed"

            for operation in operations:
                op_type = operation.get("operation")
                rel_path = operation.get("path")

                if not rel_path:
                    operations_count["skipped"] += 1
                    continue

                sanitized_path = _sanitize_operation_path(str(rel_path), replica_roots)
                if not sanitized_path:
                    logger.debug(
                        f"[upload_service] Skipping operation {op_type} for path {rel_path}: "
                        "appears to reference slug root directly.",
                    )
                    operations_count["skipped"] += 1
                    continue

                rel_path = sanitized_path

                replica_results: Dict[str, str] = {}
                for slug, root in replica_roots.items():
                    replica_results[slug] = _apply_operation_to_workspace(
                        slug,
                        root,
                        op_type,
                        rel_path,
                        operation,
                    )

                applied_any = any(result == "applied" for result in replica_results.values())
                skipped_hash_match = bool(replica_results) and all(
                    result == "skipped_hash_match" for result in replica_results.values()
                )
                success_all = all(result in {"applied", "skipped_hash_match"} for result in replica_results.values())
                if applied_any:
                    operations_count.setdefault(op_type, 0)
                    operations_count[op_type] += 1
                    if not success_all:
                        # A retry can skip replicas that already match while
                        # repairing the replica that failed this attempt.
                        operations_count["failed"] += 1
                        logger.debug(
                            f"[upload_service] Partial success for {op_type} {rel_path}: {replica_results}"
                        )
                elif skipped_hash_match:
                    operations_count["skipped"] += 1
                    operations_count["skipped_hash_match"] += 1
                else:
                    operations_count["failed"] += 1

        for slug, root in replica_roots.items():
            _enqueue_replica_journal_entries(
                workspace_root=root,
                slug=slug,
                entries=journal_entries_by_slug.get(slug, []),
            )
            # Flush updated replica hashes to disk (including empty caches)
            replica_hashes = replica_cache_hashes.get(slug, {})
            _flush_replica_cache_hashes(root, slug, replica_hashes)

        return operations_count

    except Exception as e:
        logger.error(f"Error processing delta bundle: {e}")
        raise
