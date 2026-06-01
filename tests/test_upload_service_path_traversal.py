import io
import json
import os
import tarfile
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _disable_ambient_staging(monkeypatch):
    import scripts.upload_delta_bundle as us

    monkeypatch.setattr(us, "is_staging_enabled", lambda: False)


def _write_bundle(tmp_path: Path, operations: list[dict]) -> Path:
    bundle_path = tmp_path / "bundle.tar.gz"
    payload = json.dumps({"operations": operations}).encode("utf-8")

    with tarfile.open(bundle_path, "w:gz") as tar:
        info = tarfile.TarInfo(name="metadata/operations.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))

    return bundle_path


def _write_bundle_with_moved_file(tmp_path: Path, dest_path: str, content: bytes) -> Path:
    bundle_path = tmp_path / "bundle.tar.gz"
    operations = [{"operation": "moved", "path": dest_path, "source_path": "missing_src.txt"}]
    payload = json.dumps({"operations": operations}).encode("utf-8")

    with tarfile.open(bundle_path, "w:gz") as tar:
        info = tarfile.TarInfo(name="metadata/operations.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))

        file_info = tarfile.TarInfo(name=f"files/moved/{dest_path}")
        file_info.size = len(content)
        tar.addfile(file_info, io.BytesIO(content))

    return bundle_path


def _write_bundle_with_created_file(tmp_path: Path, rel_path: str, content: bytes) -> Path:
    bundle_path = tmp_path / "bundle.tar.gz"
    operations = [{"operation": "created", "path": rel_path}]
    payload = json.dumps({"operations": operations}).encode("utf-8")

    with tarfile.open(bundle_path, "w:gz") as tar:
        info = tarfile.TarInfo(name="metadata/operations.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))

        file_info = tarfile.TarInfo(name=f"files/created/{rel_path}")
        file_info.size = len(content)
        tar.addfile(file_info, io.BytesIO(content))

    return bundle_path


def _write_bundle_with_hash_metadata(
    tmp_path: Path,
    *,
    operations: list[dict],
    file_hashes: dict[str, str] | None = None,
    created_files: dict[str, bytes] | None = None,
    updated_files: dict[str, bytes] | None = None,
) -> Path:
    bundle_path = tmp_path / "bundle-hashes.tar.gz"
    payload = json.dumps({"operations": operations}).encode("utf-8")
    hashes_payload = json.dumps({"file_hashes": file_hashes or {}}).encode("utf-8")

    with tarfile.open(bundle_path, "w:gz") as tar:
        info = tarfile.TarInfo(name="metadata/operations.json")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))

        hashes_info = tarfile.TarInfo(name="metadata/hashes.json")
        hashes_info.size = len(hashes_payload)
        tar.addfile(hashes_info, io.BytesIO(hashes_payload))

        for rel_path, content in (created_files or {}).items():
            file_info = tarfile.TarInfo(name=f"files/created/{rel_path}")
            file_info.size = len(content)
            tar.addfile(file_info, io.BytesIO(content))

        for rel_path, content in (updated_files or {}).items():
            file_info = tarfile.TarInfo(name=f"files/updated/{rel_path}")
            file_info.size = len(content)
            tar.addfile(file_info, io.BytesIO(content))

    return bundle_path


def _write_repo_cache(work_dir: Path, slug: str, rel_path: str, file_hash: str) -> None:
    target = (work_dir / slug / rel_path).resolve()
    cache_path = work_dir / ".codebase" / "repos" / slug / "cache.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "file_hashes": {
                    str(target): {
                        "hash": file_hash,
                    }
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_process_delta_bundle_rejects_traversal_created(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    bundle = _write_bundle(
        tmp_path,
        [{"operation": "created", "path": "../../evil.txt"}],
    )

    with pytest.raises(ValueError, match="escapes workspace"):
        us.process_delta_bundle(
            workspace_path="/home/user/repo",
            bundle_path=bundle,
            manifest={"bundle_id": "b1"},
        )


def test_process_delta_bundle_moved_falls_back_to_tar_payload_when_source_missing(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    bundle = _write_bundle_with_moved_file(tmp_path, "dst.txt", b"moved-payload")

    counts = us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-moved"},
    )

    assert counts.get("moved") == 1
    assert (work_dir / slug / "dst.txt").read_bytes() == b"moved-payload"


def test_process_delta_bundle_slugged_workspace_creates_marker(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    bundle = _write_bundle_with_created_file(tmp_path, "a.txt", b"hello")

    counts = us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b1"},
    )

    assert counts.get("created") == 1
    assert (work_dir / slug / "a.txt").exists()
    assert not (work_dir / slug / slug / "a.txt").exists()
    assert (work_dir / ".codebase" / "repos" / slug / ".ctxce_managed_upload").exists()


def test_process_delta_bundle_mirrors_to_old_slug_when_staging_active(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    canonical_slug = "repo1-0123456789abcdef"
    old_slug = f"{canonical_slug}_old"

    monkeypatch.setenv("CTXCE_STAGING_ENABLED", "1")
    monkeypatch.setattr(us, "is_staging_enabled", lambda: True)
    monkeypatch.setattr(us, "_extract_repo_name_from_path", lambda path: canonical_slug)
    monkeypatch.setattr(
        us,
        "get_collection_state_snapshot",
        lambda workspace_path=None, repo_name=None: {
            "serving_repo_slug": old_slug,
            "active_repo_slug": canonical_slug,
        },
    )

    bundle = _write_bundle_with_created_file(tmp_path, "src/file.txt", b"payload")

    counts = us.process_delta_bundle(
        workspace_path="/work/random",
        bundle_path=bundle,
        manifest={"bundle_id": "b-dual"},
    )

    assert counts.get("created") == 1

    for slug in (canonical_slug, old_slug):
        target = work_dir / slug / "src" / "file.txt"
        assert target.exists(), f"expected write for {slug}"
        marker = work_dir / ".codebase" / "repos" / slug / ".ctxce_managed_upload"
        assert marker.exists(), f"expected marker for {slug}"


def test_process_delta_bundle_rejects_absolute_paths(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    bundle = _write_bundle(
        tmp_path,
        [{"operation": "created", "path": "/etc/passwd"}],
    )

    with pytest.raises(ValueError, match="Absolute paths"):
        us.process_delta_bundle(
            workspace_path="/home/user/repo",
            bundle_path=bundle,
            manifest={"bundle_id": "b1"},
        )


def test_process_delta_bundle_rejects_traversal_moved_source(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    bundle = _write_bundle(
        tmp_path,
        [
            {
                "operation": "moved",
                "path": "dst.txt",
                "source_path": "../../escape.txt",
            }
        ],
    )

    with pytest.raises(ValueError, match="escapes workspace"):
        us.process_delta_bundle(
            workspace_path="/home/user/repo",
            bundle_path=bundle,
            manifest={"bundle_id": "b1"},
        )


def test_process_delta_bundle_skips_created_write_when_server_hash_matches(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    rel_path = "src/file.txt"
    content = b"same-content"
    file_hash = "sha1:efb5d7d4d38013264f2c00fceeb401f8c8d77d9f"

    target = work_dir / slug / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    os.utime(target, ns=(1_000_000_000, 1_000_000_000))
    before_mtime_ns = target.stat().st_mtime_ns
    _write_repo_cache(work_dir, slug, rel_path, file_hash)

    bundle = _write_bundle_with_hash_metadata(
        tmp_path,
        operations=[
            {
                "operation": "created",
                "path": rel_path,
                "content_hash": file_hash,
            }
        ],
        file_hashes={rel_path: file_hash},
        created_files={rel_path: content},
    )

    counts = us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-skip-created"},
    )

    assert counts.get("created") == 0
    assert counts.get("skipped") == 1
    assert counts.get("skipped_hash_match") == 1
    assert target.read_bytes() == content
    assert target.stat().st_mtime_ns == before_mtime_ns


def test_process_delta_bundle_uses_hashes_metadata_for_updated_skip(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    rel_path = "src/keep.txt"
    content = b"existing-content"
    file_hash = "sha1:2910e29d6f6d3d2f01f8cc52ec386a4936ca9d2f"

    target = work_dir / slug / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    os.utime(target, ns=(2_000_000_000, 2_000_000_000))
    before_mtime_ns = target.stat().st_mtime_ns
    _write_repo_cache(work_dir, slug, rel_path, file_hash)

    bundle = _write_bundle_with_hash_metadata(
        tmp_path,
        operations=[
            {
                "operation": "updated",
                "path": rel_path,
            }
        ],
        file_hashes={rel_path: file_hash},
        updated_files={rel_path: content},
    )

    counts = us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-skip-updated"},
    )

    assert counts.get("updated") == 0
    assert counts.get("skipped") == 1
    assert counts.get("skipped_hash_match") == 1
    assert target.read_bytes() == content
    assert target.stat().st_mtime_ns == before_mtime_ns


def test_normalize_hash_value_strips_algorithm_prefixes():
    import scripts.upload_delta_bundle as us

    assert us._normalize_hash_value("sha1:ABCDEF") == "abcdef"
    assert us._normalize_hash_value("md5:ABCDEF") == "abcdef"
    assert us._normalize_hash_value("sha256:ABCDEF") == "abcdef"
    assert us._normalize_hash_value("ABCDEF") == "abcdef"


def test_process_delta_bundle_uses_first_marker_match_for_created_members(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    rel_path = "nested/files/created/path.txt"
    content = b"marker-safe"
    bundle = _write_bundle_with_created_file(tmp_path, rel_path, content)

    counts = us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-created-marker"},
    )

    assert counts.get("created") == 1
    assert (work_dir / slug / rel_path).read_bytes() == content


def test_process_delta_bundle_deleted_prunes_empty_parent_dirs(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    rel_path = "dev-workspace/nested/stale.py"
    target = work_dir / slug / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("stale\n", encoding="utf-8")

    bundle = _write_bundle(
        tmp_path,
        [{"operation": "deleted", "path": rel_path}],
    )

    counts = us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-delete-prune"},
    )

    assert counts.get("deleted") == 1
    assert not target.exists()
    assert not (work_dir / slug / "dev-workspace" / "nested").exists()
    assert not (work_dir / slug / "dev-workspace").exists()
    assert (work_dir / slug).exists()


def test_process_delta_bundle_moved_prunes_empty_source_parent_dirs(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    src = work_dir / slug / "dev-workspace" / "nested" / "from.py"
    dest_rel_path = "dest/to.py"
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text("payload\n", encoding="utf-8")

    bundle = _write_bundle(
        tmp_path,
        [{"operation": "moved", "path": dest_rel_path, "source_path": "dev-workspace/nested/from.py"}],
    )

    counts = us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-move-prune"},
    )

    assert counts.get("moved") == 1
    assert not src.exists()
    assert (work_dir / slug / dest_rel_path).read_text(encoding="utf-8") == "payload\n"
    assert not (work_dir / slug / "dev-workspace" / "nested").exists()
    assert not (work_dir / slug / "dev-workspace").exists()
    assert (work_dir / slug).exists()


def test_process_delta_bundle_does_not_sweep_stranded_empty_dirs_without_file_ops(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))
    slug = "repo-0123456789abcdef"
    stranded = work_dir / slug / "dev-workspace" / "nested" / "empty"
    stranded.mkdir(parents=True, exist_ok=True)

    bundle = _write_bundle(tmp_path, [])

    counts = us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-sweep-empty"},
    )

    assert counts == {
        "created": 0,
        "updated": 0,
        "deleted": 0,
        "moved": 0,
        "skipped": 0,
        "skipped_hash_match": 0,
        "failed": 0,
    }
    assert stranded.exists()
    assert (work_dir / slug / "dev-workspace").exists()
    assert (work_dir / slug).exists()


def test_process_delta_bundle_skips_broad_empty_dir_sweep_when_disabled(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))
    monkeypatch.setenv("CTXCE_UPLOAD_EMPTY_DIR_SWEEP", "0")

    slug = "repo-0123456789abcdef"
    stranded = work_dir / slug / "dev-workspace" / "nested" / "empty"
    stranded.mkdir(parents=True, exist_ok=True)

    bundle = _write_bundle(tmp_path, [])

    us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-sweep-disabled"},
    )

    assert stranded.exists()


def test_process_delta_bundle_skips_broad_empty_dir_sweep_when_recent(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))
    slug = "repo-0123456789abcdef"
    stranded = work_dir / slug / "dev-workspace" / "nested" / "empty"
    stranded.mkdir(parents=True, exist_ok=True)

    bundle = _write_bundle(tmp_path, [])

    us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-sweep-recent"},
    )

    assert stranded.exists()


def test_process_delta_bundle_preserves_protected_top_level_dirs_when_empty(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))
    monkeypatch.setenv("CTXCE_UPLOAD_EMPTY_DIR_SWEEP", "1")
    monkeypatch.setenv("CTXCE_UPLOAD_EMPTY_DIR_SWEEP_INTERVAL_SECONDS", "0")

    slug = "repo-0123456789abcdef"
    protected = work_dir / slug / ".remote-git"
    protected.mkdir(parents=True, exist_ok=True)

    bundle = _write_bundle(tmp_path, [])

    us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-protected-empty"},
    )

    assert protected.exists()


def test_process_delta_bundle_preserves_nested_dirs_under_protected_top_level(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))
    monkeypatch.setenv("CTXCE_UPLOAD_EMPTY_DIR_SWEEP", "1")
    monkeypatch.setenv("CTXCE_UPLOAD_EMPTY_DIR_SWEEP_INTERVAL_SECONDS", "0")

    slug = "repo-0123456789abcdef"
    protected_nested = work_dir / slug / ".codebase" / "repos" / "empty"
    protected_nested.mkdir(parents=True, exist_ok=True)

    bundle = _write_bundle(tmp_path, [])

    us.process_delta_bundle(
        workspace_path=f"/work/{slug}",
        bundle_path=bundle,
        manifest={"bundle_id": "b-protected-nested-empty"},
    )

    assert protected_nested.exists()


def test_plan_delta_upload_skips_matching_created_files(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    rel_path = "src/file.txt"
    content = b"same-content"
    file_hash = "sha1:efb5d7d4d38013264f2c00fceeb401f8c8d77d9f"

    target = work_dir / slug / rel_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    _write_repo_cache(work_dir, slug, rel_path, file_hash)

    plan = us.plan_delta_upload(
        workspace_path=f"/work/{slug}",
        operations=[
            {
                "operation": "created",
                "path": rel_path,
                "content_hash": file_hash,
                "size_bytes": len(content),
            }
        ],
        file_hashes={rel_path: file_hash},
    )

    assert plan["needed_files"]["created"] == []
    assert plan["operation_counts_preview"]["skipped_hash_match"] == 1
    assert plan["needed_size_bytes"] == 0


def test_plan_delta_upload_marks_updated_file_needed_when_hash_missing(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    rel_path = "src/keep.txt"
    file_hash = "sha1:2910e29d6f6d3d2f01f8cc52ec386a4936ca9d2f"

    plan = us.plan_delta_upload(
        workspace_path=f"/work/{slug}",
        operations=[
            {
                "operation": "updated",
                "path": rel_path,
                "content_hash": file_hash,
                "size_bytes": 17,
            }
        ],
        file_hashes={rel_path: file_hash},
    )

    assert plan["needed_files"]["updated"] == [rel_path]
    assert plan["operation_counts_preview"]["updated"] == 1
    assert plan["needed_size_bytes"] == 17


def test_plan_delta_upload_skips_move_content_when_source_exists_on_server(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    source_rel = "src/old.py"
    dest_rel = "src/new.py"
    source = work_dir / slug / source_rel
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("print('move')\n", encoding="utf-8")

    plan = us.plan_delta_upload(
        workspace_path=f"/work/{slug}",
        operations=[
            {
                "operation": "moved",
                "path": dest_rel,
                "source_path": source_rel,
                "content_hash": "sha1:abc123",
                "size_bytes": 12,
            }
        ],
        file_hashes={dest_rel: "sha1:abc123"},
    )

    assert plan["needed_files"]["moved"] == []
    assert plan["operation_counts_preview"]["moved"] == 1
    assert plan["needed_size_bytes"] == 0


def test_plan_delta_upload_marks_move_needed_when_source_path_is_invalid(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    dest_rel = "src/new.py"

    plan = us.plan_delta_upload(
        workspace_path=f"/work/{slug}",
        operations=[
            {
                "operation": "moved",
                "path": dest_rel,
                "source_path": "../escape.py",
                "content_hash": "sha1:abc123",
                "size_bytes": 12,
            }
        ],
        file_hashes={dest_rel: "sha1:abc123"},
    )

    assert plan["needed_files"]["moved"] == [dest_rel]
    assert plan["operation_counts_preview"]["moved"] == 1
    assert plan["needed_size_bytes"] == 12


def test_apply_delta_operations_moves_file_without_bundle(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))

    slug = "repo-0123456789abcdef"
    source_rel = "src/old.py"
    dest_rel = "src/new.py"
    source = work_dir / slug / source_rel
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("print('move')\n", encoding="utf-8")

    counts = us.apply_delta_operations(
        workspace_path=f"/work/{slug}",
        operations=[
            {
                "operation": "moved",
                "path": dest_rel,
                "source_path": source_rel,
                "content_hash": "sha1:abc123",
            }
        ],
        file_hashes={dest_rel: "sha1:abc123"},
    )

    assert counts["moved"] == 1
    assert not source.exists()
    assert (work_dir / slug / dest_rel).exists()


def test_apply_delta_operations_raises_clear_error_when_no_replica_roots(tmp_path, monkeypatch):
    import scripts.upload_delta_bundle as us

    work_dir = tmp_path / "work"
    work_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(us, "WORK_DIR", str(work_dir))
    monkeypatch.setattr(us, "_resolve_replica_roots", lambda workspace_path: {})

    with pytest.raises(ValueError, match="No replica roots available"):
        us.apply_delta_operations(
            workspace_path="/work/repo",
            operations=[],
            file_hashes={},
        )
