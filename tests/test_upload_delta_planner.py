import hashlib
import importlib
import json
from pathlib import Path

import pytest


pytestmark = pytest.mark.unit


def test_plan_delta_upload_filters_remote_hash_matches(monkeypatch, tmp_path: Path):
    work_root = tmp_path / "work"
    slug = "repo-1234567890abcdef"
    replica_root = work_root / slug
    (replica_root / "src").mkdir(parents=True)
    (replica_root / "src" / "same.py").write_text("same\n", encoding="utf-8")
    (replica_root / "src" / "changed.py").write_text("old\n", encoding="utf-8")

    same_hash = "2c985b161217a952b7a410fd91495cebc349f520"
    changed_hash = "281bac2b704617e807850e07e54bae3469f6a2e7"
    cache_path = replica_root / ".codebase" / "cache.json"
    cache_path.parent.mkdir(parents=True)
    cache_path.write_text(
        json.dumps(
            {
                "file_hashes": {
                    str((replica_root / "src" / "same.py").resolve()): same_hash,
                    str((replica_root / "src" / "changed.py").resolve()): changed_hash,
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("WORK_DIR", str(work_root))
    monkeypatch.setenv("WORKSPACE_PATH", str(work_root))
    monkeypatch.setenv("WATCH_ROOT", str(work_root))
    monkeypatch.setenv("MULTI_REPO_MODE", "1")

    planner = importlib.import_module("scripts.upload_delta_bundle")
    planner = importlib.reload(planner)
    planner.WORK_DIR = str(work_root)

    result = planner.plan_delta_upload(
        workspace_path=str(replica_root),
        operations=[
            {
                "operation": "created",
                "path": "src/same.py",
                "size_bytes": 5,
                "content_hash": f"sha1:{same_hash}",
            },
            {
                "operation": "updated",
                "path": "src/changed.py",
                "size_bytes": 4,
                "content_hash": "sha1:new-hash",
            },
        ],
        file_hashes={
            "src/same.py": f"sha1:{same_hash}",
            "src/changed.py": "sha1:new-hash",
        },
    )

    assert result["needed_files"] == {
        "created": [],
        "updated": ["src/changed.py"],
        "moved": [],
    }
    assert result["operation_counts_preview"]["skipped_hash_match"] == 1
    assert result["needed_size_bytes"] == 4


def test_plan_delta_upload_requires_content_when_cache_entry_is_missing(monkeypatch, tmp_path: Path):
    work_root = tmp_path / "work"
    slug = "repo-1234567890abcdef"
    target = work_root / slug / "src" / "existing.py"
    target.parent.mkdir(parents=True)
    target.write_text("already there\n", encoding="utf-8")

    monkeypatch.setenv("WORK_DIR", str(work_root))
    monkeypatch.setenv("WORKSPACE_PATH", str(work_root))
    monkeypatch.setenv("WATCH_ROOT", str(work_root))
    monkeypatch.setenv("MULTI_REPO_MODE", "1")

    planner = importlib.import_module("scripts.upload_delta_bundle")
    planner = importlib.reload(planner)
    planner.WORK_DIR = str(work_root)
    content_hash = "sha1:" + hashlib.sha1(target.read_bytes()).hexdigest()

    result = planner.plan_delta_upload(
        workspace_path=str(work_root / slug),
        operations=[
            {"operation": "created", "path": "src/existing.py", "size_bytes": target.stat().st_size, "content_hash": content_hash}
        ],
        file_hashes={"src/existing.py": content_hash},
    )

    assert result["needed_files"]["created"] == ["src/existing.py"]
    assert result["diagnostics"]["missing_targets"] == 0
    assert "filesystem_hash_probes" not in result["diagnostics"]
    assert "filesystem_hash_matches" not in result["diagnostics"]


def test_plan_delta_upload_repairs_matching_cache_when_target_is_missing(
    monkeypatch, tmp_path: Path
):
    work_root = tmp_path / "work"
    slug = "repo-1234567890abcdef"
    replica_root = work_root / slug
    cache_path = replica_root / ".codebase" / "cache.json"
    cache_path.parent.mkdir(parents=True)
    content_hash = "sha1:" + hashlib.sha1(b"missing target\n").hexdigest()
    cache_path.write_text(
        json.dumps(
            {
                "file_hashes": {
                    str((replica_root / "src" / "missing.py").resolve()): content_hash,
                }
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("WORK_DIR", str(work_root))
    monkeypatch.setenv("WORKSPACE_PATH", str(work_root))
    monkeypatch.setenv("WATCH_ROOT", str(work_root))
    monkeypatch.setenv("MULTI_REPO_MODE", "1")

    planner = importlib.import_module("scripts.upload_delta_bundle")
    planner = importlib.reload(planner)
    planner.WORK_DIR = str(work_root)

    result = planner.plan_delta_upload(
        workspace_path=str(replica_root),
        operations=[
            {
                "operation": "created",
                "path": "src/missing.py",
                "size_bytes": len(b"missing target\n"),
                "content_hash": content_hash,
            }
        ],
        file_hashes={"src/missing.py": content_hash},
    )

    assert result["needed_files"]["created"] == ["src/missing.py"]
    assert result["operation_counts_preview"]["skipped_hash_match"] == 0
    assert result["diagnostics"]["missing_targets"] == 1


def test_plan_delta_upload_keeps_canonical_target_when_staging_has_no_old_replica(
    monkeypatch, tmp_path: Path
):
    work_root = tmp_path / "work"
    slug = "repo-1234567890abcdef"
    replica_root = work_root / slug
    replica_root.mkdir(parents=True)

    monkeypatch.setenv("WORK_DIR", str(work_root))
    monkeypatch.setenv("WORKSPACE_PATH", str(work_root))
    monkeypatch.setenv("WATCH_ROOT", str(work_root))
    monkeypatch.setenv("MULTI_REPO_MODE", "1")

    planner = importlib.import_module("scripts.upload_delta_bundle")
    planner = importlib.reload(planner)
    planner.WORK_DIR = str(work_root)
    monkeypatch.setattr(planner, "is_staging_enabled", lambda: True)
    monkeypatch.setattr(planner, "_extract_repo_name_from_path", lambda _path: slug)
    monkeypatch.setattr(
        planner,
        "get_collection_state_snapshot",
        lambda workspace_path=None, repo_name=None: {
            "active_repo_slug": slug,
            "serving_repo_slug": slug,
        },
    )

    result = planner.plan_delta_upload(
        workspace_path=str(replica_root),
        operations=[
            {
                "operation": "created",
                "path": "src/new.py",
                "size_bytes": 4,
                "content_hash": "sha1:new-hash",
            }
        ],
        file_hashes={"src/new.py": "sha1:new-hash"},
    )

    assert result["replica_targets"] == [slug]
    assert result["needed_files"]["created"] == ["src/new.py"]
