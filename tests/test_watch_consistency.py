#!/usr/bin/env python3
import importlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest


pytestmark = pytest.mark.unit


@pytest.fixture
def capture_list_workspaces():
    captured = {}

    def fake_list_workspaces(search_root=None, use_qdrant_fallback=True):
        captured["search_root"] = search_root
        captured["use_qdrant_fallback"] = use_qdrant_fallback
        return []

    return captured, fake_list_workspaces


def test_run_consistency_audit_scans_from_watcher_root(
    monkeypatch, tmp_path, capture_list_workspaces
):
    mod = importlib.import_module("scripts.watch_index_core.consistency")
    captured, fake_list_workspaces = capture_list_workspaces

    monkeypatch.setattr(mod, "list_workspaces", fake_list_workspaces)
    monkeypatch.setattr(mod, "_consistency_audit_enabled", lambda: True)

    mod.run_consistency_audit(MagicMock(), tmp_path)

    assert "search_root" in captured and "use_qdrant_fallback" in captured
    assert Path(captured["search_root"]).resolve() == Path(tmp_path).resolve()
    assert captured["use_qdrant_fallback"] is False


def test_run_empty_dir_sweep_maintenance_scans_from_watcher_root(
    monkeypatch, tmp_path, capture_list_workspaces
):
    mod = importlib.import_module("scripts.watch_index_core.consistency")
    captured, fake_list_workspaces = capture_list_workspaces

    monkeypatch.setattr(mod, "list_workspaces", fake_list_workspaces)
    monkeypatch.setattr(mod, "_empty_dir_sweep_enabled", lambda: True)

    mod.run_empty_dir_sweep_maintenance(tmp_path)

    assert "search_root" in captured
    assert Path(captured["search_root"]).resolve() == Path(tmp_path).resolve()
    assert captured.get("use_qdrant_fallback") is False


def test_consistency_audit_skips_repairs_when_scan_is_truncated(monkeypatch, tmp_path):
    mod = importlib.import_module("scripts.watch_index_core.consistency")

    workspace_root = tmp_path / "repo"
    workspace_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        mod,
        "list_workspaces",
        lambda *a, **k: [{"workspace_path": str(workspace_root)}],
    )
    monkeypatch.setattr(mod, "_consistency_audit_enabled", lambda: True)
    monkeypatch.setattr(mod, "_should_run_consistency_audit", lambda *a, **k: True)
    monkeypatch.setattr(
        mod,
        "get_collection_state_snapshot",
        lambda *a, **k: {"active_collection": "coll"},
    )
    monkeypatch.setattr(mod, "_extract_repo_name_from_path", lambda *_: "repo")
    monkeypatch.setattr(mod, "_load_cached_hashes", lambda *a, **k: {})
    monkeypatch.setattr(
        mod,
        "_scan_indexable_fs_paths",
        lambda *a, **k: ({str(workspace_root / "a.py")}, True),
    )
    monkeypatch.setattr(
        mod,
        "_load_indexed_paths_for_collection",
        lambda *a, **k: ({str(workspace_root / "ghost.py")}, False),
    )
    monkeypatch.setattr(mod.idx, "_Excluder", lambda *_: MagicMock())

    enqueue_mock = MagicMock(return_value=(0, 0))
    record_mock = MagicMock()
    monkeypatch.setattr(mod, "_enqueue_consistency_repairs", enqueue_mock)
    monkeypatch.setattr(mod, "_record_consistency_audit", record_mock)

    mod.run_consistency_audit(MagicMock(), tmp_path)

    enqueue_mock.assert_not_called()
    record_mock.assert_called_once()
    summary = record_mock.call_args.args[2]
    assert summary["fs_scan_truncated"] is True
    assert summary["qdrant_scan_truncated"] is False
    assert summary["repair_skipped_due_to_truncation"] is True
    assert summary["stale_in_qdrant_count"] == 0
    assert summary["missing_in_qdrant_count"] == 0
