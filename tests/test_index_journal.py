#!/usr/bin/env python3
import importlib
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest


pytestmark = pytest.mark.unit


@pytest.fixture
def ws_module(monkeypatch, tmp_path):
    ws_root = tmp_path / "work"
    ws_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("WORKSPACE_PATH", str(ws_root))
    monkeypatch.setenv("WATCH_ROOT", str(ws_root))
    monkeypatch.delenv("MULTI_REPO_MODE", raising=False)
    ws = importlib.import_module("scripts.workspace_state")
    return importlib.reload(ws)


def test_index_journal_roundtrip(ws_module, tmp_path):
    repo_name = "repo-1234567890abcdef"
    file_path = tmp_path / "work" / repo_name / "src" / "app.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    ws_module.upsert_index_journal_entries(
        [
            {"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"},
            {"path": str(file_path.with_name("old.py")), "op_type": "delete"},
        ],
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )

    pending = [
        str(e["path"])
        for e in ws_module.list_pending_index_journal_entries(
            workspace_path=str(tmp_path / "work" / repo_name),
            repo_name=repo_name,
        )
    ]
    assert str(file_path.resolve()) in pending
    assert str((file_path.with_name("old.py")).resolve()) in pending

    ws_module.update_index_journal_entry_status(
        str(file_path),
        status="done",
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )
    pending_after = [
        str(e["path"])
        for e in ws_module.list_pending_index_journal_entries(
            workspace_path=str(tmp_path / "work" / repo_name),
            repo_name=repo_name,
        )
    ]
    assert str(file_path.resolve()) not in pending_after
    assert str((file_path.with_name("old.py")).resolve()) in pending_after


def test_index_journal_entries_include_operation_types(ws_module, tmp_path):
    repo_name = "repo-1234567890abcdef"
    file_path = tmp_path / "work" / repo_name / "src" / "entry.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    ws_module.upsert_index_journal_entries(
        [
            {"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"},
            {"path": str(file_path.with_name("gone.py")), "op_type": "delete"},
        ],
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )

    entries = ws_module.list_pending_index_journal_entries(
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )
    by_path = {entry["path"]: entry for entry in entries}
    assert by_path[str(file_path.resolve())]["op_type"] == "upsert"
    assert by_path[str((file_path.with_name("gone.py")).resolve())]["op_type"] == "delete"


def test_index_journal_clear_entries(ws_module, tmp_path):
    repo_name = "repo-1234567890abcdef"
    file_path = tmp_path / "work" / repo_name / "src" / "entry.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    ws_module.upsert_index_journal_entries(
        [
            {"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"},
            {"path": str(file_path.with_name("gone.py")), "op_type": "delete"},
        ],
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )

    removed = ws_module.clear_index_journal_entries(
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )

    assert removed == 2
    assert (
        ws_module.list_pending_index_journal_entries(
            workspace_path=str(tmp_path / "work" / repo_name),
            repo_name=repo_name,
        )
        == []
    )


def test_index_journal_bulk_status_updates_once(ws_module, monkeypatch, tmp_path):
    repo_name = "repo-1234567890abcdef"
    repo_root = tmp_path / "work" / repo_name
    repo_root.mkdir(parents=True, exist_ok=True)
    paths = [repo_root / f"src/{idx}.py" for idx in range(3)]

    ws_module.upsert_index_journal_entries(
        [{"path": str(path), "op_type": "upsert"} for path in paths],
        workspace_path=str(repo_root),
        repo_name=repo_name,
    )

    original_update = ws_module._update_index_journal
    calls = []

    def counted_update(*args, **kwargs):
        calls.append(1)
        return original_update(*args, **kwargs)

    monkeypatch.setattr(ws_module, "_update_index_journal", counted_update)
    ws_module.update_index_journal_entries_status(
        [{"path": str(path), "status": "done"} for path in paths],
        workspace_path=str(repo_root),
        repo_name=repo_name,
    )

    assert len(calls) == 1
    assert ws_module.list_pending_index_journal_entries(
        workspace_path=str(repo_root), repo_name=repo_name
    ) == []


def test_index_journal_summary_reports_retryable_entries(ws_module, tmp_path):
    repo_name = "repo-1234567890abcdef"
    repo_root = tmp_path / "work" / repo_name
    repo_root.mkdir(parents=True, exist_ok=True)
    pending = repo_root / "src/pending.py"
    failed = repo_root / "src/failed.py"

    ws_module.upsert_index_journal_entries(
        [
            {"path": str(pending), "op_type": "upsert"},
            {"path": str(failed), "op_type": "delete"},
        ],
        workspace_path=str(repo_root),
        repo_name=repo_name,
    )
    ws_module.update_index_journal_entry_status(
        str(failed),
        status="failed",
        error="qdrant unavailable",
        workspace_path=str(repo_root),
        repo_name=repo_name,
        remove_on_done=False,
    )

    summary = ws_module.get_index_journal_summary(
        workspace_path=str(repo_root), repo_name=repo_name
    )

    assert summary["total"] == 2
    assert summary["retryable"] == 2
    assert summary["outstanding"] == 2
    assert summary["counts"]["pending"] == 1
    assert summary["counts"]["failed"] == 1
    assert summary["sample_errors"] == [
        {"path": str(failed.resolve()), "error": "qdrant unavailable"}
    ]


def test_index_journal_summary_counts_in_progress_as_outstanding(ws_module, tmp_path):
    repo_name = "repo-1234567890abcdef"
    repo_root = tmp_path / "work" / repo_name
    repo_root.mkdir(parents=True, exist_ok=True)
    path = repo_root / "src/in_progress.py"

    ws_module.upsert_index_journal_entries(
        [{"path": str(path), "op_type": "upsert"}],
        workspace_path=str(repo_root),
        repo_name=repo_name,
    )
    ws_module.update_index_journal_entry_status(
        str(path),
        status="in_progress",
        workspace_path=str(repo_root),
        repo_name=repo_name,
        remove_on_done=False,
    )

    summary = ws_module.get_index_journal_summary(
        workspace_path=str(repo_root), repo_name=repo_name
    )

    assert summary["counts"]["in_progress"] == 1
    assert summary["retryable"] == 0
    assert summary["outstanding"] == 1


def test_index_journal_unknown_status_is_reported_but_not_retried(ws_module, tmp_path):
    repo_name = "repo-1234567890abcdef"
    repo_root = tmp_path / "work" / repo_name
    path = repo_root / "src" / "unknown.py"
    journal_path = ws_module._get_index_journal_path(str(repo_root), repo_name)
    journal_path.parent.mkdir(parents=True, exist_ok=True)
    journal_path.write_text(
        json.dumps(
            {
                "version": 1,
                "operations": {
                    str(path.resolve()): {
                        "path": str(path.resolve()),
                        "op_type": "upsert",
                        "status": "mystery",
                        "attempts": 0,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    summary = ws_module.get_index_journal_summary(
        workspace_path=str(repo_root), repo_name=repo_name
    )

    assert summary["counts"]["unknown"] == 1
    assert summary["retryable"] == 0
    assert summary["outstanding"] == 1
    assert ws_module.list_pending_index_journal_entries(
        workspace_path=str(repo_root), repo_name=repo_name
    ) == []


def test_index_journal_aggregates_repo_scoped_entries(ws_module, tmp_path):
    repo_name = "repo-1234567890abcdef"
    file_path = tmp_path / "work" / repo_name / "src" / "x.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    ws_module.upsert_index_journal_entries(
        [{"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"}],
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )

    pending = [
        str(e["path"])
        for e in ws_module.list_pending_index_journal_entries(
            workspace_path=str(tmp_path / "work")
        )
    ]
    assert str(file_path.resolve()) in pending


@pytest.mark.parametrize("repo_name", ["repo-1234567890abcdef", "frontend"])
def test_index_journal_aggregates_repo_scoped_entries_in_multi_repo_mode(
    monkeypatch, tmp_path, repo_name
):
    ws_root = tmp_path / "work"
    ws_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("WORKSPACE_PATH", str(ws_root))
    monkeypatch.setenv("WATCH_ROOT", str(ws_root))
    monkeypatch.setenv("MULTI_REPO_MODE", "1")
    ws_module = importlib.import_module("scripts.workspace_state")
    ws_module = importlib.reload(ws_module)

    file_name = "app.ts" if repo_name == "frontend" else "multi.py"
    file_path = ws_root / repo_name / "src" / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)

    ws_module.upsert_index_journal_entries(
        [{"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"}],
        workspace_path=str(ws_root / repo_name),
        repo_name=repo_name,
    )

    pending = [
        str(e["path"])
        for e in ws_module.list_pending_index_journal_entries(workspace_path=str(ws_root))
    ]
    assert str(file_path.resolve()) in pending


def test_index_journal_discovery_ignores_arbitrary_workspace_directories(
    monkeypatch, tmp_path
):
    ws_root = tmp_path / "work"
    ws_root.mkdir(parents=True, exist_ok=True)
    (ws_root / "logs").mkdir()
    (ws_root / "logs" / "large.log").write_text("noise\n", encoding="utf-8")
    repo_name = "frontend"
    repo_state = ws_root / ".codebase" / "repos" / repo_name
    repo_state.mkdir(parents=True, exist_ok=True)
    (repo_state / "index_journal.json").write_text(
        json.dumps({"operations": {}}), encoding="utf-8"
    )

    monkeypatch.setenv("WORKSPACE_PATH", str(ws_root))
    monkeypatch.setenv("WATCH_ROOT", str(ws_root))
    monkeypatch.setenv("MULTI_REPO_MODE", "1")
    ws_module = importlib.import_module("scripts.workspace_state")
    ws_module = importlib.reload(ws_module)

    discovered = ws_module._discover_journal_repositories(str(ws_root))

    assert discovered == [(repo_name, None)]


def test_index_journal_aggregates_split_watch_and_metadata_roots(monkeypatch, tmp_path):
    watch_root = tmp_path / "work"
    metadata_root = tmp_path / "metadata"
    repo_name = "Context-Engine-41e67959950c8ab3"
    file_path = watch_root / repo_name / "src" / "split.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("WATCH_ROOT", str(watch_root))
    monkeypatch.setenv("WORK_DIR", str(watch_root))
    monkeypatch.setenv("CTXCE_METADATA_ROOT", str(metadata_root))
    monkeypatch.setenv("MULTI_REPO_MODE", "1")
    ws_module = importlib.import_module("scripts.workspace_state")
    ws_module = importlib.reload(ws_module)

    ws_module.upsert_index_journal_entries(
        [{"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"}],
        workspace_path=str(file_path.parent.parent),
        repo_name=repo_name,
    )

    pending = [
        str(e["path"])
        for e in ws_module.list_pending_index_journal_entries(workspace_path=str(watch_root))
    ]
    assert str(file_path.resolve()) in pending

    ws_module.update_index_journal_entry_status(
        str(file_path),
        status="done",
        workspace_path=str(file_path.parent.parent),
        repo_name=repo_name,
    )
    pending_after = [
        str(e["path"])
        for e in ws_module.list_pending_index_journal_entries(workspace_path=str(watch_root))
    ]
    assert str(file_path.resolve()) not in pending_after


def test_index_journal_file_is_group_writable(ws_module, tmp_path):
    repo_name = "repo-1234567890abcdef"
    file_path = tmp_path / "work" / repo_name / "src" / "perm.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)

    ws_module.upsert_index_journal_entries(
        [{"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"}],
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )

    journal_path = ws_module._get_index_journal_path(
        str(tmp_path / "work" / repo_name), repo_name
    )
    assert journal_path.exists()
    assert oct(journal_path.stat().st_mode & 0o777) == "0o666"


def test_index_journal_failed_entry_respects_retry_delay(ws_module, monkeypatch, tmp_path):
    repo_name = "repo-1234567890abcdef"
    file_path = tmp_path / "work" / repo_name / "src" / "retry.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("INDEX_JOURNAL_RETRY_DELAY_SECS", "60")

    ws_module.upsert_index_journal_entries(
        [{"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"}],
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )
    ws_module.update_index_journal_entry_status(
        str(file_path),
        status="failed",
        error="boom",
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
        remove_on_done=False,
    )

    pending = [
        str(e["path"])
        for e in ws_module.list_pending_index_journal_entries(
            workspace_path=str(tmp_path / "work" / repo_name),
            repo_name=repo_name,
        )
    ]
    assert str(file_path.resolve()) not in pending


def test_index_journal_failed_entry_honors_max_attempts(ws_module, monkeypatch, tmp_path):
    repo_name = "repo-1234567890abcdef"
    file_path = tmp_path / "work" / repo_name / "src" / "retry2.py"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("INDEX_JOURNAL_RETRY_DELAY_SECS", "0")
    monkeypatch.setenv("INDEX_JOURNAL_MAX_ATTEMPTS", "1")

    ws_module.upsert_index_journal_entries(
        [{"path": str(file_path), "op_type": "upsert", "content_hash": "abc123"}],
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
    )
    ws_module.update_index_journal_entry_status(
        str(file_path),
        status="failed",
        error="boom",
        workspace_path=str(tmp_path / "work" / repo_name),
        repo_name=repo_name,
        remove_on_done=False,
    )

    pending = [
        str(e["path"])
        for e in ws_module.list_pending_index_journal_entries(
            workspace_path=str(tmp_path / "work" / repo_name),
            repo_name=repo_name,
        )
    ]
    assert str(file_path.resolve()) not in pending


def test_processor_delete_marks_journal_done(monkeypatch, tmp_path):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    missing = tmp_path / "missing.py"
    assert not missing.exists()

    monkeypatch.setattr(proc_mod, "_detect_repo_for_file", lambda p: tmp_path)
    monkeypatch.setattr(proc_mod, "_get_collection_for_file", lambda p: "coll")
    monkeypatch.setattr(proc_mod, "_set_status_indexing", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "get_workspace_state", lambda *a, **k: {})
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: False)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_extract_repo_name_from_path", lambda *_: "repo")
    monkeypatch.setattr(proc_mod, "remove_cached_file", lambda *a, **k: None)

    delete_mock = MagicMock()
    graph_delete_mock = MagicMock()
    journal_mock = MagicMock()
    monkeypatch.setattr(proc_mod.idx, "delete_points_by_path", delete_mock)
    monkeypatch.setattr(proc_mod.idx, "delete_graph_edges_by_path", graph_delete_mock)
    monkeypatch.setattr(proc_mod, "_verify_delete_committed", lambda *a, **k: True)
    monkeypatch.setattr(proc_mod, "_verify_graph_delete_committed", lambda *a, **k: True)
    monkeypatch.setattr(proc_mod, "update_index_journal_entries_status", journal_mock)

    proc_mod._process_paths(
        [missing],
        client=MagicMock(),
        model=None,
        vector_name="vec",
        model_dim=1,
        workspace_path=str(tmp_path),
    )

    delete_mock.assert_called_once()
    assert graph_delete_mock.call_count == 2
    assert graph_delete_mock.call_args_list[0].kwargs["repo"] == "repo"
    assert graph_delete_mock.call_args_list[1].kwargs["repo"] is None
    journal_mock.assert_called_once()
    assert journal_mock.call_args.args[0][0]["status"] == "done"


def test_processor_honors_delete_journal_for_existing_file(monkeypatch, tmp_path):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    existing = tmp_path / "present.py"
    existing.write_text("print('x')\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod, "_detect_repo_for_file", lambda p: tmp_path)
    monkeypatch.setattr(proc_mod, "_get_collection_for_file", lambda p: "coll")
    monkeypatch.setattr(proc_mod, "_set_status_indexing", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "get_workspace_state", lambda *a, **k: {})
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: False)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_extract_repo_name_from_path", lambda *_: "repo")
    monkeypatch.setattr(proc_mod, "remove_cached_file", lambda *a, **k: None)
    monkeypatch.setattr(
        proc_mod,
        "list_pending_index_journal_entries",
        lambda *a, **k: [{"path": str(existing.resolve()), "op_type": "delete"}],
    )

    delete_mock = MagicMock()
    graph_delete_mock = MagicMock()
    journal_mock = MagicMock()
    monkeypatch.setattr(proc_mod.idx, "delete_points_by_path", delete_mock)
    monkeypatch.setattr(proc_mod.idx, "delete_graph_edges_by_path", graph_delete_mock)
    monkeypatch.setattr(proc_mod, "_verify_delete_committed", lambda *a, **k: True)
    monkeypatch.setattr(proc_mod, "_verify_graph_delete_committed", lambda *a, **k: True)
    monkeypatch.setattr(proc_mod, "update_index_journal_entries_status", journal_mock)

    proc_mod._process_paths(
        [existing],
        client=MagicMock(),
        model=None,
        vector_name="vec",
        model_dim=1,
        workspace_path=str(tmp_path),
    )

    delete_mock.assert_called_once()
    assert graph_delete_mock.call_count == 2
    assert graph_delete_mock.call_args_list[0].kwargs["repo"] == "repo"
    assert graph_delete_mock.call_args_list[1].kwargs["repo"] is None
    journal_mock.assert_called_once()
    assert journal_mock.call_args.args[0][0]["status"] == "done"


def test_processor_relinks_move_journal_before_delete(monkeypatch, tmp_path):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    src = tmp_path / "src.py"
    dest = tmp_path / "dest.py"
    dest.write_text("print('dest')\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod, "_detect_repo_for_file", lambda p: tmp_path)
    monkeypatch.setattr(proc_mod, "_get_collection_for_file", lambda p: "coll")
    monkeypatch.setattr(proc_mod, "_set_status_indexing", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "get_workspace_state", lambda *a, **k: {})
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: False)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_extract_repo_name_from_path", lambda *_: "repo")
    monkeypatch.setattr(proc_mod, "remove_cached_file", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "set_cached_file_hash", lambda *a, **k: None)
    monkeypatch.setattr(
        proc_mod,
        "list_pending_index_journal_entries",
        lambda *a, **k: [
            {"path": str(src.resolve()), "op_type": "delete", "content_hash": "cafebabe"},
            {"path": str(dest.resolve()), "op_type": "upsert", "content_hash": "cafebabe"},
        ],
    )

    rename_mock = MagicMock(return_value=(3, "cafebabe"))
    delete_mock = MagicMock()
    journal_mock = MagicMock()
    monkeypatch.setattr(proc_mod, "_rename_in_store", rename_mock)
    monkeypatch.setattr(proc_mod.idx, "delete_points_by_path", delete_mock)
    monkeypatch.setattr(proc_mod, "update_index_journal_entries_status", journal_mock)

    proc_mod._process_paths(
        [src, dest],
        client=MagicMock(),
        model=MagicMock(),
        vector_name="vec",
        model_dim=1,
        workspace_path=str(tmp_path),
    )

    rename_mock.assert_called_once()
    delete_mock.assert_not_called()
    updates = journal_mock.call_args.args[0]
    done_paths = [entry["path"] for entry in updates if entry.get("status") == "done"]
    assert str(dest.resolve()) in done_paths
    assert str(src.resolve()) in done_paths


def test_processor_skips_internal_git_path_without_collection_resolution(monkeypatch):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    internal = Path("/work/.git/HEAD")

    monkeypatch.setattr(proc_mod, "_set_status_indexing", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "get_workspace_state", lambda *a, **k: {})
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: False)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_extract_repo_name_from_path", lambda *_: "repo")

    collection_mock = MagicMock(return_value="should-not-be-used")
    journal_mock = MagicMock()
    monkeypatch.setattr(proc_mod, "_get_collection_for_file", collection_mock)
    monkeypatch.setattr(proc_mod, "update_index_journal_entry_status", journal_mock)
    monkeypatch.setattr(
        proc_mod,
        "list_pending_index_journal_entries",
        lambda *a, **k: [{"path": str(internal), "op_type": "delete"}],
    )

    proc_mod._process_paths(
        [internal],
        client=MagicMock(),
        model=None,
        vector_name="vec",
        model_dim=1,
        workspace_path="/work",
    )

    collection_mock.assert_not_called()
    journal_mock.assert_called_once()
    assert journal_mock.call_args.kwargs["status"] == "done"


def test_processor_force_upsert_empty_file_marks_done(monkeypatch, tmp_path):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    empty_file = tmp_path / "pkg" / "__init__.py"
    empty_file.parent.mkdir(parents=True, exist_ok=True)
    empty_file.write_text("", encoding="utf-8")

    monkeypatch.setattr(proc_mod, "_detect_repo_for_file", lambda p: tmp_path)
    monkeypatch.setattr(proc_mod, "_get_collection_for_file", lambda p: "coll")
    monkeypatch.setattr(proc_mod, "_set_status_indexing", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "get_workspace_state", lambda *a, **k: {})
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: False)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_extract_repo_name_from_path", lambda *_: "repo")
    monkeypatch.setattr(proc_mod, "remove_cached_file", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_run_indexing_strategy", lambda *a, **k: False)
    monkeypatch.setattr(proc_mod, "_path_has_indexed_points", lambda *a, **k: False)

    journal_mock = MagicMock()
    monkeypatch.setattr(proc_mod, "update_index_journal_entries_status", journal_mock)
    monkeypatch.setattr(
        proc_mod,
        "list_pending_index_journal_entries",
        lambda *a, **k: [
            {
                "path": str(empty_file.resolve()),
                "op_type": "upsert",
                "content_hash": "da39a3ee5e6b4b0d3255bfef95601890afd80709",
            }
        ],
    )

    proc_mod._process_paths(
        [empty_file],
        client=MagicMock(),
        model=MagicMock(),
        vector_name="vec",
        model_dim=1,
        workspace_path=str(tmp_path),
    )

    journal_mock.assert_called_once()
    assert journal_mock.call_args.args[0][0]["status"] == "done"
