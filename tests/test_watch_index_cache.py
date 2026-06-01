"""Test error handling for workspace_state cache operations in watch_index_core."""
import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def test_handler_invalidate_cache_handles_errors(monkeypatch, tmp_path):
    """Verify IndexHandler._invalidate_cache gracefully handles workspace_state errors."""
    handler_mod = importlib.import_module("scripts.watch_index_core.handler")
    ws_mod = importlib.import_module("scripts.workspace_state")
    
    # Mock workspace_state functions to raise errors
    monkeypatch.setattr(ws_mod, "remove_cached_file", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(ws_mod, "remove_cached_symbols", lambda *a: (_ for _ in ()).throw(RuntimeError("boom")))
    
    # Create a minimal handler
    queue = MagicMock()
    client = MagicMock()
    h = handler_mod.IndexHandler(tmp_path, queue, client, "test-coll")
    
    # Should not raise despite errors
    result = h._invalidate_cache(tmp_path / "test.py")
    assert result is None  # No repo_name when not in multi-repo mode


def test_processor_handles_cache_read_errors(monkeypatch, tmp_path):
    """Verify processor gracefully handles get_cached_file_hash errors."""
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")
    ws_mod = importlib.import_module("scripts.workspace_state")
    
    # Mock to raise error
    monkeypatch.setattr(ws_mod, "get_cached_file_hash", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    
    # Create a test file
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content", encoding="utf-8")
    
    # Exercise _read_text_and_sha1 - should handle errors gracefully and return content + hash
    text, sha1 = proc_mod._read_text_and_sha1(test_file)
    assert text == "test content"
    assert sha1 is not None and len(sha1) == 40  # SHA1 hex length


def test_handler_move_event_handles_cache_errors(monkeypatch, tmp_path):
    """Verify on_moved handles cache errors gracefully by completing without raising."""
    handler_mod = importlib.import_module("scripts.watch_index_core.handler")
    ws_mod = importlib.import_module("scripts.workspace_state")
    
    # Mock workspace_state to raise
    monkeypatch.setattr(ws_mod, "get_cached_file_hash", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(ws_mod, "set_cached_file_hash", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(ws_mod, "remove_cached_file", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    monkeypatch.setattr(ws_mod, "remove_cached_symbols", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    
    # Create source and dest files
    src_file = tmp_path / "src.py"
    dest_file = tmp_path / "dest.py"
    src_file.write_text("# code", encoding="utf-8")
    dest_file.write_text("# code", encoding="utf-8")
    
    # Create handler and mock event
    queue = MagicMock()
    client = MagicMock()
    client.scroll = MagicMock(return_value=([], None))  # No points to rename
    h = handler_mod.IndexHandler(tmp_path, queue, client, "test-coll")
    
    # Create mock move event
    event = MagicMock()
    event.is_directory = False
    event.src_path = str(src_file)
    event.dest_path = str(dest_file)
    
    # Should complete without raising despite cache errors
    result = h.on_moved(event)
    assert result is None  # on_moved returns None


def test_processor_handles_cache_remove_errors(monkeypatch, tmp_path):
    """Verify processor handles remove_cached_file errors when processing deletes."""
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")
    ws_mod = importlib.import_module("scripts.workspace_state")
    
    # Mock to raise
    monkeypatch.setattr(ws_mod, "remove_cached_file", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    
    missing = tmp_path / "missing.py"
    assert not missing.exists()

    # Mock other dependencies
    monkeypatch.setattr(proc_mod, "_detect_repo_for_file", lambda p: tmp_path)
    monkeypatch.setattr(proc_mod, "_get_collection_for_file", lambda p: "coll")
    monkeypatch.setattr(proc_mod, "_set_status_indexing", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "get_workspace_state", lambda *a, **k: {})
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: False)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_extract_repo_name_from_path", lambda *_: "repo")

    # _process_paths should complete without raising despite cache errors
    # This exercises the delete path when file doesn't exist
    proc_mod._process_paths(
        [missing],
        client=None,
        model=None,
        vector_name="vec",
        model_dim=1,
        workspace_path=str(tmp_path),
    )
    # If we get here without exception, the error was handled gracefully



def test_watch_index_core_config_root_dir_matches_project_root():
    cfg_mod = importlib.import_module("scripts.watch_index_core.config")
    expected = Path(cfg_mod.__file__).resolve().parents[2]
    assert Path(cfg_mod.ROOT_DIR).resolve() == expected
    assert str(expected) in sys.path


def test_processor_delete_clears_cache_even_without_client(monkeypatch, tmp_path):
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

    remove_mock = MagicMock()
    monkeypatch.setattr(proc_mod, "remove_cached_file", remove_mock)

    proc_mod._process_paths(
        [missing],
        client=None,
        model=None,
        vector_name="vec",
        model_dim=1,
        workspace_path=str(tmp_path),
    )

    remove_mock.assert_called_once_with(str(missing), "repo")


def test_run_indexing_strategy_reuses_preloaded_file_state(monkeypatch, tmp_path):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "file.py"
    path.write_text("print('x')\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod.idx, "ensure_collection_and_indexes_once", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_read_text_and_sha1", lambda _p: ("print('x')\n", "abc123"))
    monkeypatch.setattr(proc_mod, "get_cached_file_hash", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod.idx, "detect_language", lambda _p: "python")
    monkeypatch.setattr(proc_mod.idx, "should_use_smart_reindexing", lambda *a, **k: (False, "changed"))

    captured = {}

    def fake_index_single_file(*args, **kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(proc_mod.idx, "index_single_file", fake_index_single_file)

    ok = proc_mod._run_indexing_strategy(
        path,
        client=MagicMock(),
        model=MagicMock(),
        collection="coll",
        vector_name="vec",
        model_dim=1,
        repo_name="repo",
    )

    assert ok is True
    assert captured["preloaded_text"] == "print('x')\n"
    assert captured["preloaded_file_hash"] == "abc123"
    assert captured["preloaded_language"] == "python"


def test_run_indexing_strategy_skips_ensure_for_cached_hash_match(monkeypatch, tmp_path):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "file.py"
    path.write_text("print('x')\n", encoding="utf-8")

    ensure_mock = MagicMock()
    monkeypatch.setattr(proc_mod.idx, "ensure_collection_and_indexes_once", ensure_mock)
    monkeypatch.setattr(proc_mod, "_read_text_and_sha1", lambda _p: ("print('x')\n", "abc123"))
    monkeypatch.setattr(proc_mod, "get_cached_file_hash", lambda *a, **k: "abc123")
    monkeypatch.setattr(proc_mod.idx, "detect_language", lambda _p: "python")

    with pytest.raises(proc_mod._SkipUnchanged):
        proc_mod._run_indexing_strategy(
            path,
            client=MagicMock(),
            model=MagicMock(),
            collection="coll",
            vector_name="vec",
            model_dim=1,
            repo_name="repo",
        )

    ensure_mock.assert_not_called()


def test_run_indexing_strategy_force_upsert_bypasses_cached_hash_match(
    monkeypatch, tmp_path
):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "file.py"
    path.write_text("print('x')\n", encoding="utf-8")

    ensure_mock = MagicMock()
    monkeypatch.setattr(proc_mod.idx, "ensure_collection_and_indexes_once", ensure_mock)
    monkeypatch.setattr(proc_mod, "_read_text_and_sha1", lambda _p: ("print('x')\n", "abc123"))
    monkeypatch.setattr(proc_mod, "get_cached_file_hash", lambda *a, **k: "abc123")
    monkeypatch.setattr(proc_mod.idx, "detect_language", lambda _p: "python")
    monkeypatch.setattr(proc_mod.idx, "should_use_smart_reindexing", lambda *a, **k: (False, "changed"))

    index_mock = MagicMock(return_value=True)
    monkeypatch.setattr(proc_mod.idx, "index_single_file", index_mock)

    ok = proc_mod._run_indexing_strategy(
        path,
        client=MagicMock(),
        model=MagicMock(),
        collection="coll",
        vector_name="vec",
        model_dim=1,
        repo_name="repo",
        force_upsert=True,
    )

    assert ok is True
    ensure_mock.assert_called_once()
    index_mock.assert_called_once()


def test_run_indexing_strategy_skips_smart_path_for_markdown(monkeypatch, tmp_path):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "notes.md"
    path.write_text("# notes\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod.idx, "ensure_collection_and_indexes_once", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_read_text_and_sha1", lambda _p: ("# notes\n", "abc123"))
    monkeypatch.setattr(proc_mod, "get_cached_file_hash", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod.idx, "detect_language", lambda _p: "markdown")

    smart_check = MagicMock(side_effect=AssertionError("smart path must be skipped"))
    monkeypatch.setattr(proc_mod.idx, "should_use_smart_reindexing", smart_check)

    captured = {}

    def fake_index_single_file(*args, **kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(proc_mod.idx, "index_single_file", fake_index_single_file)

    ok = proc_mod._run_indexing_strategy(
        path,
        client=MagicMock(),
        model=MagicMock(),
        collection="coll",
        vector_name="vec",
        model_dim=1,
        repo_name="repo",
    )

    assert ok is True
    smart_check.assert_not_called()
    assert captured["preloaded_language"] == "markdown"


def test_run_indexing_strategy_force_upsert_missing_points_bypasses_smart(
    monkeypatch, tmp_path
):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "file.py"
    path.write_text("print('x')\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod.idx, "ensure_collection_and_indexes_once", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_read_text_and_sha1", lambda _p: ("print('x')\n", "abc123"))
    monkeypatch.setattr(proc_mod, "get_cached_file_hash", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod.idx, "detect_language", lambda _p: "python")
    monkeypatch.setattr(proc_mod.idx, "should_use_smart_reindexing", lambda *a, **k: (True, "smart_reindex"))
    monkeypatch.setattr(proc_mod.idx, "get_indexed_file_hash", lambda *a, **k: "")
    monkeypatch.setattr(proc_mod, "_path_has_indexed_points", lambda *a, **k: False)

    smart_mock = MagicMock(return_value="skipped")
    monkeypatch.setattr(proc_mod.idx, "process_file_with_smart_reindexing", smart_mock)

    index_mock = MagicMock(return_value=True)
    monkeypatch.setattr(proc_mod.idx, "index_single_file", index_mock)

    ok = proc_mod._run_indexing_strategy(
        path,
        client=MagicMock(),
        model=MagicMock(),
        collection="coll",
        vector_name="vec",
        model_dim=1,
        repo_name="repo",
        force_upsert=True,
    )

    assert ok is True
    smart_mock.assert_not_called()
    index_mock.assert_called_once()


def test_run_indexing_strategy_sets_skip_verify_reason_for_file_lock(
    monkeypatch, tmp_path
):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "file.py"
    path.write_text("print('x')\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod.idx, "ensure_collection_and_indexes_once", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_read_text_and_sha1", lambda _p: ("print('x')\n", "abc123"))
    monkeypatch.setattr(proc_mod, "get_cached_file_hash", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod.idx, "detect_language", lambda _p: "python")
    monkeypatch.setattr(proc_mod.idx, "should_use_smart_reindexing", lambda *a, **k: (False, "changed"))
    monkeypatch.setattr(proc_mod.idx, "index_single_file", lambda *a, **k: False)
    monkeypatch.setattr(proc_mod.idx, "is_file_locked", lambda *_: True)

    verify_context = {}
    ok = proc_mod._run_indexing_strategy(
        path,
        client=MagicMock(),
        model=MagicMock(),
        collection="coll",
        vector_name="vec",
        model_dim=1,
        repo_name="repo",
        force_upsert=True,
        verify_context=verify_context,
    )

    assert ok is False
    assert verify_context.get("skip_verify_reason") == "file_locked"


def test_finalize_journal_skips_force_upsert_verify_when_file_locked(monkeypatch):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    verify_mock = MagicMock()
    done_mock = MagicMock()
    failed_mock = MagicMock()
    monkeypatch.setattr(proc_mod, "_verify_and_update_journal_for_upsert", verify_mock)
    monkeypatch.setattr(proc_mod, "_mark_journal_done", done_mock)
    monkeypatch.setattr(proc_mod, "_mark_journal_failed", failed_mock)

    proc_mod._finalize_journal_after_index_attempt(
        Path("/tmp/file.py"),
        client=MagicMock(),
        collection="coll",
        repo_key="/tmp",
        repo_name="repo",
        force_upsert=True,
        journal_content_hash="abc",
        skip_verify_reason="file_locked",
    )

    verify_mock.assert_not_called()
    done_mock.assert_not_called()
    failed_mock.assert_not_called()


def test_staging_requires_subprocess_only_for_active_dual_root_state(monkeypatch):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: True)

    assert proc_mod._staging_requires_subprocess(None) is False
    assert (
        proc_mod._staging_requires_subprocess(
            {
                "indexing_env": {"FOO": "bar"},
                "active_repo_slug": "repo",
                "serving_repo_slug": "repo",
            }
        )
        is False
    )
    assert (
        proc_mod._staging_requires_subprocess(
            {
                "indexing_env": {"FOO": "bar"},
                "active_repo_slug": "repo",
                "serving_repo_slug": "repo_old",
            }
        )
        is True
    )
    assert (
        proc_mod._staging_requires_subprocess(
            {
                "indexing_env": {"FOO": "bar"},
                "active_repo_slug": "repo",
                "serving_repo_slug": "repo",
                "staging": {"collection": "repo_old_collection"},
            }
        )
        is True
    )


def test_process_paths_does_not_force_subprocess_for_non_active_staging(
    monkeypatch, tmp_path
):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "file.py"
    path.write_text("print('x')\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod, "_detect_repo_for_file", lambda p: tmp_path)
    monkeypatch.setattr(proc_mod, "_get_collection_for_file", lambda p: "coll")
    monkeypatch.setattr(proc_mod, "_set_status_indexing", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_extract_repo_name_from_path", lambda *_: "repo")
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: True)
    monkeypatch.setattr(
        proc_mod,
        "get_workspace_state",
        lambda *a, **k: {
            "indexing_env": {"FOO": "bar"},
            "active_repo_slug": "repo",
            "serving_repo_slug": "repo",
        },
    )

    staging_mock = MagicMock(return_value=False)
    monkeypatch.setattr(proc_mod, "_maybe_handle_staging_file", staging_mock)
    monkeypatch.setattr(proc_mod, "_run_indexing_strategy", lambda *a, **k: True)

    proc_mod._process_paths(
        [path],
        client=MagicMock(),
        model=MagicMock(),
        vector_name="vec",
        model_dim=1,
        workspace_path=str(tmp_path),
    )

    assert staging_mock.call_args is not None
    assert staging_mock.call_args.kwargs == {
        "force_upsert": False,
        "journal_content_hash": "",
    }
    assert staging_mock.call_args.args[0] == path
    assert staging_mock.call_args.args[6] is None


def test_process_paths_uses_subprocess_when_staging_is_actually_active(
    monkeypatch, tmp_path
):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "file.py"
    path.write_text("print('x')\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod, "_detect_repo_for_file", lambda p: tmp_path)
    monkeypatch.setattr(proc_mod, "_get_collection_for_file", lambda p: "coll")
    monkeypatch.setattr(proc_mod, "_set_status_indexing", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)
    monkeypatch.setattr(proc_mod, "_extract_repo_name_from_path", lambda *_: "repo")
    monkeypatch.setattr(proc_mod, "is_staging_enabled", lambda: True)
    monkeypatch.setattr(
        proc_mod,
        "get_workspace_state",
        lambda *a, **k: {
            "indexing_env": {"FOO": "bar"},
            "active_repo_slug": "repo",
            "serving_repo_slug": "repo_old",
        },
    )

    staging_mock = MagicMock(return_value=False)
    monkeypatch.setattr(proc_mod, "_maybe_handle_staging_file", staging_mock)
    monkeypatch.setattr(proc_mod, "_run_indexing_strategy", lambda *a, **k: True)

    proc_mod._process_paths(
        [path],
        client=MagicMock(),
        model=MagicMock(),
        vector_name="vec",
        model_dim=1,
        workspace_path=str(tmp_path),
    )

    assert staging_mock.call_args is not None
    assert staging_mock.call_args.kwargs == {
        "force_upsert": False,
        "journal_content_hash": "",
    }
    assert staging_mock.call_args.args[0] == path
    assert staging_mock.call_args.args[6] == {"FOO": "bar"}


def test_staging_force_upsert_hash_match_verifies_before_skip(monkeypatch, tmp_path):
    proc_mod = importlib.import_module("scripts.watch_index_core.processor")

    path = tmp_path / "file.py"
    path.write_text("print('x')\n", encoding="utf-8")

    monkeypatch.setattr(proc_mod, "_read_text_and_sha1", lambda _p: ("print('x')\n", "abc123"))
    monkeypatch.setattr(proc_mod, "get_cached_file_hash", lambda *a, **k: "abc123")
    monkeypatch.setattr(proc_mod, "_verify_upsert_committed", lambda *a, **k: True)
    monkeypatch.setattr(proc_mod, "_log_activity", lambda *a, **k: None)

    mark_done = MagicMock()
    monkeypatch.setattr(proc_mod, "_mark_journal_done", mark_done)
    advance = MagicMock()
    monkeypatch.setattr(proc_mod, "_advance_progress", advance)

    handled = proc_mod._maybe_handle_staging_file(
        path,
        MagicMock(),
        "coll",
        "repo",
        str(tmp_path),
        [path],
        {"FOO": "bar"},
        {str(tmp_path): 0},
        "started",
        force_upsert=True,
        journal_content_hash="abc123",
    )

    assert handled is True
    mark_done.assert_called_once_with(path, str(tmp_path), "repo")
    advance.assert_called_once()


def test_runtime_root_override_updates_internal_path_checks(monkeypatch, tmp_path):
    import scripts.watch_index as watch_index
    from scripts.watch_index_core import config as watch_config
    import scripts.watch_index_core.processor as proc_mod
    import scripts.embedder as embedder_mod

    runtime_root = tmp_path / "runtime-root"
    runtime_root.mkdir(parents=True, exist_ok=True)
    internal = runtime_root / ".git" / "HEAD"
    internal.parent.mkdir(parents=True, exist_ok=True)
    internal.write_text("ref: refs/heads/main\n", encoding="utf-8")

    original_root = watch_config.ROOT
    original_watch_root = watch_index.ROOT
    monkeypatch.setenv("WATCH_ROOT", str(runtime_root))
    monkeypatch.setattr(watch_index, "initialize_watcher_state", lambda root: {"repo_name": None})
    monkeypatch.setattr(watch_index, "get_indexing_config_snapshot", lambda repo_name=None: {})
    monkeypatch.setattr(watch_index, "compute_indexing_config_hash", lambda snapshot: "hash")
    monkeypatch.setattr(watch_index, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(watch_index, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(embedder_mod, "get_embedding_model", lambda *_: MagicMock())
    monkeypatch.setattr(embedder_mod, "get_model_dimension", lambda *_: 1)
    monkeypatch.setattr(watch_index, "resolve_vector_name_config", lambda *a, **k: "vec")
    monkeypatch.setattr(watch_index, "_start_pseudo_backfill_worker", lambda *a, **k: None)
    monkeypatch.setattr(watch_index, "create_observer", lambda *a, **k: MagicMock())
    monkeypatch.setattr(watch_index, "IndexHandler", MagicMock())
    monkeypatch.setattr(watch_index, "ChangeQueue", MagicMock())
    monkeypatch.setattr(
        watch_index,
        "QdrantClient",
        MagicMock(return_value=MagicMock(get_collection=MagicMock())),
    )
    monkeypatch.setattr(watch_index, "run_consistency_audit", lambda *a, **k: None)
    monkeypatch.setattr(watch_index, "run_empty_dir_sweep_maintenance", lambda *a, **k: None)
    monkeypatch.setattr(watch_index, "list_pending_index_journal_entries", lambda *a, **k: [])
    monkeypatch.setattr(watch_index, "get_boolean_env", lambda *a, **k: False)
    monkeypatch.setattr(watch_index, "_sleep", lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))

    try:
        watch_index.main()
    except KeyboardInterrupt:
        pass

    try:
        assert watch_config.ROOT == runtime_root.resolve()
        assert proc_mod._is_internal_ignored_path(internal) is True
    finally:
        watch_config.ROOT = original_root
        watch_index.ROOT = original_watch_root


def test_main_throttles_periodic_maintenance(monkeypatch, tmp_path):
    import scripts.watch_index as watch_index
    from scripts.watch_index_core import config as watch_config
    import scripts.embedder as embedder_mod

    runtime_root = tmp_path / "runtime-root"
    runtime_root.mkdir(parents=True, exist_ok=True)

    original_root = watch_config.ROOT
    original_watch_root = watch_index.ROOT
    monkeypatch.setenv("WATCH_ROOT", str(runtime_root))
    monkeypatch.setenv("WATCH_MAINTENANCE_INTERVAL_SECS", "300")
    monkeypatch.setenv("WATCH_INIT_MAINTENANCE_ENABLED", "0")
    monkeypatch.setattr(watch_index, "initialize_watcher_state", lambda *a, **k: {"repo_name": None})
    monkeypatch.setattr(watch_index, "get_indexing_config_snapshot", lambda repo_name=None: {})
    monkeypatch.setattr(watch_index, "compute_indexing_config_hash", lambda snapshot: "hash")
    monkeypatch.setattr(watch_index, "persist_indexing_config", lambda *a, **k: None)
    monkeypatch.setattr(watch_index, "update_indexing_status", lambda *a, **k: None)
    monkeypatch.setattr(embedder_mod, "get_embedding_model", lambda *_: MagicMock())
    monkeypatch.setattr(embedder_mod, "get_model_dimension", lambda *_: 1)
    monkeypatch.setattr(watch_index, "resolve_vector_name_config", lambda *a, **k: "vec")
    monkeypatch.setattr(watch_index, "_start_pseudo_backfill_worker", lambda *a, **k: None)

    class FakeObserver:
        def schedule(self, *a, **k):
            return None

        def start(self):
            return None

        def stop(self):
            return None

        def join(self):
            return None

    monkeypatch.setattr(watch_index, "create_observer", lambda *a, **k: FakeObserver())
    monkeypatch.setattr(watch_index, "IndexHandler", MagicMock())
    monkeypatch.setattr(watch_index, "ChangeQueue", MagicMock())
    monkeypatch.setattr(
        watch_index,
        "QdrantClient",
        MagicMock(return_value=MagicMock(get_collection=MagicMock())),
    )
    monkeypatch.setattr(watch_index, "get_boolean_env", lambda *a, **k: False)

    drain_mock = MagicMock()
    maintenance_mock = MagicMock()
    monkeypatch.setattr(watch_index, "_drain_pending_journal", drain_mock)
    monkeypatch.setattr(watch_index, "_run_periodic_maintenance", maintenance_mock)

    time_values = iter([0.0, 1.0, 2.0, 301.0])
    monkeypatch.setattr(watch_index.time, "time", lambda: next(time_values))

    sleep_calls = {"count": 0}

    def _sleep(_secs):
        sleep_calls["count"] += 1
        if sleep_calls["count"] >= 4:
            raise KeyboardInterrupt()

    monkeypatch.setattr(watch_index, "_sleep", _sleep)

    try:
        watch_index.main()
    finally:
        watch_config.ROOT = original_root
        watch_index.ROOT = original_watch_root

    assert drain_mock.call_count == 4
    assert maintenance_mock.call_count == 2
