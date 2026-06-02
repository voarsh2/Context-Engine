import importlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest


@pytest.mark.parametrize(
    "mod_name",
    ["scripts.remote_upload_client", "scripts.standalone_upload_client"],
)
def test_remote_upload_config_does_not_generate_collection_name(monkeypatch, tmp_path, mod_name):
    mod = importlib.import_module(mod_name)
    workspace = tmp_path / "repo"
    workspace.mkdir()

    monkeypatch.setattr(mod, "_compute_logical_repo_id", lambda _path: "fs:test")

    config = mod.get_remote_config(str(workspace))

    assert config["collection_name"] is None


def _exercise_ignored_path_cleanup(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    ignored = workspace / "dev-workspace" / "nested.py"
    ignored.parent.mkdir(parents=True, exist_ok=True)
    ignored.write_text("print('dogfood')\n", encoding="utf-8")

    monkeypatch.setenv("DEV_REMOTE_MODE", "1")
    monkeypatch.setattr(mod, "get_cached_file_hash", lambda path, repo_name=None: "abc123")
    monkeypatch.setattr(mod, "set_cached_file_hash", lambda *a, **k: None)

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    changes = client.detect_file_changes([ignored])

    assert ignored in changes["deleted"]
    assert not changes["created"]
    assert not changes["updated"]
    assert not changes["moved"]


def test_remote_upload_client_marks_ignored_cached_paths_deleted(monkeypatch, tmp_path):
    _exercise_ignored_path_cleanup("scripts.remote_upload_client", monkeypatch, tmp_path)


def test_standalone_upload_client_marks_ignored_cached_paths_deleted(monkeypatch, tmp_path):
    _exercise_ignored_path_cleanup("scripts.standalone_upload_client", monkeypatch, tmp_path)


def _exercise_force_mode_cleanup(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    current = workspace / "app.py"
    current.write_text("print('current')\n", encoding="utf-8")

    stale_ignored = workspace / "dev-workspace" / "nested.py"
    stale_ignored.parent.mkdir(parents=True, exist_ok=True)
    stale_ignored.write_text("print('stale')\n", encoding="utf-8")

    monkeypatch.setenv("DEV_REMOTE_MODE", "1")
    monkeypatch.setattr(mod, "get_all_cached_paths", lambda repo_name=None: [str(stale_ignored)])
    monkeypatch.setattr(mod, "get_cached_file_hash", lambda path, repo_name=None: "abc123")
    monkeypatch.setattr(mod, "set_cached_file_hash", lambda *a, **k: None)

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    changes = client.build_force_changes([current])

    assert current in changes["created"]
    assert stale_ignored in changes["deleted"]
    assert not changes["updated"]
    assert not changes["moved"]


def test_remote_upload_client_force_mode_keeps_creates_and_deletes_ignored_cached_paths(monkeypatch, tmp_path):
    _exercise_force_mode_cleanup("scripts.remote_upload_client", monkeypatch, tmp_path)


def test_standalone_upload_client_force_mode_keeps_creates_and_deletes_ignored_cached_paths(monkeypatch, tmp_path):
    _exercise_force_mode_cleanup("scripts.standalone_upload_client", monkeypatch, tmp_path)


def _exercise_force_mode_excludes_ignored_current_files(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    current = workspace / "app.py"
    current.write_text("print('current')\n", encoding="utf-8")

    ignored_current = workspace / "dev-workspace" / "ignored.py"
    ignored_current.parent.mkdir(parents=True, exist_ok=True)
    ignored_current.write_text("print('ignored')\n", encoding="utf-8")

    monkeypatch.setenv("DEV_REMOTE_MODE", "1")
    monkeypatch.setattr(mod, "get_all_cached_paths", lambda repo_name=None: [])
    monkeypatch.setattr(mod, "get_cached_file_hash", lambda path, repo_name=None: None)
    monkeypatch.setattr(mod, "set_cached_file_hash", lambda *a, **k: None)

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    changes = client.build_force_changes([current, ignored_current])

    assert current in changes["created"]
    assert ignored_current not in changes["created"]
    assert ignored_current in changes["deleted"]
    assert not changes["updated"]
    assert not changes["moved"]


def test_remote_upload_client_force_mode_excludes_ignored_current_files(monkeypatch, tmp_path):
    _exercise_force_mode_excludes_ignored_current_files(
        "scripts.remote_upload_client",
        monkeypatch,
        tmp_path,
    )


def test_standalone_upload_client_force_mode_excludes_ignored_current_files(monkeypatch, tmp_path):
    _exercise_force_mode_excludes_ignored_current_files(
        "scripts.standalone_upload_client",
        monkeypatch,
        tmp_path,
    )


def _exercise_force_mode_dev_workspace_cleanup_without_cache(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    current = workspace / "app.py"
    current.write_text("print('current')\n", encoding="utf-8")

    mirrored = workspace / "dev-workspace" / "nested" / "stale.py"
    mirrored.parent.mkdir(parents=True, exist_ok=True)
    mirrored.write_text("print('stale')\n", encoding="utf-8")

    monkeypatch.setenv("DEV_REMOTE_MODE", "1")
    monkeypatch.setattr(mod, "get_all_cached_paths", lambda repo_name=None: [])
    monkeypatch.setattr(mod, "get_cached_file_hash", lambda path, repo_name=None: None)
    monkeypatch.setattr(mod, "set_cached_file_hash", lambda *a, **k: None)

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    changes = client.build_force_changes([current])

    assert current in changes["created"]
    assert mirrored in changes["deleted"]
    assert not changes["updated"]
    assert not changes["moved"]


def test_remote_upload_client_force_mode_deletes_dev_workspace_without_cache(monkeypatch, tmp_path):
    _exercise_force_mode_dev_workspace_cleanup_without_cache("scripts.remote_upload_client", monkeypatch, tmp_path)


def test_standalone_upload_client_force_mode_deletes_dev_workspace_without_cache(monkeypatch, tmp_path):
    _exercise_force_mode_dev_workspace_cleanup_without_cache("scripts.standalone_upload_client", monkeypatch, tmp_path)


def _exercise_plan_skip_avoids_bundle_upload(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    current = workspace / "app.py"
    current.write_text("print('current')\n", encoding="utf-8")

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    monkeypatch.setattr(
        client,
        "_plan_delta_upload",
        lambda changes: {
            "needed_files": {"created": [], "updated": [], "moved": []},
            "operation_counts_preview": {
                "created": 0,
                "updated": 0,
                "deleted": 0,
                "moved": 0,
                "skipped": 1,
                "skipped_hash_match": 1,
                "failed": 0,
            },
            "needed_size_bytes": 0,
        },
    )
    monkeypatch.setattr(client, "create_delta_bundle", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("should not bundle")))
    monkeypatch.setattr(client, "upload_bundle", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("should not upload")))

    assert client.process_changes_and_upload(
        {
            "created": [current],
            "updated": [],
            "deleted": [],
            "moved": [],
            "unchanged": [],
        }
    ) is True
    assert client.last_upload_result["outcome"] == "skipped_by_plan"


def test_remote_upload_client_plan_skip_avoids_bundle_upload(monkeypatch, tmp_path):
    _exercise_plan_skip_avoids_bundle_upload("scripts.remote_upload_client", monkeypatch, tmp_path)


def test_standalone_upload_client_plan_skip_avoids_bundle_upload(monkeypatch, tmp_path):
    _exercise_plan_skip_avoids_bundle_upload("scripts.standalone_upload_client", monkeypatch, tmp_path)


def _exercise_detect_file_changes_does_not_persist_hash(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    current = workspace / "app.py"
    current.write_text("print('current')\n", encoding="utf-8")

    set_hash = MagicMock()
    monkeypatch.setattr(mod, "get_cached_file_hash", lambda path, repo_name=None: "oldhash")
    monkeypatch.setattr(mod, "set_cached_file_hash", set_hash)

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    changes = client.detect_file_changes([current])

    assert current in changes["updated"]
    set_hash.assert_not_called()


def test_remote_upload_client_detect_file_changes_does_not_persist_hash(monkeypatch, tmp_path):
    _exercise_detect_file_changes_does_not_persist_hash(
        "scripts.remote_upload_client", monkeypatch, tmp_path
    )


def test_standalone_upload_client_detect_file_changes_does_not_persist_hash(monkeypatch, tmp_path):
    _exercise_detect_file_changes_does_not_persist_hash(
        "scripts.standalone_upload_client", monkeypatch, tmp_path
    )


def _exercise_plan_skip_finalizes_hash(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    current = workspace / "app.py"
    current.write_text("print('current')\n", encoding="utf-8")

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    set_hash = MagicMock()
    monkeypatch.setattr(mod, "set_cached_file_hash", set_hash)
    monkeypatch.setattr(
        client,
        "_plan_delta_upload",
        lambda changes: {
            "needed_files": {"created": [], "updated": [], "moved": []},
            "operation_counts_preview": {
                "created": 0,
                "updated": 0,
                "deleted": 0,
                "moved": 0,
                "skipped": 1,
                "skipped_hash_match": 1,
                "failed": 0,
            },
            "needed_size_bytes": 0,
        },
    )
    monkeypatch.setattr(client, "create_delta_bundle", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("should not bundle")))
    monkeypatch.setattr(client, "upload_bundle", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("should not upload")))

    assert client.process_changes_and_upload(
        {
            "created": [],
            "updated": [current],
            "deleted": [],
            "moved": [],
            "unchanged": [],
        }
    ) is True
    assert client.last_upload_result["outcome"] == "skipped_by_plan"
    set_hash.assert_called_once()


def test_remote_upload_client_plan_skip_finalizes_hash(monkeypatch, tmp_path):
    _exercise_plan_skip_finalizes_hash(
        "scripts.remote_upload_client", monkeypatch, tmp_path
    )


def test_standalone_upload_client_plan_skip_finalizes_hash(monkeypatch, tmp_path):
    _exercise_plan_skip_finalizes_hash(
        "scripts.standalone_upload_client", monkeypatch, tmp_path
    )


def test_standalone_upload_client_plan_payload_prefixes_previous_hash(monkeypatch, tmp_path):
    mod = importlib.import_module("scripts.standalone_upload_client")

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    updated = workspace / "app.py"
    updated.write_text("print('updated')\n", encoding="utf-8")

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    monkeypatch.setattr(mod, "get_cached_file_hash", lambda path, repo_name=None: "abc123")

    payload = client._build_plan_payload(
        {
            "created": [],
            "updated": [updated],
            "deleted": [updated],
            "moved": [],
        }
    )

    updated_op = next(op for op in payload["operations"] if op["operation"] == "updated")
    deleted_op = next(op for op in payload["operations"] if op["operation"] == "deleted")
    assert updated_op["previous_hash"] == "sha1:abc123"
    assert deleted_op["previous_hash"] == "sha1:abc123"


def _exercise_delete_only_plan_uses_apply_ops(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    deleted = workspace / "old.py"
    deleted.write_text("print('old')\n", encoding="utf-8")

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )
    removed_paths = []

    monkeypatch.setattr(
        client,
        "_plan_delta_upload",
        lambda changes: {
            "needed_files": {"created": [], "updated": [], "moved": []},
            "operation_counts_preview": {
                "created": 0,
                "updated": 0,
                "deleted": 1,
                "moved": 0,
                "skipped": 0,
                "skipped_hash_match": 0,
                "failed": 0,
            },
            "needed_size_bytes": 0,
        },
    )
    monkeypatch.setattr(
        client,
        "_build_plan_payload",
        lambda changes: {
            "manifest": {"bundle_id": "b1", "sequence_number": None},
            "operations": [{"operation": "deleted", "path": "old.py"}],
            "file_hashes": {},
        },
    )
    monkeypatch.setattr(client, "create_delta_bundle", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("should not bundle")))
    monkeypatch.setattr(client, "upload_bundle", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("should not upload")))

    class _Resp:
        status_code = 200

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {
                "success": True,
                "bundle_id": "b1",
                "sequence_number": 3,
                "processed_operations": {"deleted": 1, "created": 0, "updated": 0, "moved": 0, "skipped": 0, "skipped_hash_match": 0, "failed": 0},
            }

    monkeypatch.setattr(client.session, "post", lambda *a, **k: _Resp())
    monkeypatch.setattr(mod, "remove_cached_file", lambda path, repo_name=None: removed_paths.append((path, repo_name)))

    assert client.process_changes_and_upload(
        {
            "created": [],
            "updated": [],
            "deleted": [deleted],
            "moved": [],
            "unchanged": [],
        }
    ) is True
    assert client.last_upload_result["outcome"] == "uploaded"
    assert client.last_upload_result["processed_operations"]["deleted"] == 1
    assert removed_paths == [(str(deleted.resolve()), client.repo_name)]


def test_remote_upload_client_delete_only_plan_uses_apply_ops(monkeypatch, tmp_path):
    _exercise_delete_only_plan_uses_apply_ops("scripts.remote_upload_client", monkeypatch, tmp_path)


def test_standalone_upload_client_delete_only_plan_uses_apply_ops(monkeypatch, tmp_path):
    _exercise_delete_only_plan_uses_apply_ops("scripts.standalone_upload_client", monkeypatch, tmp_path)


def _exercise_async_upload_sets_queued_result(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    current = workspace / "app.py"
    current.write_text("print('current')\n", encoding="utf-8")

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    bundle_path = workspace / "bundle.tar.gz"
    bundle_path.write_bytes(b"bundle")
    monkeypatch.setattr(client, "_plan_delta_upload", lambda changes: None)
    monkeypatch.setattr(
        client,
        "create_delta_bundle",
        lambda changes: (str(bundle_path), {"bundle_id": "bundle-1", "total_size_bytes": 6}),
    )
    monkeypatch.setattr(
        client,
        "upload_bundle",
        lambda *a, **k: {"success": True, "sequence_number": 7, "processed_operations": None},
    )
    monkeypatch.setattr(mod, "flush_cached_file_hashes", lambda: None, raising=False)

    assert client.process_changes_and_upload(
        {
            "created": [current],
            "updated": [],
            "deleted": [],
            "moved": [],
            "unchanged": [],
        }
    ) is True
    assert client.last_upload_result["outcome"] == "queued"
    assert client.last_upload_result["sequence_number"] == 7


def _exercise_async_upload_promotes_completed_result(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    current = workspace / "app.py"
    current.write_text("print('current')\n", encoding="utf-8")

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    bundle_path = workspace / "bundle.tar.gz"
    bundle_path.write_bytes(b"bundle")
    monkeypatch.setattr(client, "_plan_delta_upload", lambda changes: None)
    monkeypatch.setattr(
        client,
        "create_delta_bundle",
        lambda changes: (str(bundle_path), {"bundle_id": "bundle-1", "total_size_bytes": 6}),
    )
    monkeypatch.setattr(
        client,
        "upload_bundle",
        lambda *a, **k: {"success": True, "sequence_number": 7, "processed_operations": None},
    )
    monkeypatch.setattr(
        client,
        "get_server_status",
        lambda: {
            "success": True,
            "last_sequence": 7,
            "server_info": {
                "last_bundle_id": "bundle-1",
                "last_upload_status": "completed",
                "last_processed_operations": {"updated": 1, "failed": 0},
                "last_processing_time_ms": 12,
            },
        },
    )
    monkeypatch.setattr(mod, "flush_cached_file_hashes", lambda: None, raising=False)

    assert client.process_changes_and_upload(
        {
            "created": [current],
            "updated": [],
            "deleted": [],
            "moved": [],
            "unchanged": [],
        }
    ) is True
    assert client.last_upload_result["outcome"] == "uploaded_async"
    assert client.last_upload_result["processed_operations"] == {"updated": 1, "failed": 0}


def test_remote_upload_client_async_upload_sets_queued_result(monkeypatch, tmp_path):
    _exercise_async_upload_sets_queued_result("scripts.remote_upload_client", monkeypatch, tmp_path)


def test_standalone_upload_client_async_upload_sets_queued_result(monkeypatch, tmp_path):
    _exercise_async_upload_sets_queued_result("scripts.standalone_upload_client", monkeypatch, tmp_path)


def test_remote_upload_client_async_upload_promotes_completed_result(monkeypatch, tmp_path):
    _exercise_async_upload_promotes_completed_result("scripts.remote_upload_client", monkeypatch, tmp_path)


def test_standalone_upload_client_async_upload_promotes_completed_result(monkeypatch, tmp_path):
    _exercise_async_upload_promotes_completed_result("scripts.standalone_upload_client", monkeypatch, tmp_path)


def _exercise_watchable_path_excludes_ignored_updates(mod_name: str, monkeypatch, tmp_path: Path) -> None:
    mod = importlib.import_module(mod_name)

    workspace = tmp_path / "repo"
    workspace.mkdir(parents=True, exist_ok=True)
    source = workspace / "src" / "tracked.py"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("print('tracked')\n", encoding="utf-8")

    mirrored = workspace / "dev-workspace" / "nested" / "ignored.py"
    mirrored.parent.mkdir(parents=True, exist_ok=True)
    mirrored.write_text("print('ignored')\n", encoding="utf-8")

    monkeypatch.setenv("DEV_REMOTE_MODE", "1")

    client = mod.RemoteUploadClient(
        upload_endpoint="http://localhost:8004",
        workspace_path=str(workspace),
        collection_name="test-coll",
    )

    assert client._is_watchable_path(source) is True
    assert client._is_watchable_path(mirrored) is False


def test_remote_upload_client_watchable_path_excludes_ignored_updates(monkeypatch, tmp_path):
    _exercise_watchable_path_excludes_ignored_updates(
        "scripts.remote_upload_client",
        monkeypatch,
        tmp_path,
    )


def test_standalone_upload_client_watchable_path_excludes_ignored_updates(monkeypatch, tmp_path):
    _exercise_watchable_path_excludes_ignored_updates(
        "scripts.standalone_upload_client",
        monkeypatch,
        tmp_path,
    )
