import importlib
from pathlib import Path


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
