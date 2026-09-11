import asyncio
import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


def _disable_auth(srv, monkeypatch) -> None:
    monkeypatch.setattr(srv, "AUTH_ENABLED", False)


@pytest.mark.unit
def test_delta_status_exposes_last_processed_operations(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(srv, "get_collection_name", lambda _repo=None: "test-coll")
    monkeypatch.setattr(srv, "_extract_repo_name_from_path", lambda _path: "repo")

    key = srv.get_workspace_key("/work/repo")
    srv._sequence_tracker[key] = 7
    srv._upload_result_tracker[key] = {
        "workspace_path": "/work/repo",
        "bundle_id": "bundle-123",
        "sequence_number": 7,
        "processed_operations": {
            "created": 1,
            "updated": 2,
            "deleted": 0,
            "moved": 0,
            "skipped": 5,
            "skipped_hash_match": 4,
            "failed": 0,
        },
        "processing_time_ms": 321,
        "status": "completed",
        "completed_at": "2026-03-07T15:40:46.623000",
    }

    client = TestClient(srv.app)
    resp = client.get("/api/v1/delta/status", params={"workspace_path": "/work/repo"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["last_sequence"] == 7
    assert body["last_upload"] == "2026-03-07T15:40:46.623000"
    assert body["status"] == "ready"
    assert body["server_info"]["last_bundle_id"] == "bundle-123"
    assert body["server_info"]["last_processing_time_ms"] == 321
    assert body["server_info"]["last_processed_operations"]["skipped_hash_match"] == 4
    assert body["server_info"]["last_upload_status"] == "completed"
    assert body["server_info"]["last_error"] is None


@pytest.mark.unit
def test_process_bundle_background_tracks_completed_operations(monkeypatch, tmp_path: Path):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    bundle_path = tmp_path / "bundle.tar.gz"
    bundle_path.write_bytes(b"placeholder")

    monkeypatch.setattr(
        srv,
        "process_delta_bundle",
        lambda workspace_path, bundle_path, manifest: {
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "moved": 0,
            "skipped": 10,
            "skipped_hash_match": 10,
            "failed": 0,
        },
    )
    monkeypatch.setattr(srv, "log_activity", lambda *a, **k: None)

    asyncio.run(
        srv._process_bundle_background(
            workspace_path="/work/repo",
            bundle_path=bundle_path,
            manifest={"bundle_id": "bundle-xyz"},
            sequence_number=3,
            bundle_id="bundle-xyz",
        )
    )

    key = srv.get_workspace_key("/work/repo")
    tracked = srv._upload_result_tracker[key]
    assert tracked["status"] == "completed"
    assert tracked["sequence_number"] == 3
    assert tracked["processed_operations"]["skipped_hash_match"] == 10
    assert tracked["processing_time_ms"] is not None
    assert not bundle_path.exists()


@pytest.mark.unit
def test_process_bundle_background_does_not_advance_sequence_after_partial_failure(
    monkeypatch, tmp_path: Path
):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    bundle_path = tmp_path / "bundle.tar.gz"
    bundle_path.write_bytes(b"placeholder")
    monkeypatch.setattr(
        srv,
        "process_delta_bundle",
        lambda *_args: {
            "created": 1,
            "updated": 0,
            "deleted": 0,
            "moved": 0,
            "skipped": 0,
            "skipped_hash_match": 0,
            "failed": 1,
        },
    )
    monkeypatch.setattr(srv, "log_activity", lambda *a, **k: None)

    key = srv.get_workspace_key("/work/repo")
    srv._sequence_tracker[key] = 2
    asyncio.run(
        srv._process_bundle_background(
            workspace_path="/work/repo",
            bundle_path=bundle_path,
            manifest={"bundle_id": "bundle-partial"},
            sequence_number=3,
            bundle_id="bundle-partial",
        )
    )

    assert srv._sequence_tracker[key] == 2
    assert srv._upload_result_tracker[key]["status"] == "failed"
    assert srv._upload_result_tracker[key]["failed_count"] == 1


@pytest.mark.unit
def test_delta_status_reports_processing_while_upload_in_progress(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(srv, "get_collection_name", lambda _repo=None: "test-coll")
    monkeypatch.setattr(srv, "_extract_repo_name_from_path", lambda _path: "repo")

    key = srv.get_workspace_key("/work/repo")
    srv._upload_result_tracker[key] = {
        "workspace_path": "/work/repo",
        "bundle_id": "bundle-123",
        "sequence_number": 8,
        "processed_operations": None,
        "processing_time_ms": None,
        "status": "processing",
        "completed_at": None,
    }

    client = TestClient(srv.app)
    resp = client.get("/api/v1/delta/status", params={"workspace_path": "/work/repo"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "processing"
    assert body["server_info"]["last_upload_status"] == "processing"


@pytest.mark.unit
def test_delta_status_exposes_journal_summary(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(srv, "get_collection_name", lambda _repo=None: "test-coll")
    monkeypatch.setattr(srv, "_extract_repo_name_from_path", lambda _path: "repo")
    monkeypatch.setattr(
        srv,
        "get_index_journal_summary",
        lambda **_: {
            "total": 12,
            "retryable": 7,
            "outstanding": 9,
            "counts": {"pending": 5, "failed": 2},
            "sample_errors": [{"path": "/work/repo/bad.py", "error": "boom"}],
        },
    )

    client = TestClient(srv.app)
    resp = client.get("/api/v1/delta/status", params={"workspace_path": "/work/repo"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["pending_operations"] == 9
    assert body["server_info"]["journal"]["total"] == 12
    assert body["server_info"]["journal"]["sample_errors"][0]["error"] == "boom"


@pytest.mark.unit
def test_delta_status_aggregates_journal_for_workspace_root(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(srv, "get_collection_name", lambda repo=None: f"coll-{repo or 'root'}")
    monkeypatch.setattr(srv, "_extract_repo_name_from_path", lambda _path: "should-not-be-used")
    summary_calls = []

    def journal_summary(**kwargs):
        summary_calls.append(kwargs)
        return {"total": 2, "retryable": 1, "outstanding": 1, "counts": {"pending": 1}}

    monkeypatch.setattr(srv, "get_index_journal_summary", journal_summary)

    client = TestClient(srv.app)
    resp = client.get("/api/v1/delta/status", params={"workspace_path": "/work"})

    assert resp.status_code == 200
    assert resp.json()["pending_operations"] == 1
    assert summary_calls == [{"workspace_path": "/work", "repo_name": None}]


@pytest.mark.unit
def test_delta_plan_endpoint_returns_needed_files(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(
        srv,
        "plan_delta_upload",
        lambda workspace_path, operations, file_hashes=None: {
            "needed_files": {"created": ["src/app.py"], "updated": [], "moved": []},
            "operation_counts_preview": {
                "created": 1,
                "updated": 0,
                "deleted": 0,
                "moved": 0,
                "skipped": 2,
                "skipped_hash_match": 2,
                "failed": 0,
            },
            "needed_size_bytes": 123,
            "replica_targets": ["repo-0123456789abcdef"],
        },
    )

    client = TestClient(srv.app)
    resp = client.post(
        "/api/v1/delta/plan",
        json={
            "workspace_path": "/work/repo",
            "manifest": {"bundle_id": "b1"},
            "operations": [{"operation": "created", "path": "src/app.py"}],
            "file_hashes": {"src/app.py": "sha1:abc"},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["needed_files"]["created"] == ["src/app.py"]
    assert body["operation_counts_preview"]["skipped_hash_match"] == 2
    assert body["needed_size_bytes"] == 123


@pytest.mark.unit
def test_upload_managed_resolution_ignores_client_collection(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(srv, "logical_repo_reuse_enabled", lambda: False)
    monkeypatch.setattr(srv, "_extract_repo_name_from_path", lambda path: Path(path).name)
    monkeypatch.setattr(srv, "get_collection_name", lambda repo=None: f"server-{repo}")

    collection, repo = srv._resolve_collection_for_request(
        workspace_path="/work/repo",
        client_collection_name="repo-071ca222",
        logical_repo_id="fs:123",
        source_path="/host/Context-Engine",
    )

    assert repo == "Context-Engine"
    assert collection == "server-Context-Engine"


@pytest.mark.unit
def test_delta_plan_endpoint_uses_safe_defaults_for_sparse_plan(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(
        srv,
        "plan_delta_upload",
        lambda workspace_path, operations, file_hashes=None: {},
    )

    client = TestClient(srv.app)
    resp = client.post(
        "/api/v1/delta/plan",
        json={
            "workspace_path": "/work/repo",
            "manifest": {"bundle_id": "b1"},
            "operations": [{"operation": "created", "path": "src/app.py"}],
            "file_hashes": {"src/app.py": "sha1:abc"},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["needed_files"] == {"created": [], "updated": [], "moved": []}
    assert body["operation_counts_preview"]["failed"] == 0
    assert body["needed_size_bytes"] == 0
    assert body["replica_targets"] == []


@pytest.mark.unit
def test_apply_ops_endpoint_returns_processed_operations(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(
        srv,
        "apply_delta_operations",
        lambda workspace_path, operations, file_hashes=None: {
            "created": 0,
            "updated": 0,
            "deleted": 1,
            "moved": 0,
            "skipped": 0,
            "skipped_hash_match": 0,
            "failed": 0,
        },
    )

    client = TestClient(srv.app)
    resp = client.post(
        "/api/v1/delta/apply_ops",
        json={
            "workspace_path": "/work/repo",
            "manifest": {"bundle_id": "b2"},
            "operations": [{"operation": "deleted", "path": "src/old.py"}],
            "file_hashes": {},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is True
    assert body["processed_operations"]["deleted"] == 1
    assert body["processing_time_ms"] is not None


@pytest.mark.unit
def test_apply_ops_advances_sequence_when_all_operations_match(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(
        srv,
        "apply_delta_operations",
        lambda *_args, **_kwargs: {
            "created": 0,
            "updated": 0,
            "deleted": 0,
            "moved": 0,
            "skipped": 1,
            "skipped_hash_match": 1,
            "failed": 0,
        },
    )

    key = srv.get_workspace_key("/work/repo")
    srv._sequence_tracker[key] = 4
    client = TestClient(srv.app)
    resp = client.post(
        "/api/v1/delta/apply_ops",
        json={
            "workspace_path": "/work/repo",
            "manifest": {"bundle_id": "b-match", "sequence_number": 5},
            "operations": [{"operation": "moved", "path": "src/new.py"}],
            "file_hashes": {"src/new.py": "sha1:match"},
        },
    )

    assert resp.status_code == 200
    assert resp.json()["success"] is True
    assert srv._sequence_tracker[key] == 5


@pytest.mark.unit
def test_apply_ops_endpoint_does_not_advance_sequence_after_partial_failure(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(
        srv,
        "apply_delta_operations",
        lambda *_args, **_kwargs: {
            "created": 1,
            "updated": 0,
            "deleted": 0,
            "moved": 0,
            "skipped": 0,
            "skipped_hash_match": 0,
            "failed": 1,
        },
    )

    key = srv.get_workspace_key("/work/repo")
    srv._sequence_tracker[key] = 4
    client = TestClient(srv.app)
    resp = client.post(
        "/api/v1/delta/apply_ops",
        json={
            "workspace_path": "/work/repo",
            "manifest": {"bundle_id": "b-partial", "sequence_number": 5},
            "operations": [{"operation": "created", "path": "src/new.py"}],
            "file_hashes": {"src/new.py": "sha1:new"},
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "APPLY_OPS_PARTIAL_FAILURE"
    assert srv._sequence_tracker[key] == 4


@pytest.mark.unit
def test_apply_ops_endpoint_marks_tracker_error_state_on_failure(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)
    _disable_auth(srv, monkeypatch)

    monkeypatch.setattr(
        srv,
        "apply_delta_operations",
        lambda workspace_path, operations, file_hashes=None: (_ for _ in ()).throw(
            RuntimeError("boom")
        ),
    )

    client = TestClient(srv.app)
    resp = client.post(
        "/api/v1/delta/apply_ops",
        json={
            "workspace_path": "/work/repo",
            "manifest": {"bundle_id": "b3"},
            "operations": [{"operation": "deleted", "path": "src/old.py"}],
            "file_hashes": {},
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["success"] is False
    assert body["error"]["code"] == "APPLY_OPS_ERROR"

    key = srv.get_workspace_key("/work/repo")
    tracked = srv._upload_result_tracker[key]
    assert tracked["status"] == "error"
    assert tracked["error"] == "boom"
    assert tracked["message"] == "boom"
    assert tracked["completed_at"] is not None
