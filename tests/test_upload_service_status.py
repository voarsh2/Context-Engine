import asyncio
import importlib
from pathlib import Path

import pytest
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_delta_status_exposes_last_processed_operations(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)

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
    monkeypatch.setattr(srv, "log_activity", None)

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
def test_delta_status_reports_processing_while_upload_in_progress(monkeypatch):
    srv = importlib.import_module("scripts.upload_service")
    srv = importlib.reload(srv)

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
