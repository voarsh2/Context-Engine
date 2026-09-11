"""Integration test for the relevance feedback pipeline.

Validates the full end-to-end flow:
1. Search produces result_id on every result
2. rate_search_results logs feedback events
3. relevance_trainer aggregates events into weight files
4. Subsequent searches apply learned relevance boosts

Uses Qdrant via CI service, explicit QDRANT_URL, or local testcontainers fallback.
Requires --run-integration flag.
"""

import json
import os
import asyncio
import importlib
import pytest

pytestmark = pytest.mark.integration


class FakeEmbedder:
    def __init__(self, model_name: str = "fake"):
        self.model_name = model_name

    class _Vec:
        def __init__(self, arr):
            self._arr = arr

        def tolist(self):
            return self._arr

        def __len__(self):
            return len(self._arr)

    def embed(self, texts):
        for t in texts:
            h = sum(ord(c) for c in t) % 997
            vec = [(float((h + i) % 13) / 13.0) for i in range(32)]
            yield self._Vec(vec)


def _load_modules():
    return {
        "ing": importlib.import_module("scripts.ingest_code"),
        "srv": importlib.import_module("scripts.mcp_indexer_server"),
        "embedder": importlib.import_module("scripts.embedder"),
        "hy": importlib.import_module("scripts.hybrid_search"),
        "pipeline": importlib.import_module("scripts.ingest.pipeline"),
        "rt": importlib.import_module("scripts.relevance_trainer"),
    }


def _stub_embeddings(monkeypatch, modules):
    """Stub all embedding paths with FakeEmbedder (32-dim), avoiding real model loads."""
    from qdrant_client import QdrantClient as _RealQdrantClient

    ing = modules["ing"]
    srv = modules["srv"]
    embedder = modules["embedder"]
    hy = modules["hy"]
    pipeline = modules["pipeline"]

    monkeypatch.setattr(ing, "TextEmbedding", lambda *a, **k: FakeEmbedder("fake"))
    monkeypatch.setattr(embedder, "get_embedding_model", lambda *a, **k: FakeEmbedder("fake"))
    monkeypatch.setattr(embedder, "get_model_dimension", lambda *a, **k: 32)
    monkeypatch.setattr(srv, "_get_embedding_model", lambda *a, **k: FakeEmbedder("fake"))
    monkeypatch.setattr(hy, "TextEmbedding", lambda *a, **k: FakeEmbedder("fake"))
    monkeypatch.setattr(hy, "_get_embedding_model", lambda *a, **k: FakeEmbedder("fake"))
    # Override TYPE_CHECKING shim; pipeline/hybrid use QdrantClient=Any at runtime.
    monkeypatch.setattr(pipeline, "QdrantClient", _RealQdrantClient)
    monkeypatch.setattr(hy, "QdrantClient", _RealQdrantClient)


def _make_tiny_repo(tmp_path):
    """Create a small multi-file repo for search testing."""
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "auth.py").write_text("def authenticate(user):\n    return user == 'admin'\n")
    (tmp_path / "pkg" / "utils.py").write_text("def log(msg):\n    print(msg)\n")
    (tmp_path / "pkg" / "README.md").write_text("# Test Project\nThis is a test project.\n")
    return tmp_path


@pytest.mark.integration
def test_relevance_feedback_pipeline(tmp_path, monkeypatch, qdrant_url, test_collection):
    """One indexed repo validates ids, hands-off rating, training, and boosts."""
    modules = _load_modules()
    ing = modules["ing"]
    srv = modules["srv"]
    rt = modules["rt"]

    events_dir = tmp_path / "rerank_events"
    weights_dir = tmp_path / "rerank_weights"
    events_dir.mkdir()
    weights_dir.mkdir()

    coll = test_collection
    monkeypatch.setenv("QDRANT_URL", qdrant_url)
    monkeypatch.setenv("COLLECTION_NAME", coll)
    monkeypatch.setenv("USE_TREE_SITTER", "0")
    monkeypatch.setenv("HYBRID_IN_PROCESS", "1")
    monkeypatch.setenv("HYBRID_EXPAND", "0")
    monkeypatch.setenv("SEMANTIC_EXPANSION_ENABLED", "0")
    monkeypatch.setenv("RERANKER_ENABLED", "0")
    monkeypatch.setenv("EMBEDDING_MODEL", "fake")
    monkeypatch.setenv("RERANK_EVENTS_DIR", str(events_dir))
    monkeypatch.setenv("RERANKER_WEIGHTS_DIR", str(weights_dir))
    monkeypatch.setenv("RELEVANCE_TRAINER_MIN_EVENTS", "1")
    monkeypatch.setenv("RELEVANCE_BOOST_FACTOR", "0.5")
    _stub_embeddings(monkeypatch, modules)

    _make_tiny_repo(tmp_path)
    ing.index_repo(
        root=tmp_path, qdrant_url=qdrant_url, api_key="",
        collection=coll, model_name="fake", recreate=True,
    )

    res = asyncio.run(
        srv.repo_search(queries=["authenticate"], limit=5, compact=False, rerank_enabled=False)
    )
    results = res.get("results", [])
    assert len(results) >= 1

    for r in results:
        rid = r.get("result_id", "")
        assert len(rid) == 12, f"result_id should be 12 hex chars, got: {rid!r}"
        assert all(c in "0123456789abcdef" for c in rid), f"result_id not hex: {rid!r}"
        assert r.get("target_id") == rid
        assert len(r.get("impression_id", "")) == 12

    res_compact = asyncio.run(
        srv.repo_search(queries=["authenticate"], limit=5, compact=True, rerank_enabled=False)
    )
    compact_results = res_compact.get("results", [])
    assert compact_results
    assert all(len(r.get("result_id", "")) == 12 for r in compact_results)

    rated_rid = results[0]["result_id"]

    rating_res = asyncio.run(
        srv.rate_search_results(
            query="authenticate",
            ratings=[{"result_id": rated_rid, "relevance": 2}],
            collection=coll,
        )
    )
    assert rating_res.get("ok"), f"rate_search_results failed: {rating_res}"
    assert rating_res.get("rated") == 1

    event_files = list(events_dir.glob(f"events_{coll}_*.ndjson"))
    assert len(event_files) > 0, "No event files written"

    with open(event_files[0], "r") as f:
        event = json.loads(f.readline())
    assert event["type"] == "relevance_feedback"
    assert event["query"] == "authenticate"
    assert event["collection"] == coll
    assert event["source"] == "mcp_tool"
    assert event["ratings"][0]["result_id"] == rated_rid
    assert event["ratings"][0]["target_id"] == rated_rid
    assert event["ratings"][0]["path"]

    trainer_res = rt.process_collection(coll)
    assert trainer_res["collection"] == coll
    assert trainer_res["events"] >= 1
    assert not trainer_res.get("skipped"), f"Trainer skipped: {trainer_res.get('reason')}"
    assert trainer_res["new_entries"] >= 1

    weight_file = weights_dir / f"{coll}_relevance.json"
    assert weight_file.exists(), f"Weight file not found at {weight_file}"

    weights = json.loads(weight_file.read_text())
    assert "results" in weights
    assert rated_rid in weights["results"], f"Rated result_id {rated_rid} not in weights"
    assert weights["results"][rated_rid]["avg_relevance"] == 2.0
    assert weights["results"][rated_rid]["target"]["path"]

    boosted = asyncio.run(
        srv.repo_search(
            queries=["authenticate"],
            limit=5,
            compact=False,
            debug=True,
            rerank_enabled=False,
        )
    )
    boosted_results = boosted.get("results", [])
    rated_result = next((r for r in boosted_results if r.get("result_id") == rated_rid), None)
    assert rated_result is not None, "Rated result disappeared from results"
    assert rated_result.get("relevance_boost", 0) > 0
