import sys
import types
import json

from scripts.mcp_impl.search import (
    _RECENT_RESULT_META,
    _feedback_recall_candidates,
    _inject_result_ids,
    _remember_result_metadata,
    enrich_feedback_rating,
)
from scripts.relevance_feedback import (
    build_symbol_reconciliations,
    reconcile_collection_weights,
)
from scripts.relevance_trainer import aggregate_ratings, process_collection


def _result(file_hash: str) -> dict:
    return {
        "path": "/repo/pkg/auth.py",
        "container_path": "/work/repo/pkg/auth.py",
        "repo": "repo",
        "kind": "function",
        "symbol": "authenticate",
        "start_line": 10,
        "end_line": 20,
        "file_hash": file_hash,
    }


def test_result_id_survives_content_hash_changes():
    before = _result("hash-before")
    after = _result("hash-after")

    _inject_result_ids([before], "authenticate user")
    _inject_result_ids([after], "authenticate user")

    assert before["result_id"] == after["result_id"]
    assert before["target_id"] == after["target_id"]
    assert before["impression_id"] != after["impression_id"]


def test_result_id_is_stable_for_same_query_location_and_content():
    first = _result("same-hash")
    second = _result("same-hash")

    _inject_result_ids([first], "authenticate user")
    _inject_result_ids([second], "authenticate user")

    assert first["result_id"] == second["result_id"]
    assert first["impression_id"] == second["impression_id"]
    assert len(first["result_id"]) == 12


def test_result_id_survives_line_shifts_for_same_symbol():
    before = _result("same-hash")
    after = _result("same-hash")
    after["start_line"] = 50
    after["end_line"] = 80

    _inject_result_ids([before], "authenticate user")
    _inject_result_ids([after], "authenticate user")

    assert before["result_id"] == after["result_id"]
    assert before["impression_id"] != after["impression_id"]


def test_trainer_preserves_target_metadata_for_recall():
    result = _result("same-hash")
    _inject_result_ids([result], "authenticate user")

    weights = aggregate_ratings([
        {
            "type": "relevance_feedback",
            "ratings": [
                {
                    "result_id": result["result_id"],
                    "relevance": 2,
                    "target_id": result["target_id"],
                    "impression_id": result["impression_id"],
                    "path": result["path"],
                    "container_path": result["container_path"],
                    "symbol": result["symbol"],
                    "kind": result["kind"],
                    "repo": result["repo"],
                    "file_hash": result["file_hash"],
                }
            ],
        }
    ])

    entry = weights[result["result_id"]]
    assert entry["avg_relevance"] == 2
    assert entry["target"]["symbol"] == "authenticate"
    assert entry["target"]["container_path"] == "/work/repo/pkg/auth.py"


def test_feedback_rating_enriches_from_recent_search_result():
    result = _result("same-hash")
    _inject_result_ids([result], "authenticate user")
    _remember_result_metadata([result])

    rating = enrich_feedback_rating({
        "result_id": result["result_id"],
        "relevance": 2,
    })

    assert rating["target_id"] == result["target_id"]
    assert rating["container_path"] == "/work/repo/pkg/auth.py"
    assert rating["symbol"] == "authenticate"


def test_feedback_rating_enriches_from_shared_storage(tmp_path, monkeypatch):
    monkeypatch.setenv("RERANKER_WEIGHTS_DIR", str(tmp_path))
    result = _result("same-hash")
    result["symbol_content_hash"] = "symbol-hash"
    _inject_result_ids([result], "authenticate user")
    _remember_result_metadata([result], "repo-collection")

    _RECENT_RESULT_META.clear()
    rating = enrich_feedback_rating(
        {"result_id": result["result_id"], "relevance": 2},
        "repo-collection",
    )

    assert rating["symbol"] == "authenticate"
    assert rating["symbol_content_hash"] == "symbol-hash"


def test_exact_content_rename_reconciles_feedback_weight(tmp_path, monkeypatch):
    monkeypatch.setenv("RERANKER_WEIGHTS_DIR", str(tmp_path))
    old = {
        "function_authenticate_10": {
            "name": "authenticate",
            "type": "function",
            "content_hash": "same-body",
            "content": "def authenticate(user): return user",
        }
    }
    new = {
        "function_verify_user_20": {
            "name": "verify_user",
            "type": "function",
            "content_hash": "same-body",
            "content": "def authenticate(user): return user",
        }
    }
    before = _result("file-hash")
    _inject_result_ids([before], "authenticate")
    weights_file = tmp_path / "repo-collection_relevance.json"
    weights_file.write_text(json.dumps({
        "results": {
            before["result_id"]: {
                "total_relevance": 2,
                "count": 1,
                "avg_relevance": 2.0,
                "target": {
                    "target_id": before["result_id"],
                    "repo": "repo",
                    "kind": "function",
                    "symbol": "authenticate",
                    "path": "/repo/pkg/auth.py",
                },
            }
        }
    }))

    mappings = build_symbol_reconciliations(
        old,
        new,
        repo="repo",
        path="/repo/pkg/auth.py",
    )
    assert reconcile_collection_weights("repo-collection", mappings) == 1

    data = json.loads(weights_file.read_text())
    successor = next(
        entry
        for rid, entry in data["results"].items()
        if rid != before["result_id"] and entry.get("target", {}).get("symbol") == "verify_user"
    )
    assert successor["inheritance_weight"] == 1.0
    assert successor["lineage"][0]["reason"] == "rename_exact_content"


def test_symbol_split_divides_inherited_feedback(tmp_path, monkeypatch):
    monkeypatch.setenv("RERANKER_WEIGHTS_DIR", str(tmp_path))
    old = {
        "function_process_1": {
            "name": "process",
            "type": "function",
            "content_hash": "old",
            "content": "def process order validate payment persist receipt notify customer",
        }
    }
    new = {
        "function_validate_order_1": {
            "name": "validate_order",
            "type": "function",
            "content_hash": "new-a",
            "content": "def validate_order order validate payment customer",
        },
        "function_persist_receipt_10": {
            "name": "persist_receipt",
            "type": "function",
            "content_hash": "new-b",
            "content": "def persist_receipt persist receipt notify customer",
        },
    }
    mappings = build_symbol_reconciliations(
        old,
        new,
        repo="repo",
        path="/repo/pkg/orders.py",
        split_min_overlap=0.35,
        split_min_coverage=0.7,
    )

    successors = next(iter(mappings.values()))
    assert {item["target"]["symbol"] for item in successors} == {
        "validate_order",
        "persist_receipt",
    }
    assert round(sum(item["inheritance_weight"] for item in successors), 6) == 1.0
    assert all(item["reason"] == "split_token_coverage" for item in successors)


def test_trainer_preserves_existing_reconciled_entries(tmp_path, monkeypatch):
    events_dir = tmp_path / "events"
    weights_dir = tmp_path / "weights"
    events_dir.mkdir()
    weights_dir.mkdir()
    monkeypatch.setenv("RERANK_EVENTS_DIR", str(events_dir))
    monkeypatch.setenv("RERANKER_WEIGHTS_DIR", str(weights_dir))
    monkeypatch.setenv("RELEVANCE_TRAINER_MIN_EVENTS", "1")
    (weights_dir / "repo_relevance.json").write_text(json.dumps({
        "results": {
            "inherited": {
                "total_relevance": 2,
                "count": 1,
                "avg_relevance": 2.0,
                "inheritance_weight": 0.5,
                "target": {"symbol": "split_child"},
            }
        }
    }))
    (events_dir / "events_repo_2026060902.ndjson").write_text(json.dumps({
        "type": "relevance_feedback",
        "ratings": [{"result_id": "fresh", "relevance": 2}],
    }) + "\n")

    process_collection("repo")
    data = json.loads((weights_dir / "repo_relevance.json").read_text())
    assert "inherited" in data["results"]
    assert "fresh" in data["results"]


def test_graph_recall_adds_callers_when_rated_target_already_present(monkeypatch):
    class FakeMatchValue:
        def __init__(self, value):
            self.value = value

    class FakeFieldCondition:
        def __init__(self, key, match):
            self.key = key
            self.match = match

    class FakeFilter:
        def __init__(self, must=None):
            self.must = must or []

    class FakeModels:
        MatchValue = FakeMatchValue
        FieldCondition = FakeFieldCondition
        Filter = FakeFilter

    class FakePoint:
        def __init__(self, payload):
            self.payload = payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def scroll(self, collection_name, scroll_filter, **kwargs):
            terms = {cond.key: cond.match.value for cond in scroll_filter.must}
            if collection_name.endswith("_graph"):
                if terms.get("edge_type") == "calls" and terms.get("callee_symbol") == "foo":
                    return [FakePoint({
                        "caller_path": "/work/repo/pkg/caller.py",
                        "repo": "repo",
                    })], None
                return [], None

            if terms.get("metadata.path") == "/work/repo/pkg/caller.py":
                return [FakePoint({
                    "metadata": {
                        "path": "/work/repo/pkg/caller.py",
                        "host_path": "/repo/pkg/caller.py",
                        "container_path": "/work/repo/pkg/caller.py",
                        "repo": "repo",
                        "kind": "function",
                        "symbol": "caller",
                        "symbol_path": "caller",
                        "start_line": 1,
                        "end_line": 5,
                    }
                })], None

            return [FakePoint({
                "metadata": {
                    "path": "/work/repo/pkg/foo.py",
                    "repo": "repo",
                    "kind": "function",
                    "symbol": "foo",
                    "symbol_path": "foo",
                    "start_line": 1,
                    "end_line": 5,
                }
            })], None

    fake_qdrant = types.SimpleNamespace(QdrantClient=FakeClient, models=FakeModels)
    monkeypatch.setitem(sys.modules, "qdrant_client", fake_qdrant)
    monkeypatch.setenv("RELEVANCE_GRAPH_RECALL_MAX", "2")

    weights = {
        "results": {
            "rated-target": {
                "avg_relevance": 2,
                "count": 1,
                "target": {
                    "repo": "repo",
                    "kind": "function",
                    "symbol": "foo",
                    "container_path": "/work/repo/pkg/foo.py",
                },
            }
        }
    }

    recalled = _feedback_recall_candidates(
        collection="repo",
        weights=weights,
        existing_target_ids={"rated-target"},
        existing_paths={"/repo/pkg/foo.py"},
        base_score=0.5,
        max_candidates=3,
    )

    assert len(recalled) == 1
    assert recalled[0]["feedback_graph_recall"] is True
    assert recalled[0]["path"] == "/repo/pkg/caller.py"
