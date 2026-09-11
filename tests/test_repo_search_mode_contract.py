import asyncio
import importlib
import sys
import types

import pytest

srv = importlib.import_module("scripts.mcp_indexer_server")


def _make_hybrid_module_stub(calls: dict):
    mod = types.ModuleType("scripts.hybrid_search")

    def run_pure_dense_search(**kwargs):
        calls["dense"] = int(calls.get("dense", 0)) + 1
        calls["dense_kwargs"] = dict(kwargs)
        return [
            {
                "score": 0.91,
                "path": "/work/dense.py",
                "symbol": "",
                "start_line": 1,
                "end_line": 3,
                "payload": {},
            }
        ]

    def run_hybrid_search(**kwargs):
        calls["hybrid"] = int(calls.get("hybrid", 0)) + 1
        calls["hybrid_kwargs"] = dict(kwargs)
        return [
            {
                "score": 0.75,
                "path": "/work/hybrid.py",
                "symbol": "",
                "start_line": 4,
                "end_line": 7,
            }
        ]

    mod.run_pure_dense_search = run_pure_dense_search
    mod.run_hybrid_search = run_hybrid_search
    mod.lang_matches_path = lambda path, lang=None: True
    mod._merge_and_budget_spans = lambda spans, *args, **kwargs: spans
    mod.TextEmbedding = object
    mod.QdrantClient = object
    return mod


@pytest.mark.service
def test_repo_search_dense_default_from_env_is_explicit_and_stable(monkeypatch):
    # Contract: global default mode should route repo_search to dense path when set to dense.
    calls = {"dense": 0, "hybrid": 0}
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "dense")
    monkeypatch.setenv("HYBRID_IN_PROCESS", "1")
    monkeypatch.setattr(srv, "_get_embedding_model", lambda *a, **k: object())
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", _make_hybrid_module_stub(calls))

    res = asyncio.run(srv.repo_search(query="q", limit=1, compact=True, rerank_enabled=False))

    assert res.get("ok") is True
    assert calls["dense"] == 1
    assert calls["hybrid"] == 0
    assert res.get("results", [{}])[0].get("path") == "/work/dense.py"


@pytest.mark.service
def test_repo_search_explicit_hybrid_overrides_dense_default_for_non_dense_tests(monkeypatch):
    # Contract: non-dense tests can force hybrid behavior even under dense global default.
    calls = {"dense": 0, "hybrid": 0}
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "dense")
    monkeypatch.setenv("HYBRID_IN_PROCESS", "1")
    monkeypatch.setattr(srv, "_get_embedding_model", lambda *a, **k: object())
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", _make_hybrid_module_stub(calls))

    res = asyncio.run(
        srv.repo_search(
            query="q",
            mode="hybrid",
            limit=1,
            compact=True,
            rerank_enabled=False,
        )
    )

    assert res.get("ok") is True
    assert calls["dense"] == 0
    assert calls["hybrid"] == 1
    assert res.get("results", [{}])[0].get("path") == "/work/hybrid.py"


@pytest.mark.service
def test_repo_search_dense_default_forwards_structured_filters(monkeypatch):
    calls = {"dense": 0, "hybrid": 0}
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "dense")
    monkeypatch.setenv("HYBRID_IN_PROCESS", "1")
    monkeypatch.setattr(srv, "_get_embedding_model", lambda *a, **k: object())
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", _make_hybrid_module_stub(calls))

    asyncio.run(
        srv.repo_search(
            query="q",
            limit=1,
            compact=True,
            rerank_enabled=False,
            kind="function",
            symbol="my_symbol",
            ext="py",
        )
    )

    assert calls["dense"] == 1
    dense_kwargs = calls.get("dense_kwargs") or {}
    assert dense_kwargs.get("kind") == "function"
    assert dense_kwargs.get("symbol") == "my_symbol"
    assert dense_kwargs.get("ext") == "py"


@pytest.mark.service
def test_repo_search_dense_default_forwards_per_path(monkeypatch):
    calls = {"dense": 0, "hybrid": 0}
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "dense")
    monkeypatch.setenv("HYBRID_IN_PROCESS", "1")
    monkeypatch.setattr(srv, "_get_embedding_model", lambda *a, **k: object())
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", _make_hybrid_module_stub(calls))

    asyncio.run(
        srv.repo_search(
            query="q",
            limit=3,
            per_path=1,
            compact=True,
            rerank_enabled=False,
        )
    )

    assert calls["dense"] == 1
    assert calls.get("dense_kwargs", {}).get("per_path") == 1


@pytest.mark.service
def test_repo_search_profile_tests_adds_material_globs(monkeypatch):
    calls = {"dense": 0, "hybrid": 0}
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "dense")
    monkeypatch.setenv("HYBRID_IN_PROCESS", "1")
    monkeypatch.setattr(srv, "_get_embedding_model", lambda *a, **k: object())
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", _make_hybrid_module_stub(calls))

    res = asyncio.run(
        srv.repo_search(
            query="q",
            profile="tests",
            limit=1,
            compact=False,
            rerank_enabled=False,
        )
    )

    args = res.get("args") or {}
    assert args.get("profile") == "tests"
    assert "tests/**" in args.get("path_glob", [])
    assert "**/*_test.*" in args.get("path_glob", [])


@pytest.mark.service
def test_repo_search_profile_preserves_user_globs(monkeypatch):
    calls = {"dense": 0, "hybrid": 0}
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "dense")
    monkeypatch.setenv("HYBRID_IN_PROCESS", "1")
    monkeypatch.setattr(srv, "_get_embedding_model", lambda *a, **k: object())
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", _make_hybrid_module_stub(calls))

    res = asyncio.run(
        srv.repo_search(
            query="q",
            profile="config",
            path_glob=["custom/**"],
            limit=1,
            compact=False,
            rerank_enabled=False,
        )
    )

    globs = (res.get("args") or {}).get("path_glob", [])
    assert globs[0] == "custom/**"
    assert "**/*.yaml" in globs
