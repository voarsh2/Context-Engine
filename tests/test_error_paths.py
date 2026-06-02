import importlib
import sys
import types
import pytest

search_impl = importlib.import_module("scripts.mcp_impl.search")


@pytest.mark.service
def test_repo_search_malformed_jsonl_subprocess(monkeypatch):
    # Force subprocess path and simulate malformed JSONL stdout
    monkeypatch.setenv("HYBRID_IN_PROCESS", "0")
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "hybrid")
    monkeypatch.setenv("RERANKER_ENABLED", "0")

    fake_hybrid = types.ModuleType("scripts.hybrid_search")
    fake_hybrid.run_hybrid_search = lambda *a, **k: []
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", fake_hybrid)

    async def fake_run(cmd, **kwargs):
        # Simulate subprocess failure with malformed output
        return {"ok": False, "code": 1, "stdout": "not-json\n", "stderr": "malformed"}

    res = search_impl.asyncio.get_event_loop().run_until_complete(
        search_impl._repo_search_impl(
            queries=["x"],
            limit=1,
            compact=False,
            run_async_fn=fake_run,
            require_auth_session_fn=lambda session: session,
        )
    )

    assert res.get("ok") is False
    assert res.get("code", 1) != 0


@pytest.mark.service
def test_repo_search_inproc_qdrant_failure_fallback_and_fail(monkeypatch):
    # In-process hybrid raises (simulating Qdrant connectivity failure),
    # subprocess fallback also fails.
    monkeypatch.setenv("HYBRID_IN_PROCESS", "1")
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "hybrid")
    monkeypatch.setenv("RERANKER_ENABLED", "0")

    # Cause in-process path to fail
    def boom(*a, **k):
        raise ConnectionError("qdrant down")

    fake_hybrid = types.ModuleType("scripts.hybrid_search")
    fake_hybrid.run_hybrid_search = boom
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", fake_hybrid)

    # And make the subprocess fallback fail too
    async def fake_run(cmd, **kwargs):
        return {"ok": False, "code": 1, "stdout": "", "stderr": "qdrant unreachable"}

    res = search_impl.asyncio.get_event_loop().run_until_complete(
        search_impl._repo_search_impl(
            queries=["x"],
            limit=1,
            compact=True,
            get_embedding_model_fn=lambda *a, **k: object(),
            run_async_fn=fake_run,
            require_auth_session_fn=lambda session: session,
        )
    )

    assert res.get("ok") is False
    assert res.get("code", 0) != 0
    assert "stderr" in res or res.get("error")
