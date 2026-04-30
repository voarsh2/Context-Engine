import asyncio
import pytest

# These tests exercise argument plumbing independent of live retrieval.

@pytest.mark.asyncio
async def test_per_path_zero_is_echoed_and_respected_in_args(monkeypatch):
    from scripts.mcp_indexer_server import repo_search
    import scripts.mcp_indexer_server as srv

    async def _fake_run_async(_cmd, **_kwargs):
        return {"ok": True, "code": 0, "stdout": "", "stderr": ""}

    monkeypatch.setenv("HYBRID_IN_PROCESS", "0")
    monkeypatch.setattr(srv, "_run_async", _fake_run_async)

    # Arg-plumbing test for the hybrid/subprocess (non-dense) path; mode is explicit by design.
    res = await repo_search(query="anything", limit=3, per_path=0, mode="hybrid")
    assert isinstance(res, dict)
    args = res.get("args") or {}
    assert args.get("per_path") == 0, f"expected per_path echoed as 0, got {args.get('per_path')}"


@pytest.mark.asyncio
async def test_compact_string_false_is_normalized_in_args(monkeypatch):
    from scripts.mcp_indexer_server import repo_search
    import scripts.mcp_indexer_server as srv

    async def _fake_run_async(_cmd, **_kwargs):
        return {"ok": True, "code": 0, "stdout": "", "stderr": ""}

    monkeypatch.setenv("HYBRID_IN_PROCESS", "0")
    monkeypatch.setattr(srv, "_run_async", _fake_run_async)

    # Passing compact as a string "false" should normalize to False in echoed args.
    # Keep mode explicit so dense-default env does not alter this contract test.
    res = await repo_search(query="anything", limit=1, compact="false", mode="hybrid")
    assert isinstance(res, dict)
    args = res.get("args") or {}
    assert args.get("compact") is False, f"expected compact False, got {args.get('compact')}"
