import asyncio
import json
import pytest

# These tests exercise argument plumbing independent of live retrieval.

@pytest.mark.asyncio
async def test_per_path_zero_is_echoed_and_respected_in_args(monkeypatch):
    from scripts.mcp_impl.search import _repo_search_impl

    async def _fake_run_async(_cmd, **_kwargs):
        item = {"path": "src/a.py", "start_line": 1, "end_line": 1, "score": 1.0}
        return {"ok": True, "code": 0, "stdout": json.dumps(item), "stderr": ""}

    monkeypatch.setenv("HYBRID_IN_PROCESS", "0")

    # Arg-plumbing test for the hybrid/subprocess (non-dense) path; mode is explicit by design.
    res = await _repo_search_impl(
        query="anything",
        limit=3,
        per_path=0,
        mode="hybrid",
        require_auth_session_fn=lambda session: session,
        run_async_fn=_fake_run_async,
    )
    assert isinstance(res, dict)
    args = res.get("args") or {}
    assert args.get("per_path") == 0, f"expected per_path echoed as 0, got {args.get('per_path')}"


@pytest.mark.asyncio
async def test_compact_string_false_is_normalized_in_args(monkeypatch):
    from scripts.mcp_impl.search import _repo_search_impl

    async def _fake_run_async(_cmd, **_kwargs):
        item = {"path": "src/a.py", "start_line": 1, "end_line": 1, "score": 1.0}
        return {"ok": True, "code": 0, "stdout": json.dumps(item), "stderr": ""}

    monkeypatch.setenv("HYBRID_IN_PROCESS", "0")

    # Passing compact as a string "false" should normalize to False in echoed args.
    # Keep mode explicit so dense-default env does not alter this contract test.
    res = await _repo_search_impl(
        query="anything",
        limit=1,
        compact="false",
        mode="hybrid",
        require_auth_session_fn=lambda session: session,
        run_async_fn=_fake_run_async,
    )
    assert isinstance(res, dict)
    args = res.get("args") or {}
    assert args.get("compact") is False, f"expected compact False, got {args.get('compact')}"
