import importlib
import pytest

search_impl = importlib.import_module("scripts.mcp_impl.search")


@pytest.mark.service
def test_rerank_timeout_floor_and_env_defaults(monkeypatch):
    # Force rerank via env default when arg not provided
    monkeypatch.setenv("RERANKER_ENABLED", "1")
    monkeypatch.setenv("RERANK_IN_PROCESS", "0")
    monkeypatch.setenv("HYBRID_IN_PROCESS", "0")
    monkeypatch.setenv("REPO_SEARCH_DEFAULT_MODE", "hybrid")

    # Floor 1500ms; client asks 200ms -> effective >= 1500ms -> 1.5s
    monkeypatch.setenv("RERANK_TIMEOUT_FLOOR_MS", "1500")
    # Fix default timeout for test determinism (CI may set a higher value)
    monkeypatch.setenv("RERANKER_TIMEOUT_MS", "200")

    # Fake subprocess runner to capture hybrid + rerank calls without loading the MCP facade.
    calls = []

    async def fake_run(cmd, env=None, timeout=None):
        calls.append({"cmd": cmd, "timeout": timeout})
        # Distinguish hybrid vs rerank by module name
        if "scripts.rerank_tools.local" in " ".join(map(str, cmd)):
            # Return something that looks like rerank stdout
            return {
                "ok": True,
                "stdout": "0.9\t/path.py\t\t1-3\n",
                "stderr": "",
                "code": 0,
            }
        else:
            # Hybrid JSONL minimal
            return {
                "ok": True,
                "stdout": '{"score": 0.1, "path": "/p", "start_line": 1, "end_line": 2}\n',
                "stderr": "",
                "code": 0,
            }

    # Call repo_search with no rerank_enabled arg to pick env default
    res = search_impl.asyncio.get_event_loop().run_until_complete(
        search_impl._repo_search_impl(
            query="foo",
            limit=3,
            per_path=1,
            run_async_fn=fake_run,
            require_auth_session_fn=lambda session: session,
        )
    )

    assert any(
        "scripts.rerank_tools.local" in " ".join(map(str, c["cmd"])) for c in calls
    ), "rerank subprocess should be invoked"
    # find rerank call
    rc = next(c for c in calls if "scripts.rerank_tools.local" in " ".join(map(str, c["cmd"])))
    assert rc["timeout"] >= 1.5 and rc["timeout"] <= 2.0
    assert res["used_rerank"] is True
