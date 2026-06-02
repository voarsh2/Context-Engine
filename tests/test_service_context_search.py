import importlib
import json
import sys
import types
import pytest

ctx_search = importlib.import_module("scripts.mcp_impl.context_search")


class FakeEmbed:
    class _Vec:
        def tolist(self):
            return [0.1] * 8

    def embed(self, texts):
        # return an iterator of vector-like objects with .tolist()
        for _ in texts:
            yield self._Vec()


@pytest.mark.service
@pytest.mark.asyncio
async def test_context_search_blend_compact(monkeypatch):
    # repo_search returns two code hits (async stub)
    async def fake_repo_search(**kwargs):
        return {
            "results": [
                {"score": 0.8, "path": "/x/a.py", "start_line": 1, "end_line": 3},
                {"score": 0.6, "path": "/x/b.py", "start_line": 5, "end_line": 9},
            ]
        }

    monkeypatch.setenv("MEMORY_SSE_ENABLED", "1")
    monkeypatch.setenv("MEMORY_COLLECTION_NAME", "test-memory")
    monkeypatch.setenv("MEMORY_MCP_READY_RETRIES", "1")
    monkeypatch.setenv("MEMORY_MCP_READY_BACKOFF", "0")
    monkeypatch.setenv("MEMORY_MCP_LIST_RETRIES", "1")
    monkeypatch.setenv("MEMORY_MCP_LIST_BACKOFF", "0")

    import urllib.request

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *a, **k: (_ for _ in ()).throw(OSError("not ready")),
    )

    class T:
        def __init__(self, name):
            self.name = name

    class Item:
        def __init__(self, text):
            self.text = text

    class Resp:
        def __init__(self):
            self.content = [Item("foo note one"), Item("bar note two")]

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def list_tools(self):
            return [T("find")]

        async def call_tool(self, *a, **k):
            return Resp()

    monkeypatch.setitem(
        sys.modules,
        "fastmcp",
        types.SimpleNamespace(Client=lambda *a, **k: FakeClient()),
    )

    res = await ctx_search._context_search_impl(
        query="foo bar",
        limit=3,
        per_path=1,
        include_memories=True,
        memory_weight=0.5,
        compact=True,
        repo_search_fn=fake_repo_search,
    )

    assert "results" in res
    # Compact shape: code entries have path+lines; memory entries have content only
    for it in res["results"]:
        if it.get("source") == "code":
            assert "path" in it and "start_line" in it and "end_line" in it
        else:
            assert "content" in it and len(it["content"]) > 0


@pytest.mark.service
@pytest.mark.asyncio
async def test_context_search_weight_scaling(monkeypatch):
    # repo_search returns one code hit (async stub)
    async def fake_repo_search(**kwargs):
        return {
            "results": [
                {"score": 0.5, "path": "/x/a.py", "start_line": 1, "end_line": 3}
            ]
        }

    # Force SSE memory path with a fake FastMCP client
    monkeypatch.setenv("MEMORY_SSE_ENABLED", "1")
    monkeypatch.setenv("MEMORY_COLLECTION_NAME", "test-memory")
    monkeypatch.setenv("MEMORY_MCP_READY_RETRIES", "1")
    monkeypatch.setenv("MEMORY_MCP_READY_BACKOFF", "0")
    monkeypatch.setenv("MEMORY_MCP_LIST_RETRIES", "1")
    monkeypatch.setenv("MEMORY_MCP_LIST_BACKOFF", "0")

    import urllib.request

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *a, **k: (_ for _ in ()).throw(OSError("not ready")),
    )

    class T:
        def __init__(self, name):
            self.name = name

    class Item:
        def __init__(self, text):
            self.text = text

    class Resp:
        def __init__(self):
            self.content = [Item("foo note")]

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def list_tools(self):
            return [T("find")]

        async def call_tool(self, *a, **k):
            return Resp()

    monkeypatch.setitem(
        sys.modules,
        "fastmcp",
        types.SimpleNamespace(Client=lambda *a, **k: FakeClient()),
    )

    res = await ctx_search._context_search_impl(
        query="foo",
        limit=2,
        per_path=1,
        include_memories=True,
        memory_weight=2.0,
        compact=False,
        repo_search_fn=fake_repo_search,
    )

    mem_scores = [r["score"] for r in res["results"] if r.get("source") == "memory"]
    code_scores = [r["score"] for r in res["results"] if r.get("source") == "code"]
    assert mem_scores, "expected at least one memory result"
    assert code_scores, "expected at least one code result"
    assert max(mem_scores) > max(code_scores)


@pytest.mark.service
@pytest.mark.asyncio
async def test_context_search_per_source_limits(monkeypatch):
    # repo_search returns three code hits
    async def fake_repo_search(**kwargs):
        return {
            "results": [
                {"score": 0.9, "path": "/x/a.py", "start_line": 1, "end_line": 3},
                {"score": 0.8, "path": "/x/b.py", "start_line": 4, "end_line": 6},
                {"score": 0.7, "path": "/x/c.py", "start_line": 7, "end_line": 9},
            ]
        }

    # Drive memory hits via SSE path with a fake FastMCP client yielding 3 notes
    monkeypatch.setenv("MEMORY_SSE_ENABLED", "1")
    monkeypatch.setenv("MEMORY_COLLECTION_NAME", "test-memory")
    monkeypatch.setenv("MEMORY_MCP_READY_RETRIES", "1")
    monkeypatch.setenv("MEMORY_MCP_READY_BACKOFF", "0")
    monkeypatch.setenv("MEMORY_MCP_LIST_RETRIES", "1")
    monkeypatch.setenv("MEMORY_MCP_LIST_BACKOFF", "0")

    import urllib.request

    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *a, **k: (_ for _ in ()).throw(OSError("not ready")),
    )

    class T:
        def __init__(self, name):
            self.name = name

    class Item:
        def __init__(self, text):
            self.text = text

    class Resp:
        def __init__(self):
            self.content = [Item("m1"), Item("m2"), Item("m3")]

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def list_tools(self):
            return [T("find")]

        async def call_tool(self, *a, **k):
            return Resp()

    monkeypatch.setitem(
        sys.modules,
        "fastmcp",
        types.SimpleNamespace(Client=lambda *a, **k: FakeClient()),
    )

    res = await ctx_search._context_search_impl(
        query="foo",
        limit=5,
        per_path=1,
        include_memories=True,
        per_source_limits=json.dumps({"code": 1, "memory": 2}),
        compact=True,
        repo_search_fn=fake_repo_search,
        get_embedding_model_fn=lambda *a, **k: FakeEmbed(),
    )

    kinds = [r.get("source") for r in res.get("results", [])]
    assert kinds.count("code") <= 1
    assert kinds.count("memory") <= 2
    # Ensure at least one of each (since both sources available)
    assert "code" in kinds and "memory" in kinds
