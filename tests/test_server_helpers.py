import json
import asyncio
import types
import importlib
from pathlib import Path

srv = importlib.import_module("scripts.mcp_indexer_server")
admin_tools = importlib.import_module("scripts.mcp_impl.admin_tools")


def test_tokens_from_queries_basic():
    toks = srv._tokens_from_queries(["FooBar baz42", "baz-42 foo"])
    # Lowercased, split on camel/snake/digits, dedup preserving order
    assert toks[0] == "foo" and "bar" in toks and "baz" in toks and "42" in toks


def test_highlight_snippet_simple():
    s = "hello foo bar"
    out = srv._highlight_snippet(s, ["foo", "bar"])
    assert "<<foo>>" in out and "<<bar>>" in out


def test_detect_repo_from_work_path_ignores_invalid_root_git(tmp_path, monkeypatch):
    """A metadata-only /work/.git must not be treated as repo name "work"."""
    work = tmp_path / "work"
    (work / ".git" / ".codebase").mkdir(parents=True)
    monkeypatch.delenv("CURRENT_REPO", raising=False)
    monkeypatch.delenv("REPO_NAME", raising=False)
    monkeypatch.setattr(admin_tools, "Path", lambda value: work if value == "/work" else Path(value))

    assert admin_tools._detect_current_repo() is None


def test_detect_repo_from_work_path_skips_internal_dirs(tmp_path, monkeypatch):
    work = tmp_path / "work"
    (work / ".git").mkdir(parents=True)
    (work / ".codebase" / ".git").mkdir(parents=True)
    (work / "__pycache__" / ".git").mkdir(parents=True)
    (work / "real-repo" / ".git").mkdir(parents=True)
    monkeypatch.delenv("CURRENT_REPO", raising=False)
    monkeypatch.delenv("REPO_NAME", raising=False)
    monkeypatch.setenv("CTXCE_BINDMOUNT_REPO_DETECTION", "1")
    monkeypatch.setattr(admin_tools, "Path", lambda value: work if value == "/work" else Path(value))

    assert admin_tools._detect_current_repo() == "real-repo"


def test_detect_repo_from_work_path_skips_git_without_bindmount_mode(tmp_path, monkeypatch):
    work = tmp_path / "work"
    (work / "real-repo" / ".git").mkdir(parents=True)
    monkeypatch.delenv("CURRENT_REPO", raising=False)
    monkeypatch.delenv("REPO_NAME", raising=False)
    monkeypatch.delenv("CTXCE_BINDMOUNT_REPO_DETECTION", raising=False)
    monkeypatch.setattr(admin_tools, "Path", lambda value: work if value == "/work" else Path(value))

    assert admin_tools._detect_current_repo() is None


def fake_async_run_factory(text):
    async def _fake(cmd, **kwargs):  # accept env/timeout/cwd
        return {"ok": True, "code": 0, "stdout": text, "stderr": ""}

    return _fake


async def _call_repo_search(**kwargs):
    return await srv.repo_search(**kwargs)


def test_repo_search_arg_normalization(monkeypatch, tmp_path):
    # Prepare a temp file to allow snippet read
    p = tmp_path / "a.py"
    p.write_text("def f():\n    return 1\n")

    # Fake hybrid_search JSONL output (1 item)
    item = {
        "score": 0.9,
        "path": str(p),
        "symbol": "f",
        "start_line": 1,
        "end_line": 2,
        "components": {"test": 1.0},
        "why": "test",
    }
    jsonl = json.dumps(item) + "\n"

    # Monkeypatch async runner
    monkeypatch.setattr(srv, "_run_async", fake_async_run_factory(jsonl))
    # Avoid model loading
    monkeypatch.setattr(srv, "_get_embedding_model", lambda *a, **k: None)

    # Ensure in-process branch stays off
    monkeypatch.delenv("HYBRID_IN_PROCESS", raising=False)

    res = asyncio.run(
        _call_repo_search(
            queries=["FooBar"],
            limit="12",  # str on purpose to test coercion
            # This test targets arg normalization + JSONL shaping from the non-dense path.
            # Keep mode explicit so global dense defaults don't change behavior here.
            mode="hybrid",
            per_path=None,
            language=None,
            under=None,
            kind=None,
            symbol=None,
            ext=None,
            not_=None,  # Fixed: was not_filter
            case=None,
            path_regex=None,
            path_glob=None,
            not_glob=None,
            include_snippet=True,
            compact=True,
        )
    )

    assert res.get("ok") is True
    assert res.get("used_rerank") in (False, None)
    assert len(res.get("results", [])) == 1
    args = res.get("args", {})
    assert isinstance(args.get("limit"), int) and args.get("limit") == 12
    assert isinstance(args.get("compact"), bool) and args.get("compact") is True
    # snippet highlighting applied
    if res["results"][0].get("snippet"):
        assert "<<" in res["results"][0]["snippet"]
