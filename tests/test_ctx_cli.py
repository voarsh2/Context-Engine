import sys

import scripts.ctx as ctx


def test_parse_mcp_response_prefers_structured_content():
    payload = {
        "result": {
            "content": [{"type": "text", "text": '{"result":{"results":[]}}'}],
            "structuredContent": {
                "result": {"results": [{"path": "structured.py"}], "total": 1}
            },
        }
    }

    assert ctx.parse_mcp_response(payload) == {
        "results": [{"path": "structured.py"}],
        "total": 1,
    }


def test_parse_mcp_response_unwraps_text_result_payload():
    payload = {
        "result": {
            "content": [
                {
                    "type": "text",
                    "text": '{"result":{"results":[{"path":"text.py"}],"total":1}}',
                }
            ]
        }
    }

    assert ctx.parse_mcp_response(payload) == {
        "results": [{"path": "text.py"}],
        "total": 1,
    }


def test_main_with_context_appends_supporting_context(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["ctx.py", "--with-context", "where is dense search?"],
    )
    monkeypatch.setattr(
        ctx,
        "fetch_context",
        lambda *a, **k: (
            "- /work/scripts/hybrid_search.py:428-565 (run_pure_dense_search)",
            "",
        ),
    )
    monkeypatch.setattr(ctx, "rewrite_prompt", lambda *a, **k: "rewritten prompt")
    monkeypatch.setattr(
        ctx,
        "extract_allowed_citations",
        lambda *a, **k: ({"/work/scripts/hybrid_search.py"}, {}),
    )
    monkeypatch.setattr(ctx, "sanitize_citations", lambda text, *_: text)

    ctx.main()

    out = capsys.readouterr().out
    assert "rewritten prompt" in out
    assert "Supporting context:" in out
    assert "run_pure_dense_search" in out
