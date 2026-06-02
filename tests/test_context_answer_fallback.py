import asyncio
import sys
import types
import pytest

from scripts.mcp_impl.context_answer import _context_answer_impl


@pytest.mark.asyncio
async def test_context_answer_has_no_filesystem_fallback_when_no_hits(monkeypatch):
    """When retrieval yields no spans, we do NOT glob or read the host filesystem.
    Citations may be empty, and that's expected.
    """
    fake_hybrid = types.ModuleType("scripts.hybrid_search")
    fake_hybrid.run_hybrid_search = lambda **k: []
    fake_hybrid.lang_matches_path = lambda language, path: True
    fake_hybrid._merge_and_budget_spans = lambda items: list(items or [])

    monkeypatch.setenv("REFRAG_RUNTIME", "llamacpp")
    monkeypatch.setenv("CTX_MULTI_COLLECTION", "0")
    monkeypatch.setenv("CTX_DOC_PASS", "0")
    monkeypatch.setenv("CTX_DOC_TOP_FALLBACK", "0")
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", fake_hybrid)

    def _empty_retrieval(**_kwargs):
        return {
            "items": [],
            "eff_language": "nonexistentlang",
            "eff_path_glob": ["scripts/hybrid_search.py"],
            "eff_not_glob": [],
            "override_under": None,
            "sym_arg": None,
            "cwd_root": "/work",
            "path_regex": None,
            "ext": None,
            "kind": None,
            "case": None,
        }

    out = await _context_answer_impl(
        query="Describe module roles",
        limit=3,
        per_path=1,
        include_snippet=True,
        path_glob=["scripts/hybrid_search.py"],
        language="nonexistentlang",
        get_embedding_model_fn=lambda *_args, **_kwargs: None,
        prepare_filters_and_retrieve_fn=_empty_retrieval,
    )
    assert isinstance(out, dict)
    # No fallback: citations can be empty
    cits = out.get("citations") or []
    assert len(cits) == 0
