import asyncio
import sys
import threading
import types
import pytest

from scripts.mcp_impl.context_answer import (
    _ca_prepare_filters_and_retrieve,
    _context_answer_impl,
)


def _run_context_answer(**kwargs):
    return asyncio.get_event_loop().run_until_complete(
        _context_answer_impl(
            **kwargs,
            get_embedding_model_fn=lambda *a, **k: None,
            env_lock=threading.Lock(),
            prepare_filters_and_retrieve_fn=_ca_prepare_filters_and_retrieve,
        )
    )


def _install_fake_hybrid(monkeypatch, run_hybrid_search):
    fake = types.ModuleType("scripts.hybrid_search")
    fake.run_hybrid_search = run_hybrid_search
    fake.lang_matches_path = lambda language, path: True
    fake._merge_and_budget_spans = lambda items: list(items or [])
    monkeypatch.setitem(sys.modules, "scripts.hybrid_search", fake)
    return fake


def _isolate_context_answer_unit(monkeypatch):
    monkeypatch.setenv("REFRAG_RUNTIME", "llamacpp")
    monkeypatch.setenv("CTX_MULTI_COLLECTION", "0")
    monkeypatch.setenv("CTX_DOC_PASS", "0")
    monkeypatch.setenv("CTX_DOC_TOP_FALLBACK", "0")


@pytest.mark.service
def test_context_answer_path_mention_fallback(monkeypatch):
    _isolate_context_answer_unit(monkeypatch)
    # Force retrieval to return nothing so path-mention fallback engages
    _install_fake_hybrid(monkeypatch, lambda **k: [])

    import scripts.refrag_llamacpp as ref

    class FakeLlama:
        def __init__(self, *a, **k):
            pass

        def generate_with_soft_embeddings(self, prompt: str, max_tokens: int = 64, **kw):
            # Should still include Sources and [1] with the mentioned file
            assert "Sources:" in prompt
            assert "[1]" in prompt
            return "ok [1]"

    monkeypatch.setattr(ref, "LlamaCppRefragClient", FakeLlama)
    monkeypatch.setattr(ref, "is_decoder_enabled", lambda: True)

    # Mention an actual file in this repo so fallback can find it
    q = "explain something in scripts/hybrid_search.py"
    out = _run_context_answer(query=q, limit=3, per_path=2)
    assert isinstance(out, dict)
    cits = out.get("citations") or []
    assert len(cits) >= 1
    # Either path or rel_path should indicate the file
    p = cits[0].get("path") or ""
    rp = cits[0].get("rel_path") or ""
    assert p.endswith("scripts/hybrid_search.py") or rp.endswith("scripts/hybrid_search.py")
