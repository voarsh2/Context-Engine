import importlib


rr = importlib.import_module("scripts.rerank_tools.local")


class _Pt:
    def __init__(self, pid: str, path: str):
        self.id = pid
        self.payload = {
            "metadata": {
                "path": path,
                "start_line": 1,
                "end_line": 2,
                "symbol": "f",
            }
        }


class _FakeModel:
    def embed(self, texts):
        for _ in texts:
            yield [0.01] * 8


def test_rerank_in_process_under_excludes_out_of_scope(monkeypatch):
    monkeypatch.setattr(rr, "QdrantClient", lambda *a, **k: object())
    monkeypatch.setattr(rr, "_select_dense_vector_name", lambda *a, **k: "vec")
    monkeypatch.setattr(
        rr,
        "dense_results",
        lambda *a, **k: [_Pt("1", "/work/repo/direct/tools/b.py")],
    )
    monkeypatch.setattr(rr, "rerank_local", lambda pairs: [0.9] * len(pairs))

    out = rr.rerank_in_process(
        query="rotate heading",
        topk=10,
        limit=5,
        under="space",
        model=_FakeModel(),
        collection="codebase",
    )
    assert out == []


def test_rerank_in_process_under_keeps_in_scope(monkeypatch):
    monkeypatch.setattr(rr, "QdrantClient", lambda *a, **k: object())
    monkeypatch.setattr(rr, "_select_dense_vector_name", lambda *a, **k: "vec")
    monkeypatch.setattr(
        rr,
        "dense_results",
        lambda *a, **k: [_Pt("1", "/work/repo/space/ship/a.py")],
    )
    monkeypatch.setattr(rr, "rerank_local", lambda pairs: [0.9] * len(pairs))

    out = rr.rerank_in_process(
        query="rotate heading",
        topk=10,
        limit=5,
        under="space",
        model=_FakeModel(),
        collection="codebase",
    )
    assert len(out) == 1
    assert out[0]["path"] == "/work/repo/space/ship/a.py"
