from types import SimpleNamespace

import pytest


pytestmark = pytest.mark.unit


class _FakeVec:
    def __init__(self, size):
        self.size = size


class _FakeCollections:
    def __init__(self, names):
        self.collections = [SimpleNamespace(name=name) for name in names]


class _FakeClient:
    collection_names = ["context-engine", "stale-empty"]
    valid_vector_collections = {"context-engine"}

    def __init__(self, *args, **kwargs):
        self.checked = []

    def get_collections(self):
        return _FakeCollections(self.collection_names)

    def get_collection(self, name):
        self.checked.append(name)
        vectors = (
            {"fast-bge-base-en-v1.5": _FakeVec(768)}
            if name in self.valid_vector_collections
            else {}
        )
        return SimpleNamespace(
            config=SimpleNamespace(
                params=SimpleNamespace(vectors=vectors),
                hnsw_config=SimpleNamespace(m=16, ef_construct=256),
            )
        )

    def query_points(self, *args, **kwargs):
        return SimpleNamespace(points=[])


class _FakeEmbedding:
    def embed(self, texts):
        for _ in texts:
            yield SimpleNamespace(tolist=lambda: [0.0] * 768)


def test_health_check_checks_all_collections_without_crashing_on_mismatch(monkeypatch, capsys):
    import scripts.health_check as health_check

    monkeypatch.setenv("COLLECTION_NAME", "context-engine")
    monkeypatch.setenv("EMBEDDING_MODEL", "fast/bge-base-en-v1.5")
    monkeypatch.setattr(health_check, "QdrantClient", _FakeClient)
    monkeypatch.setattr(health_check, "get_embedding_model", lambda *_: _FakeEmbedding())
    monkeypatch.setattr(health_check, "get_model_dimension", lambda *_: 768)
    monkeypatch.setattr(health_check, "ensure_collections", lambda *_: 0)

    health_check.main()

    output = capsys.readouterr().out
    assert "Checking collection: context-engine" in output
    assert "Checking collection: stale-empty" in output
    assert "Skipping vector query for stale-empty" in output


def test_health_check_missing_named_vector_does_not_keyerror(monkeypatch, capsys):
    import scripts.health_check as health_check

    monkeypatch.setenv("COLLECTION_NAME", "stale-empty")
    monkeypatch.setenv("EMBEDDING_MODEL", "fast/bge-base-en-v1.5")
    monkeypatch.setattr(health_check, "QdrantClient", _FakeClient)
    monkeypatch.setattr(health_check, "get_embedding_model", lambda *_: _FakeEmbedding())
    monkeypatch.setattr(health_check, "get_model_dimension", lambda *_: 768)
    monkeypatch.setattr(health_check, "ensure_collections", lambda *_: 0)

    health_check.main()

    output = capsys.readouterr().out
    assert "Expected vector name present" in output
    assert "Skipping vector query for stale-empty" in output
