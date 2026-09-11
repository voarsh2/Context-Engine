import os
import sys
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import MagicMock

import pytest


class _Sym(dict):
    __getattr__ = dict.get


def _patch_symbol(monkeypatch, ingest_pipeline, *, name: str, start: int = 1, end: int = 2):
    monkeypatch.setattr(
        ingest_pipeline,
        "extract_symbols_with_tree_sitter",
        lambda _fp: {
            f"function_{name}_{start}": {
                "name": name,
                "type": "function",
                "start_line": start,
                "end_line": end,
                "content_hash": "samehash",
                "pseudo": "",
                "tags": [],
                "qdrant_ids": [],
            }
        },
    )
    monkeypatch.setattr(
        ingest_pipeline,
        "_extract_symbols",
        lambda *_a, **_k: [
            _Sym(kind="function", name=name, path=name, start=start, end=end)
        ],
    )


def _patch_qdrant_models(monkeypatch, ingest_pipeline):
    class FakeModels:
        class Filter:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class FieldCondition:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class MatchValue:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class SparseVector:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        class PointStruct:
            def __init__(self, id, vector, payload):
                self.id = id
                self.vector = vector
                self.payload = payload

    monkeypatch.setattr(ingest_pipeline, "models", FakeModels)


def _patch_smart_side_effects(monkeypatch, ingest_pipeline):
    monkeypatch.setenv("LEX_SPARSE_MODE", "0")
    monkeypatch.setattr(ingest_pipeline, "LEX_SPARSE_MODE", False)
    monkeypatch.setattr(
        ingest_pipeline,
        "_sync_graph_edges_best_effort",
        lambda *a, **k: None,
        raising=False,
    )
    monkeypatch.setattr(ingest_pipeline, "_get_imports_calls", lambda *a, **k: ([], []))
    monkeypatch.setattr(ingest_pipeline, "_git_metadata", lambda *a, **k: (0, 0, 0))
    monkeypatch.setattr(
        ingest_pipeline,
        "_compute_host_and_container_paths",
        lambda _p: ("", ""),
    )


@pytest.mark.usefixtures("monkeypatch")
def test_smart_reindex_refreshes_lex_vector_for_reused_chunks(tmp_path, monkeypatch):
    """When reusing an existing dense embedding, smart reindex must refresh LEX vector.

    Otherwise pseudo/tags changes can drift from the stored lexical vector.
    """
    # The smart reindex logic we test doesn't require the real library.
    monkeypatch.setitem(sys.modules, "fastembed", SimpleNamespace(TextEmbedding=object))

    from scripts.ingest import pipeline as ingest_pipeline
    _patch_qdrant_models(monkeypatch, ingest_pipeline)

    # Deterministic pseudo/tags so we can predict lexical vector.
    monkeypatch.setattr(
        ingest_pipeline,
        "should_process_pseudo_for_chunk",
        lambda fp, ch, changed: (False, "pseudo", ["tag"]),
    )

    # Avoid touching any caches.
    monkeypatch.setattr(ingest_pipeline, "get_cached_symbols", lambda fp: {})
    monkeypatch.setattr(ingest_pipeline, "compare_symbol_changes", lambda a, b: ([], []))
    monkeypatch.setattr(ingest_pipeline, "set_cached_pseudo", None)
    monkeypatch.setattr(ingest_pipeline, "set_cached_symbols", None)
    monkeypatch.setattr(ingest_pipeline, "set_cached_file_hash", None)

    # Force simple line chunking.
    monkeypatch.setenv("INDEX_MICRO_CHUNKS", "0")
    monkeypatch.setenv("INDEX_SEMANTIC_CHUNKS", "0")
    monkeypatch.setenv("USE_TREE_SITTER", "0")
    monkeypatch.setenv("REFRAG_MODE", "0")
    _patch_symbol(monkeypatch, ingest_pipeline, name="add")
    _patch_smart_side_effects(monkeypatch, ingest_pipeline)

    code = "def add(a, b):\n    return a + b\n"
    fp = tmp_path / "x.py"
    fp.write_text(code, encoding="utf-8")

    # Compute the exact chunk text the indexer will use.
    chunk = ingest_pipeline.chunk_lines(code, max_lines=120, overlap=20)[0]
    code_text = chunk["text"]
    info_text = ingest_pipeline.build_information(
        "python",
        Path(fp),
        chunk["start"],
        chunk["end"],
        code_text.splitlines()[0] if code_text else "",
    )

    dense_key = "dense"
    old_lex = [0.0] * ingest_pipeline.LEX_VECTOR_DIM
    old_lex[0] = 1.0

    existing_record = SimpleNamespace(
        payload={
            "document": info_text,
            "information": info_text,
            "metadata": {
                "path": str(fp),
                "code": code_text,
                "kind": "function",
                "symbol": "add",
                "start_line": 1,
            }
        },
        vector={dense_key: [0.1, 0.2, 0.3], ingest_pipeline.LEX_VECTOR_NAME: old_lex},
    )

    class FakeClient:
        def scroll(self, **kwargs):
            # Return one existing point and then stop.
            if getattr(self, "_done", False):
                return ([], None)
            self._done = True
            return ([existing_record], None)

    captured = {}

    def fake_upsert_points(_client, _collection, points):
        captured["points"] = points

    monkeypatch.setattr(ingest_pipeline, "upsert_points", fake_upsert_points)
    monkeypatch.setattr(ingest_pipeline, "delete_points_by_path", lambda *a, **k: None)

    # Mock embed_batch since dense enrichment may trigger re-embedding
    reused_dense = [0.1, 0.2, 0.3]
    monkeypatch.setattr(ingest_pipeline, "embed_batch", lambda _model, texts: [reused_dense for _ in texts])

    # Model is unused when embeddings are mocked.
    dummy_model = object()

    status = ingest_pipeline.process_file_with_smart_reindexing(
        file_path=Path(fp),
        text=code,
        language="python",
        client=FakeClient(),
        current_collection="c",
        per_file_repo="r",
        model=dummy_model,
        vector_name=dense_key,
    )

    assert status == "success"
    assert "points" in captured and len(captured["points"]) == 1

    out_vec = captured["points"][0].vector
    assert isinstance(out_vec, dict)
    assert ingest_pipeline.LEX_VECTOR_NAME in out_vec

    expected_aug = (code_text or "") + " pseudo" + " tag"
    expected_lex = ingest_pipeline._lex_hash_vector_text(expected_aug)
    assert out_vec[ingest_pipeline.LEX_VECTOR_NAME] == expected_lex
    # Make sure we didn't keep the old lex vector.
    assert out_vec[ingest_pipeline.LEX_VECTOR_NAME] != old_lex


def test_should_process_pseudo_for_chunk_reuses_cache_after_line_shift(monkeypatch):
    from scripts.ingest import pseudo as pseudo_mod

    monkeypatch.setattr(pseudo_mod, "get_cached_pseudo", lambda *a, **k: ("", []))
    monkeypatch.setattr(
        pseudo_mod,
        "get_cached_symbols",
        lambda _fp: {
            "function_foo_10": {
                "name": "foo",
                "type": "function",
                "pseudo": "cached pseudo",
                "tags": ["alpha", "beta"],
            }
        },
    )

    needs_processing, pseudo, tags = pseudo_mod.should_process_pseudo_for_chunk(
        "x.py",
        {"symbol": "foo", "kind": "function", "start": 12},
        changed_symbols=set(),
    )

    assert needs_processing is False
    assert pseudo == "cached pseudo"
    assert tags == ["alpha", "beta"]


def test_smart_reindex_persists_pseudo_on_shifted_symbol_ids(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "fastembed", SimpleNamespace(TextEmbedding=object))
    monkeypatch.setenv("PSEUDO_BATCH_CONCURRENCY", "1")

    from scripts.ingest import pipeline as ingest_pipeline
    _patch_qdrant_models(monkeypatch, ingest_pipeline)

    fp = tmp_path / "x.py"
    fp.write_text("def foo():\n    return 1\n", encoding="utf-8")

    monkeypatch.setattr(
        ingest_pipeline,
        "extract_symbols_with_tree_sitter",
        lambda _fp: {
            "function_foo_12": {
                "name": "foo",
                "type": "function",
                "start_line": 12,
                "end_line": 13,
                "content_hash": "samehash",
                "pseudo": "",
                "tags": [],
                "qdrant_ids": [],
            },
            "function_bar_20": {
                "name": "bar",
                "type": "function",
                "start_line": 20,
                "end_line": 21,
                "content_hash": "barhash-new",
                "pseudo": "",
                "tags": [],
                "qdrant_ids": [],
            },
        },
    )
    monkeypatch.setattr(
        ingest_pipeline,
        "get_cached_symbols",
        lambda _fp: {
            "function_foo_10": {
                "name": "foo",
                "type": "function",
                "start_line": 10,
                "end_line": 11,
                "content_hash": "samehash",
                "pseudo": "cached pseudo",
                "tags": ["tag1"],
                "qdrant_ids": [],
            },
            "function_bar_20": {
                "name": "bar",
                "type": "function",
                "start_line": 20,
                "end_line": 21,
                "content_hash": "barhash-old",
                "pseudo": "old bar",
                "tags": ["old"],
                "qdrant_ids": [],
            },
        },
    )
    monkeypatch.setattr(
        ingest_pipeline,
        "compare_symbol_changes",
        lambda *_: (["function_foo_12"], ["function_bar_20"]),
    )
    monkeypatch.setattr(ingest_pipeline, "ensure_collection_and_indexes_once", lambda *a, **k: None)

    class FakeClient:
        def scroll(self, **kwargs):
            return ([], None)

    monkeypatch.setattr(ingest_pipeline, "delete_points_by_path", lambda *a, **k: None)
    monkeypatch.setattr(ingest_pipeline, "upsert_points", lambda *a, **k: None)
    monkeypatch.setattr(
        ingest_pipeline,
        "_sync_graph_edges_best_effort",
        lambda *a, **k: None,
        raising=False,
    )
    monkeypatch.setattr(ingest_pipeline, "_get_imports_calls", lambda *a, **k: ([], []))
    monkeypatch.setattr(ingest_pipeline, "_git_metadata", lambda *a, **k: (0, 0, 0))
    monkeypatch.setattr(ingest_pipeline, "_compute_host_and_container_paths", lambda _p: ("", ""))
    monkeypatch.setattr(ingest_pipeline, "_lex_hash_vector_text", lambda _t: [0.0] * ingest_pipeline.LEX_VECTOR_DIM)
    monkeypatch.setattr(ingest_pipeline, "_select_dense_text", lambda **kwargs: kwargs.get("code_text") or "")
    monkeypatch.setattr(ingest_pipeline, "embed_batch", lambda _model, texts: [[0.1, 0.2, 0.3] for _ in texts])
    monkeypatch.setattr(ingest_pipeline, "embed_batch", lambda _model, texts: [[0.1, 0.2, 0.3] for _ in texts])
    monkeypatch.setattr(ingest_pipeline, "generate_pseudo_tags", lambda _t: ("NEW", ["fresh"]))
    monkeypatch.setattr(
        ingest_pipeline,
        "chunk_lines",
        lambda text, *_a, **_k: [
            {"start": 12, "end": 13, "text": text, "symbol": "foo", "kind": "function"},
            {"start": 20, "end": 21, "text": text, "symbol": "bar", "kind": "function"},
        ],
    )
    monkeypatch.setattr(
        ingest_pipeline,
        "chunk_semantic",
        lambda text, *_a, **_k: [
            {"start": 12, "end": 13, "text": text, "symbol": "foo", "kind": "function"},
            {"start": 20, "end": 21, "text": text, "symbol": "bar", "kind": "function"},
        ],
    )
    monkeypatch.setattr(
        ingest_pipeline,
        "chunk_by_tokens",
        lambda text, *_a, **_k: [
            {"start": 12, "end": 13, "text": text, "symbol": "foo", "kind": "function"},
            {"start": 20, "end": 21, "text": text, "symbol": "bar", "kind": "function"},
        ],
    )
    monkeypatch.setattr(ingest_pipeline, "_extract_symbols", lambda *_a, **_k: [])
    monkeypatch.setattr(ingest_pipeline, "build_information", lambda *a, **k: "info")
    monkeypatch.setattr(ingest_pipeline, "hash_id", lambda *a, **k: 1)
    monkeypatch.setattr(ingest_pipeline, "generate_pseudo_tags_batch", None, raising=False)

    saved = {}
    monkeypatch.setattr(ingest_pipeline, "set_cached_pseudo", lambda *a, **k: None)
    monkeypatch.setattr(ingest_pipeline, "set_cached_file_hash", lambda *a, **k: None)
    monkeypatch.setattr(ingest_pipeline, "should_process_pseudo_for_chunk", ingest_pipeline.should_process_pseudo_for_chunk)
    monkeypatch.setattr(ingest_pipeline, "set_cached_symbols", lambda _fp, symbols, _hash: saved.update(symbols))

    status = ingest_pipeline.process_file_with_smart_reindexing(
        file_path=fp,
        text=fp.read_text(encoding="utf-8"),
        language="python",
        client=FakeClient(),
        current_collection="c",
        per_file_repo="r",
        model=object(),
        vector_name="dense",
        model_dim=3,
    )

    assert status == "success"
    # `foo` is logically reusable across the line shift, but chunk-level pseudo
    # generation may still refresh it depending on chunk processing order.
    assert saved["function_foo_12"]["pseudo"] in {"cached pseudo", "NEW"}
    assert saved["function_bar_20"]["pseudo"] == "NEW"
    assert saved["function_foo_12"]["tags"]


def test_smart_reindex_does_not_reuse_when_info_changes(tmp_path, monkeypatch):
    """Dense embeddings must not be reused if `information` differs."""

    monkeypatch.setitem(sys.modules, "fastembed", SimpleNamespace(TextEmbedding=object))

    from scripts.ingest import pipeline as ingest_pipeline
    _patch_qdrant_models(monkeypatch, ingest_pipeline)

    # Avoid touching any caches.
    monkeypatch.setattr(ingest_pipeline, "get_cached_symbols", lambda fp: {})
    monkeypatch.setattr(ingest_pipeline, "compare_symbol_changes", lambda a, b: ([], []))
    monkeypatch.setattr(ingest_pipeline, "set_cached_pseudo", None)
    monkeypatch.setattr(ingest_pipeline, "set_cached_symbols", None)
    monkeypatch.setattr(ingest_pipeline, "set_cached_file_hash", None)

    # Force simple line chunking.
    monkeypatch.setenv("INDEX_MICRO_CHUNKS", "0")
    monkeypatch.setenv("INDEX_SEMANTIC_CHUNKS", "0")
    monkeypatch.setenv("USE_TREE_SITTER", "0")
    monkeypatch.setenv("REFRAG_MODE", "0")
    _patch_symbol(monkeypatch, ingest_pipeline, name="hi")
    _patch_smart_side_effects(monkeypatch, ingest_pipeline)
    monkeypatch.setattr(
        ingest_pipeline,
        "should_process_pseudo_for_chunk",
        lambda fp, ch, changed: (False, "", []),
    )

    # Make build_information return a value that won't match the stored record.
    old_info = "old-info"
    new_info = "new-info"
    monkeypatch.setattr(ingest_pipeline, "build_information", lambda *a, **k: new_info)

    code = "def hi():\n    return 1\n"
    fp = tmp_path / "x.py"
    fp.write_text(code, encoding="utf-8")

    chunk = ingest_pipeline.chunk_lines(code, max_lines=120, overlap=20)[0]
    code_text = chunk["text"]

    dense_key = "dense"
    reused_dense = [0.123, 0.456]

    existing_record = SimpleNamespace(
        payload={
            "document": old_info,
            "information": old_info,
            "metadata": {
                "path": str(fp),
                "code": code_text,
                "kind": "function",
                "symbol": "hi",
                "start_line": 1,
            },
        },
        vector={dense_key: reused_dense, ingest_pipeline.LEX_VECTOR_NAME: [0.0] * ingest_pipeline.LEX_VECTOR_DIM},
    )

    class FakeClient:
        def scroll(self, **kwargs):
            if getattr(self, "_done", False):
                return ([], None)
            self._done = True
            return ([existing_record], None)

    captured = {}

    def fake_upsert_points(_client, _collection, points):
        captured["points"] = points

    monkeypatch.setattr(ingest_pipeline, "upsert_points", fake_upsert_points)
    monkeypatch.setattr(ingest_pipeline, "delete_points_by_path", lambda *a, **k: None)

    embedded_vec = [9.9, 8.8]
    monkeypatch.setattr(ingest_pipeline, "embed_batch", lambda _model, texts: [embedded_vec for _ in texts])

    status = ingest_pipeline.process_file_with_smart_reindexing(
        file_path=Path(fp),
        text=code,
        language="python",
        client=FakeClient(),
        current_collection="c",
        per_file_repo="r",
        model=object(),
        vector_name=dense_key,
    )

    assert status == "success"
    assert len(captured["points"]) == 1
    out_vec = captured["points"][0].vector
    assert out_vec[dense_key] == embedded_vec
    assert out_vec[dense_key] != reused_dense


def test_smart_reindex_unnamed_reuse_requires_dense_vector(tmp_path, monkeypatch):
    """If an existing unnamed-vector point has only lex/mini, re-embed instead of reusing []."""

    monkeypatch.setitem(sys.modules, "fastembed", SimpleNamespace(TextEmbedding=object))

    from scripts.ingest import pipeline as ingest_pipeline
    _patch_qdrant_models(monkeypatch, ingest_pipeline)

    # Avoid touching any caches.
    monkeypatch.setattr(ingest_pipeline, "get_cached_symbols", lambda fp: {})
    monkeypatch.setattr(ingest_pipeline, "compare_symbol_changes", lambda a, b: ([], []))
    monkeypatch.setattr(ingest_pipeline, "set_cached_pseudo", None)
    monkeypatch.setattr(ingest_pipeline, "set_cached_symbols", None)
    monkeypatch.setattr(ingest_pipeline, "set_cached_file_hash", None)

    # Force simple line chunking.
    monkeypatch.setenv("INDEX_MICRO_CHUNKS", "0")
    monkeypatch.setenv("INDEX_SEMANTIC_CHUNKS", "0")
    monkeypatch.setenv("USE_TREE_SITTER", "0")
    monkeypatch.setenv("REFRAG_MODE", "0")
    _patch_symbol(monkeypatch, ingest_pipeline, name="hi")
    _patch_smart_side_effects(monkeypatch, ingest_pipeline)
    monkeypatch.setattr(
        ingest_pipeline,
        "should_process_pseudo_for_chunk",
        lambda fp, ch, changed: (False, "", []),
    )

    code = "def hi():\n    return 1\n"
    fp = tmp_path / "x.py"
    fp.write_text(code, encoding="utf-8")

    chunk = ingest_pipeline.chunk_lines(code, max_lines=120, overlap=20)[0]
    code_text = chunk["text"]
    info_text = ingest_pipeline.build_information(
        "python",
        Path(fp),
        chunk["start"],
        chunk["end"],
        code_text.splitlines()[0] if code_text else "",
    )

    existing_record = SimpleNamespace(
        payload={
            "document": info_text,
            "information": info_text,
            "metadata": {
                "path": str(fp),
                "code": code_text,
                "kind": "function",
                "symbol": "hi",
                "start_line": 1,
            },
        },
        # Only lex/mini present: should not be reused as dense.
        vector={
            ingest_pipeline.LEX_VECTOR_NAME: [0.0] * ingest_pipeline.LEX_VECTOR_DIM,
            ingest_pipeline.MINI_VECTOR_NAME: [0.0] * ingest_pipeline.MINI_VEC_DIM,
        },
    )

    class FakeClient:
        def scroll(self, **kwargs):
            if getattr(self, "_done", False):
                return ([], None)
            self._done = True
            return ([existing_record], None)

    captured = {}

    def fake_upsert_points(_client, _collection, points):
        captured["points"] = points

    monkeypatch.setattr(ingest_pipeline, "upsert_points", fake_upsert_points)
    monkeypatch.setattr(ingest_pipeline, "delete_points_by_path", lambda *a, **k: None)

    embedded_vec = [7.7, 6.6]
    monkeypatch.setattr(ingest_pipeline, "embed_batch", lambda _model, texts: [embedded_vec for _ in texts])

    status = ingest_pipeline.process_file_with_smart_reindexing(
        file_path=Path(fp),
        text=code,
        language="python",
        client=FakeClient(),
        current_collection="c",
        per_file_repo="r",
        model=object(),
        vector_name=None,
    )

    assert status == "success"
    assert len(captured["points"]) == 1
    out_vec = captured["points"][0].vector
    assert out_vec == embedded_vec


def test_smart_reindex_no_symbol_changes_falls_back_without_hash_cache(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "fastembed", SimpleNamespace(TextEmbedding=object))

    from scripts.ingest import pipeline as ingest_pipeline
    _patch_qdrant_models(monkeypatch, ingest_pipeline)

    code = "def hi():\n    return 1\n"
    fp = tmp_path / "x.py"
    fp.write_text(code, encoding="utf-8")

    monkeypatch.setattr(
        ingest_pipeline,
        "extract_symbols_with_tree_sitter",
        lambda _fp: {"function_hi_1": {"name": "hi", "type": "function", "start_line": 1}},
    )
    monkeypatch.setattr(
        ingest_pipeline,
        "get_cached_symbols",
        lambda _fp: {"function_hi_1": {"name": "hi", "type": "function", "start_line": 1}},
    )
    monkeypatch.setattr(ingest_pipeline, "compare_symbol_changes", lambda *_: ([], []))
    monkeypatch.setattr(ingest_pipeline, "get_cached_file_hash", lambda *_: None)
    set_cached_file_hash = MagicMock()
    monkeypatch.setattr(ingest_pipeline, "set_cached_file_hash", set_cached_file_hash)

    status = ingest_pipeline.process_file_with_smart_reindexing(
        file_path=Path(fp),
        text=code,
        language="python",
        client=MagicMock(),
        current_collection="c",
        per_file_repo="r",
        model=object(),
        vector_name="dense",
    )

    assert status == "failed"
    set_cached_file_hash.assert_not_called()
