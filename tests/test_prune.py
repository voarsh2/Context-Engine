from types import SimpleNamespace

import scripts.prune as prune


class _FakeClient:
    def __init__(self, points):
        self._points = points

    def scroll(self, **kwargs):
        return self._points, None


def _point(path, file_hash=None, repo="repo-a"):
    return SimpleNamespace(
        payload={
            "metadata": {
                "path": path,
                "file_hash": file_hash,
                "repo": repo,
            }
        }
    )


def test_prune_excludes_deleted_paths_from_orphan_keepalive(monkeypatch, tmp_path):
    keep_path = tmp_path / "keep.py"
    keep_path.write_text("keep = True\n", encoding="utf-8")

    mismatch_path = tmp_path / "mismatch.py"
    mismatch_path.write_text("new = True\n", encoding="utf-8")

    points = [
        _point("missing.py", file_hash="missing-hash"),
        _point("mismatch.py", file_hash="old-hash"),
        _point("keep.py", file_hash=prune.sha1_file(keep_path)),
    ]
    fake_client = _FakeClient(points)
    captured_valid_paths = []

    monkeypatch.setattr(prune, "QdrantClient", lambda **kwargs: fake_client)
    monkeypatch.setattr(prune, "ROOT", tmp_path)
    monkeypatch.setattr(prune, "delete_by_path", lambda *args, **kwargs: 1)
    monkeypatch.setattr(prune, "delete_graph_edges_by_path", lambda *args, **kwargs: 0)
    monkeypatch.setattr(
        prune,
        "delete_orphan_graph_edges",
        lambda client, valid_paths: captured_valid_paths.append(set(valid_paths)) or 0,
    )

    prune.main()

    assert captured_valid_paths == [{"keep.py"}]

