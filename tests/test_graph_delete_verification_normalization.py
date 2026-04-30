from pathlib import PureWindowsPath


def test_graph_delete_verification_normalizes_caller_path():
    # Unit-level guard: watcher delete verification must query graph edges using
    # the same path normalization as graph edge writes/deletes (Windows -> POSIX).
    from scripts.watch_index_core import processor as proc

    captured = {}

    class DummyClient:
        def scroll(
            self,
            *,
            collection_name,
            scroll_filter,
            with_payload=False,
            with_vectors=False,
            limit=1,
        ):
            captured["collection_name"] = collection_name
            captured["filter"] = scroll_filter
            return ([], None)

    client = DummyClient()
    path = PureWindowsPath(r"C:\repo\foo.py")

    has_edges = proc._path_has_graph_edges(client, "base_collection", path)
    assert has_edges is False

    flt = captured["filter"]
    assert flt is not None
    assert getattr(flt, "must", None)
    cond = flt.must[0]
    assert cond.key == "caller_path"

    match = cond.match
    values = []
    if hasattr(match, "any") and match.any is not None:
        values = list(match.any)
    elif hasattr(match, "value"):
        values = [match.value]

    assert "C:/repo/foo.py" in values

