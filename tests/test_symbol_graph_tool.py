import pytest


@pytest.mark.asyncio
async def test_symbol_graph_under_filters_results_by_recursive_scope():
    # Validate that under applies as recursive subtree filter (user-facing scope).
    from scripts.mcp_impl import symbol_graph as sg

    class _Pt:
        def __init__(self, pid, path):
            self.id = pid
            self.payload = {
                "metadata": {
                    "repo": "repo",
                    "path": path,
                    "start_line": 1,
                    "end_line": 2,
                    "symbol": "f",
                    "symbol_path": "f",
                    "language": "python",
                    "calls": ["foo"],
                }
            }

    class FakeClient:
        def __init__(self):
            self.scroll_filters = []

        def scroll(self, *, collection_name, scroll_filter, limit, with_payload, with_vectors):
            self.scroll_filters.append(scroll_filter)
            return (
                [
                    _Pt("1", "/work/repo/scripts/a.py"),
                    _Pt("2", "/work/repo/tests/b.py"),
                ],
                None,
            )

    client = FakeClient()
    out = await sg._query_array_field(  # type: ignore[attr-defined]
        client=client,
        collection="codebase",
        field_key="metadata.calls",
        value="foo",
        limit=10,
        language="python",
        under=sg._norm_under("scripts"),  # type: ignore[attr-defined]
    )

    # Validate _query_array_field forwards language/value constraints to scroll_filter.
    assert client.scroll_filters, "Expected at least one scroll() call"
    first_filter = client.scroll_filters[0]
    first_must = list(getattr(first_filter, "must", []) or [])
    assert any(
        getattr(cond, "key", None) == "metadata.calls"
        and getattr(getattr(cond, "match", None), "any", None) == ["foo"]
        for cond in first_must
    )
    assert any(
        getattr(cond, "key", None) == "metadata.language"
        and getattr(getattr(cond, "match", None), "value", None) == "python"
        for cond in first_must
    )
    assert any(
        any(
            getattr(cond, "key", None) == "metadata.calls"
            and getattr(getattr(cond, "match", None), "text", None) == "foo"
            for cond in list(getattr(sf, "must", []) or [])
        )
        for sf in client.scroll_filters
    ), "Expected MatchText fallback filter for metadata.calls"

    paths = {r.get("path") for r in out}
    assert "/work/repo/scripts/a.py" in paths
    assert "/work/repo/tests/b.py" not in paths
