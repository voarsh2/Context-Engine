import sys
from pathlib import Path

import pytest


@pytest.mark.unit
def test_cli_force_collection_disables_multi_repo_enumeration(monkeypatch, tmp_path: Path):
    from scripts.ingest import cli

    # Create fake repo dirs to prove we are not enumerating them.
    (tmp_path / "repo_a").mkdir()
    (tmp_path / "repo_b").mkdir()

    calls = []

    def _fake_index_repo(
        root,
        qdrant_url,
        api_key,
        collection,
        model_name,
        recreate,
        dedupe,
        skip_unchanged,
        pseudo_mode,
        schema_mode,
    ):
        calls.append(
            {
                "root": Path(root),
                "collection": collection,
                "recreate": recreate,
                "dedupe": dedupe,
                "skip_unchanged": skip_unchanged,
            }
        )

    monkeypatch.setattr(cli, "index_repo", _fake_index_repo)
    monkeypatch.setattr(cli, "is_multi_repo_mode", lambda: True)
    monkeypatch.setattr(cli, "get_collection_name", lambda *_: "should-not-use")

    monkeypatch.setenv("MULTI_REPO_MODE", "1")
    monkeypatch.setenv("COLLECTION_NAME", "forced-collection")
    monkeypatch.setenv("CTXCE_FORCE_COLLECTION_NAME", "1")

    monkeypatch.setattr(
        sys,
        "argv",
        ["ingest_code.py", "--root", str(tmp_path)],
    )

    cli.main()

    assert len(calls) == 1
    assert calls[0]["root"] == tmp_path
    assert calls[0]["collection"] == "forced-collection"
