import importlib


ps = importlib.import_module("scripts.path_scope")


def test_normalize_under_strips_work_prefix():
    assert ps.normalize_under("/work/scripts/mcp_impl") == "scripts/mcp_impl"


def test_normalize_under_keeps_repo_prefixed_path():
    assert (
        ps.normalize_under("/work/Context-Engine/scripts/mcp_impl")
        == "Context-Engine/scripts/mcp_impl"
    )


def test_normalize_under_rebases_single_segment_from_cwd(monkeypatch, tmp_path):
    repo = tmp_path / "repo"
    (repo / "nested" / "scope").mkdir(parents=True)
    monkeypatch.setattr(ps, "_repo_root_hint", lambda: str(repo))
    monkeypatch.setattr(ps.os, "getcwd", lambda: str(repo / "nested"))

    assert ps.normalize_under("scope") == "nested/scope"


def test_normalize_under_does_not_rebase_when_top_level_exists(monkeypatch, tmp_path):
    repo = tmp_path / "repo"
    (repo / "nested" / "scope").mkdir(parents=True)
    (repo / "scope").mkdir(parents=True)
    monkeypatch.setattr(ps, "_repo_root_hint", lambda: str(repo))
    monkeypatch.setattr(ps.os, "getcwd", lambda: str(repo / "nested"))

    assert ps.normalize_under("scope") == "scope"


def test_normalize_under_expands_unique_segment(monkeypatch, tmp_path):
    repo = tmp_path / "repo"
    (repo / "alpha" / "mcp_impl").mkdir(parents=True)
    monkeypatch.setattr(ps, "_repo_root_hint", lambda: str(repo))
    monkeypatch.setattr(ps.os, "getcwd", lambda: str(repo))
    ps._unique_segment_path.cache_clear()

    assert ps.normalize_under("mcp_impl") == "alpha/mcp_impl"


def test_normalize_under_keeps_ambiguous_segment(monkeypatch, tmp_path):
    repo = tmp_path / "repo"
    (repo / "alpha" / "dup").mkdir(parents=True)
    (repo / "beta" / "dup").mkdir(parents=True)
    monkeypatch.setattr(ps, "_repo_root_hint", lambda: str(repo))
    monkeypatch.setattr(ps.os, "getcwd", lambda: str(repo))
    ps._unique_segment_path.cache_clear()

    assert ps.normalize_under("dup") == "dup"


def test_metadata_matches_under_without_repo_hint_for_work_repo_paths():
    md = {"path": "/work/repo/space/ship/a.py"}
    assert ps.metadata_matches_under(md, "space")
    assert not ps.metadata_matches_under(md, "direct")
