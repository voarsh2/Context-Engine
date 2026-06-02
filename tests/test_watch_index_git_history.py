import io
import subprocess

import pytest


pytestmark = pytest.mark.unit


def test_git_history_ingest_runs_as_package_module(monkeypatch, tmp_path):
    from scripts.watch_index_core import processor
    from scripts.watch_index_core import config as watch_config

    manifest = tmp_path / "git_history.json"
    manifest.write_text('{"commits": []}', encoding="utf-8")

    captured = {}

    class FakePopen:
        def __init__(self, cmd, **kwargs):
            captured["cmd"] = cmd
            captured["cwd"] = kwargs.get("cwd")
            captured["env"] = kwargs.get("env")
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO("")

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

        def kill(self):
            pass

    monkeypatch.setattr(subprocess, "Popen", FakePopen)

    processor._run_git_history_ingest(
        manifest,
        collection="Context-Engine-41e67959",
        repo_name="Context-Engine-41e67959950c8ab3",
    )

    assert captured["cmd"][:3] == [processor.sys.executable or "python3", "-m", "scripts.ingest_history"]
    assert "--manifest-json" in captured["cmd"]
    assert captured["cwd"] == str(watch_config.ROOT_DIR)
    assert captured["env"]["COLLECTION_NAME"] == "Context-Engine-41e67959"
    assert captured["env"]["REPO_NAME"] == "Context-Engine-41e67959950c8ab3"
