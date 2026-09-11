import importlib
import subprocess


def test_init_maintenance_interval_defaults_to_two_hours(monkeypatch):
    monkeypatch.delenv("WATCH_INIT_MAINTENANCE_INTERVAL_MINUTES", raising=False)
    monkeypatch.delenv("INIT_MAINTENANCE_INTERVAL_MINUTES", raising=False)

    mod = importlib.import_module("scripts.watch_index_core.init_maintenance")
    mod = importlib.reload(mod)

    assert mod._interval_seconds() == 120 * 60


def test_init_maintenance_runs_existing_scripts_under_lock(monkeypatch, tmp_path):
    mod = importlib.import_module("scripts.watch_index_core.init_maintenance")
    mod = importlib.reload(mod)

    calls = []

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(mod.subprocess, "run", fake_run)
    monkeypatch.setenv("WATCH_INIT_MAINTENANCE_COMMAND_TIMEOUT_SECS", "7")

    commands = [
        ["wait-for-qdrant.sh"],
        ["python", "create_indexes.py"],
        ["python", "warm_all_collections.py"],
        ["python", "health_check.py"],
    ]

    ok = mod.run_init_maintenance_once(commands=commands, lock_path=tmp_path / "init.lock")

    assert ok is True
    assert [call[0] for call in calls] == commands
    assert all(call[1]["timeout"] == 7 for call in calls)
    assert all(call[1]["check"] is False for call in calls)
    assert all("PYTHONPATH" in call[1]["env"] for call in calls)


def test_init_maintenance_stops_sequence_on_failure(monkeypatch, tmp_path):
    mod = importlib.import_module("scripts.watch_index_core.init_maintenance")
    mod = importlib.reload(mod)

    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="boom")

    monkeypatch.setattr(mod.subprocess, "run", fake_run)

    ok = mod.run_init_maintenance_once(
        commands=[["first"], ["second"]],
        lock_path=tmp_path / "init.lock",
    )

    assert ok is False
    assert calls == [["first"]]


def test_init_maintenance_worker_can_be_disabled(monkeypatch):
    mod = importlib.import_module("scripts.watch_index_core.init_maintenance")
    mod = importlib.reload(mod)

    monkeypatch.setenv("WATCH_INIT_MAINTENANCE_ENABLED", "0")

    assert mod.start_init_maintenance_worker() is None
