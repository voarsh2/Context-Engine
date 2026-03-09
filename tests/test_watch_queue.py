def test_change_queue_suppresses_recent_identical_fingerprint(monkeypatch, tmp_path):
    from scripts.watch_index_core import queue as queue_mod

    monkeypatch.setattr(queue_mod, "RECENT_FINGERPRINT_TTL_SECS", 10.0)

    processed = []
    q = queue_mod.ChangeQueue(lambda paths: processed.append(list(paths)))

    p = tmp_path / "file.py"
    p.write_text("print('x')\n", encoding="utf-8")

    q._paths.add(p)
    q._flush()
    assert processed == [[p]]

    q._paths.add(p)
    q._flush()
    assert processed == [[p]]


def test_change_queue_reprocesses_when_fingerprint_changes(monkeypatch, tmp_path):
    from scripts.watch_index_core import queue as queue_mod

    monkeypatch.setattr(queue_mod, "RECENT_FINGERPRINT_TTL_SECS", 10.0)

    processed = []
    q = queue_mod.ChangeQueue(lambda paths: processed.append(list(paths)))

    p = tmp_path / "file.py"
    p.write_text("print('x')\n", encoding="utf-8")

    q._paths.add(p)
    q._flush()

    p.write_text("print('changed-again')\n", encoding="utf-8")
    q._paths.add(p)
    q._flush()

    assert processed == [[p], [p]]


def test_change_queue_force_bypasses_recent_fingerprint_suppression(monkeypatch, tmp_path):
    from scripts.watch_index_core import queue as queue_mod

    monkeypatch.setattr(queue_mod, "RECENT_FINGERPRINT_TTL_SECS", 10.0)

    processed = []
    q = queue_mod.ChangeQueue(lambda paths: processed.append(list(paths)))

    p = tmp_path / "file.py"
    p.write_text("print('x')\n", encoding="utf-8")

    q.add(p)
    q._flush()
    q.add(p, force=True)
    q._flush()

    assert processed == [[p], [p]]


def test_change_queue_repeated_same_path_does_not_rearm_timer(monkeypatch, tmp_path):
    from scripts.watch_index_core import queue as queue_mod

    class FakeTimer:
        created = 0
        canceled = 0

        def __init__(self, _delay, _cb):
            FakeTimer.created += 1
            self.daemon = False

        def start(self):
            return None

        def cancel(self):
            FakeTimer.canceled += 1

    monkeypatch.setattr(queue_mod.threading, "Timer", FakeTimer)

    q = queue_mod.ChangeQueue(lambda _paths: None)
    p = tmp_path / "file.py"
    p.write_text("print('x')\n", encoding="utf-8")

    q.add(p, force=True)
    q.add(p, force=True)
    q.add(p, force=True)

    assert FakeTimer.created == 1
    assert FakeTimer.canceled == 0
