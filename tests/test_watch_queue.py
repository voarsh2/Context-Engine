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
