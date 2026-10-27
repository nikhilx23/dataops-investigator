import multiprocessing as mp

import pytest

from dataops.streaming.lock import LakehouseBusyError, lakehouse_lock


def _try_lock(lake: str, q) -> None:
    import os

    os.environ["DATAOPS_LAKE_DIR"] = lake
    try:
        with lakehouse_lock("second"):
            q.put("acquired")
    except LakehouseBusyError as exc:
        q.put(str(exc))


def test_second_process_is_rejected_while_held(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAOPS_LAKE_DIR", str(tmp_path))
    q = mp.get_context("spawn").Queue()
    with lakehouse_lock("first holder"):
        p = mp.get_context("spawn").Process(target=_try_lock, args=(str(tmp_path), q))
        p.start()
        p.join(10)
        msg = q.get(timeout=5)
    assert "another lakehouse writer" in msg and "first holder" in msg
    p = mp.get_context("spawn").Process(target=_try_lock, args=(str(tmp_path), q))
    p.start()
    p.join(10)
    assert q.get(timeout=5) == "acquired"  # released when the holder exits the block


def test_lock_released_after_exception(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAOPS_LAKE_DIR", str(tmp_path))
    with pytest.raises(ValueError), lakehouse_lock("x"):
        raise ValueError("boom")
    with lakehouse_lock("y"):
        pass
