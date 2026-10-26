from datetime import UTC, datetime, timedelta

import pytest

from dataops.cli import _duration
from dataops.config import redact
from dataops.ingestion.batch import _validate, payload_checksum, record_id, split_windows
from dataops.reconciliation.revenue import compare_daily


def test_payload_checksum_ignores_key_order():
    assert payload_checksum({"a": 1, "b": "x"}) == payload_checksum({"b": "x", "a": 1})
    assert payload_checksum({"a": 1}) != payload_checksum({"a": 2})


def test_record_id_is_deterministic_and_content_addressed():
    c = payload_checksum({"order_id": 1, "status": "paid"})
    assert record_id("orders", "1", c) == record_id("orders", "1", c)
    assert record_id("orders", "1", c) != record_id("orders", "2", c)
    assert record_id("orders", "1", c) != record_id("payments", "1", c)


def test_split_windows_covers_range_without_gaps():
    start = datetime(2026, 3, 1, tzinfo=UTC)
    end = start + timedelta(days=2, hours=6)
    windows = split_windows(start, end, timedelta(days=1))
    assert windows[0][0] == start and windows[-1][1] == end
    assert all(a[1] == b[0] for a, b in zip(windows, windows[1:]))
    assert len(windows) == 3


@pytest.mark.parametrize(
    "table,start,end",
    [
        ("not_a_table", datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)),
        ("orders", datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC)),
        ("orders", datetime(2026, 1, 1), datetime(2026, 1, 2)),
    ],
)
def test_validate_rejects_bad_requests(table, start, end):
    with pytest.raises(ValueError):
        _validate(table, start, end)


def test_compare_daily_detects_value_and_missing_key_differences():
    left = {"2026-03-01": {"m": 10}, "2026-03-02": {"m": 5}}
    right = {"2026-03-01": {"m": 10}, "2026-03-02": {"m": 4}, "2026-03-03": {"m": 1}}
    compared, mismatches = compare_daily(left, right, ["m"])
    assert compared == 3
    by_key = {m["key"]: m for m in mismatches}
    assert by_key["2026-03-02"]["metrics"]["m"]["delta"] == -1
    assert by_key["2026-03-03"]["missing_left"] is True
    assert "2026-03-01" not in by_key


def test_compare_daily_equal():
    data = {"2026-03-01": {"m": 1}}
    assert compare_daily(data, dict(data), ["m"]) == (1, [])


def test_redact_removes_secrets():
    text = (
        "password=hunter2xyz conn postgresql://bob:s3cretpw@db:5432/x api_key: sk-abcdefghijklmnop"
    )
    out = redact(text)
    assert "hunter2xyz" not in out and "s3cretpw" not in out and "sk-abcdefghijklmnop" not in out


def test_duration_parser():
    assert _duration("15m") == timedelta(minutes=15)
    assert _duration("1d") == timedelta(days=1)
