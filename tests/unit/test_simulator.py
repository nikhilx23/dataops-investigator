from datetime import timedelta

import pytest

from dataops.generator.generate import PRIMARY_KEYS, TABLE_ORDER, generate
from dataops.generator.simulator import changes, snapshot_at


@pytest.fixture(scope="module", params=["tiny", "small"])
def ds(request):
    return generate(42, request.param)


def test_replay_to_end_reproduces_final_dataset(ds):
    replayed = snapshot_at(ds, ds.end)
    for table in TABLE_ORDER:
        expected = sorted(ds.tables[table], key=lambda r: r[PRIMARY_KEYS[table]])
        assert replayed[table] == expected, table


def test_states_never_come_from_the_future(ds):
    for when, _rank, table, row in changes(ds):
        assert row["updated_at"] <= when or table == "product_versions", table


def test_intermediate_snapshot_shows_earlier_states(ds):
    mid = ds.start + timedelta(days=ds.profile.days // 2)
    snap = snapshot_at(ds, mid)
    assert 0 < len(snap["orders"]) < len(ds.tables["orders"])
    assert all(o["placed_at"] <= mid and o["updated_at"] <= mid for o in snap["orders"])
    # Some orders that end up delivered are still in an earlier state mid-way.
    final = {o["order_id"]: o["status"] for o in ds.tables["orders"]}
    assert any(o["status"] != final[o["order_id"]] for o in snap["orders"])
    # Referential integrity holds at every point in time.
    order_ids = {o["order_id"] for o in snap["orders"]}
    assert all(p["order_id"] in order_ids for p in snap["payments"])
    assert all(i["order_id"] in order_ids for i in snap["order_items"])
