from datetime import UTC, datetime
from pathlib import Path

import pytest

from dataops.contracts import load
from dataops.reliability.collectors import compare_to_contract
from dataops.reliability.incidents import _clusters
from dataops.reliability.lineage import Edge, Graph, build_graph, impact, most_upstream

MANIFEST = Path(__file__).resolve().parents[2] / "dbt" / "target" / "manifest.json"


def graph(*edges: tuple[str, str]) -> Graph:
    g = Graph()
    for a, b in edges:
        g.nodes.setdefault(a, {"kind": "model", "criticality": "standard"})
        g.nodes.setdefault(b, {"kind": "model", "criticality": "standard"})
        g.edges.append(Edge(a, b, "declared", "depends_on"))
    return g


def test_downstream_upstream_and_shortest_paths():
    g = graph(("src", "a"), ("a", "b"), ("src", "b"), ("b", "dash"))
    assert g.downstream("src") == {
        "a": ["src", "a"],
        "b": ["src", "b"],
        "dash": ["src", "b", "dash"],
    }
    assert g.upstream("dash")["src"] == ["src", "b", "dash"]
    assert g.path("a", "dash") == ["a", "b", "dash"]
    assert g.path("dash", "src") is None
    hops = g.explain(["src", "b", "dash"])
    assert [h["to"] for h in hops] == ["b", "dash"] and hops[0]["extraction"] == "declared"


def test_cycle_detection():
    assert graph(("a", "b"), ("b", "c")).find_cycles() == []
    cycles = graph(("a", "b"), ("b", "c"), ("c", "a"), ("c", "d")).find_cycles()
    assert cycles == [["a", "b", "c", "a"]]


def test_unknown_dataset_raises():
    with pytest.raises(KeyError):
        graph(("a", "b")).downstream("zzz")


def test_most_upstream_picks_roots_only():
    g = graph(("src", "stg"), ("stg", "mart"), ("other", "x"))
    assert most_upstream(g, ["mart", "stg", "x"]) == ["stg", "x"]


def test_clusters_group_connected_symptoms_and_split_unrelated():
    g = graph(("src", "stg"), ("stg", "mart"), ("other", "x"))
    t = datetime(2026, 3, 2, tzinfo=UTC)
    events = [
        {"event_id": i, "dataset_id": d, "observed_at": t, "incident_id": None}
        for i, d in enumerate(["mart", "src", "x", None])
    ]
    clusters = sorted(sorted(e["event_id"] for e in c) for c in _clusters(g, events))
    assert clusters == [[0, 1], [2], [3]]


@pytest.mark.skipif(not MANIFEST.exists(), reason="run `dbt parse` first")
def test_real_graph_is_acyclic_and_routes_sources_per_table():
    g = build_graph(MANIFEST)
    assert g.find_cycles() == []
    # payments must not reach the inventory dashboard; it must reach revenue.
    dashboards = impact(g, "commerce_source.shop.payments")["dashboards"]
    assert "bi.dashboard.revenue_overview" in dashboards
    assert "bi.dashboard.inventory_overview" not in dashboards
    kinds = {e.extraction for e in g.edges}
    assert kinds == {"dbt_manifest", "declared"}


def _observed(table: str, **override):
    cols = [
        {"name": c.name, "type": c.type, "nullable": not c.required} for c in load()[table].columns
    ]
    for c in cols:
        if c["name"] in override:
            c.update(override[c["name"]])
    return cols


def test_contract_compatibility_rules():
    c = load()["payments"]
    assert compare_to_contract(_observed("payments"), c) == ("matches_contract", [])
    added = [*_observed("payments"), {"name": "currency", "type": "text", "nullable": True}]
    assert compare_to_contract(added, c)[0] == "compatible"
    changed = _observed("payments", amount_cents={"type": "numeric"})
    comp, diffs = compare_to_contract(changed, c)
    assert comp == "breaking" and diffs[0]["change"] == "type_changed"
    removed = [x for x in _observed("payments") if x["name"] != "method"]
    assert compare_to_contract(removed, c)[1][0]["change"] == "removed"
    nullable = _observed("payments", order_id={"nullable": True})
    assert compare_to_contract(nullable, c)[1][0]["change"] == "became_nullable"


def test_shared_group_correlates_unconnected_datasets():
    g = graph(("t1", "b1"), ("t2", "b2"))
    t = datetime(2026, 3, 2, tzinfo=UTC)
    lag = {"group": "consumer:spark:bronze"}
    events = [
        {
            "event_id": 0,
            "dataset_id": "t1",
            "evidence_ref": lag,
            "incident_id": None,
            "observed_at": t,
        },
        {
            "event_id": 1,
            "dataset_id": "t2",
            "evidence_ref": lag,
            "incident_id": None,
            "observed_at": t,
        },
        {
            "event_id": 2,
            "dataset_id": "t2",
            "evidence_ref": {},
            "incident_id": None,
            "observed_at": t,
        },
    ]
    assert len(_clusters(g, events)) == 1
    assert len(_clusters(g, [events[0], {**events[2], "dataset_id": "b1"}])) == 1
    assert len(_clusters(g, [events[0], {**events[2], "dataset_id": "b2"}])) == 2
