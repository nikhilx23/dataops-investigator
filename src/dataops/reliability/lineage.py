"""Model-level lineage: build, persist (versioned) and traverse.

Edges come from two places and keep that provenance on every edge:
  dbt_manifest  model -> model, source -> model, model -> exposure (dashboard), read from
                dbt's manifest.json (parent/child map of the selected nodes)
  declared      catalog/declared.yaml: source table -> raw/CDC/lakehouse/publication datasets,
                which no tool can extract
Column-level lineage is NOT implemented.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dataops.config import REPO_ROOT, Role
from dataops.contracts import load as load_contracts
from dataops.db import connect

DECLARED_PATH = REPO_ROOT / "catalog" / "declared.yaml"
CRITICAL_SOURCE_TABLES = {"orders", "order_items", "payments", "refunds"}


@dataclass(frozen=True)
class Edge:
    upstream: str
    downstream: str
    extraction: str  # dbt_manifest | declared
    relationship: str


@dataclass
class Graph:
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    sources: dict[str, str] = field(default_factory=dict)  # input -> checksum
    node_by_unique_id: dict[str, str] = field(default_factory=dict)

    # --------------------------------------------------------------- queries
    def children(self, node: str) -> list[Edge]:
        return [e for e in self.edges if e.upstream == node]

    def parents(self, node: str) -> list[Edge]:
        return [e for e in self.edges if e.downstream == node]

    def _walk(self, start: str, forward: bool) -> dict[str, list[str]]:
        """BFS; returns {reached node: shortest path from start}."""
        if start not in self.nodes:
            raise KeyError(f"unknown dataset {start!r}")
        paths: dict[str, list[str]] = {start: [start]}
        queue = deque([start])
        while queue:
            cur = queue.popleft()
            nxt = (
                [e.downstream for e in self.children(cur)]
                if forward
                else [e.upstream for e in self.parents(cur)]
            )
            for n in sorted(nxt):
                if n not in paths:
                    paths[n] = [*paths[cur], n]
                    queue.append(n)
        del paths[start]
        return paths

    def downstream(self, node: str) -> dict[str, list[str]]:
        return self._walk(node, forward=True)

    def upstream(self, node: str) -> dict[str, list[str]]:
        return {n: list(reversed(p)) for n, p in self._walk(node, forward=False).items()}

    def path(self, src: str, dst: str) -> list[str] | None:
        return self.downstream(src).get(dst)

    def explain(self, path: list[str]) -> list[dict[str, str]]:
        """Each hop of a path with how the edge is known."""
        by_pair = {(e.upstream, e.downstream): e for e in self.edges}
        return [
            {
                "from": a,
                "to": b,
                "relationship": by_pair[(a, b)].relationship,
                "extraction": by_pair[(a, b)].extraction,
            }
            for a, b in zip(path, path[1:], strict=False)
        ]

    def find_cycles(self) -> list[list[str]]:
        """All elementary cycles reachable by DFS (graphs here are small)."""
        adj: dict[str, list[str]] = {}
        for e in self.edges:
            adj.setdefault(e.upstream, []).append(e.downstream)
        cycles: list[list[str]] = []
        seen_keys: set[tuple[str, ...]] = set()
        state: dict[str, int] = {}  # 0 unvisited, 1 on stack, 2 done
        stack: list[str] = []

        def visit(n: str) -> None:
            state[n] = 1
            stack.append(n)
            for m in adj.get(n, []):
                if state.get(m, 0) == 0:
                    visit(m)
                elif state.get(m) == 1:
                    cyc = stack[stack.index(m) :]
                    k = min(range(len(cyc)), key=lambda i: cyc[i])
                    key = tuple(cyc[k:] + cyc[:k])
                    if key not in seen_keys:
                        seen_keys.add(key)
                        cycles.append([*key, key[0]])
            stack.pop()
            state[n] = 2

        for n in sorted(self.nodes):
            if state.get(n, 0) == 0:
                visit(n)
        return cycles

    def checksum(self) -> str:
        payload = sorted(
            (e.upstream, e.downstream, e.extraction, e.relationship) for e in self.edges
        )
        return hashlib.sha256(json.dumps(payload).encode()).hexdigest()


# ----------------------------------------------------------------------- build
def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dataset_id_for_node(node: dict[str, Any]) -> str:
    rt = node["resource_type"]
    if rt == "exposure":
        return f"bi.dashboard.{node['name']}"
    if rt == "source":
        return f"warehouse.{node['schema']}.{node['identifier']}"
    return f"warehouse.{node['schema']}.{node.get('alias') or node['name']}"


def build_graph(manifest_path: Path, declared_path: Path = DECLARED_PATH) -> Graph:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    declared = yaml.safe_load(declared_path.read_text(encoding="utf-8"))
    contracts = load_contracts()
    g = Graph(sources={"dbt_manifest": _sha(manifest_path), "declared": _sha(declared_path)})

    # Source tables (from the contract) and declared logical datasets.
    for t, c in contracts.items():
        crit = "critical" if t in CRITICAL_SOURCE_TABLES else "standard"
        g.nodes[f"commerce_source.shop.{t}"] = {
            "system": "commerce_source",
            "kind": "source_table",
            "display_name": f"shop.{t}",
            "owner": "commerce-platform",
            "criticality": crit,
            "freshness_hours": c.freshness_hours,
            "contract_id": f"{c.contract}.v{c.version}",
            "registered_by": "contract",
            "description": f"Operational table shop.{t}",
        }
        for prefix, spec_key in (
            ("warehouse.raw", "raw"),
            ("kafka.cdc.shop", "topic"),
            ("lakehouse.bronze", "bronze"),
            ("lakehouse.silver", "silver"),
            ("warehouse.cdc_pub", "cdc_pub"),
        ):
            spec = declared["datasets"][spec_key]
            g.nodes[f"{prefix}.{t}"] = {
                "system": spec["system"],
                "kind": spec["kind"],
                "display_name": f"{prefix.split('.')[-1]}.{t}",
                "owner": spec["owner"],
                "criticality": crit,
                "freshness_hours": c.freshness_hours,
                "contract_id": f"{c.contract}.v{c.version}",
                "registered_by": "declared",
                "description": spec["description"].format(t=t),
            }
    g.nodes["lakehouse.silver.shipment_events_hourly"] = {
        "system": "lakehouse",
        "kind": "delta_table",
        "display_name": "silver.shipment_events_hourly",
        "owner": "data-platform",
        "criticality": "standard",
        "freshness_hours": 12,
        "contract_id": None,
        "registered_by": "declared",
        "description": "Event-time hourly shipment event counts (streaming, watermark)",
    }

    # dbt nodes: models, seeds, snapshots and exposures (tests are checks, not datasets).
    coarse = set(declared["coarse_sources"])
    staging_for = {v: k for k, v in declared["staging_inputs"].items()}
    for uid, node in {**manifest["nodes"], **manifest["exposures"]}.items():
        if node["resource_type"] not in ("model", "seed", "snapshot", "exposure"):
            continue
        ds = dataset_id_for_node(node)
        g.node_by_unique_id[uid] = ds
        meta = node.get("meta") or node.get("config", {}).get("meta") or {}
        exposure_crit = node.get("maturity") == "high"
        g.nodes[ds] = {
            "system": "bi" if node["resource_type"] == "exposure" else "warehouse",
            "kind": node["resource_type"],
            "display_name": node.get("label") or node["name"],
            "owner": (node.get("owner") or {}).get("name")
            if node["resource_type"] == "exposure"
            else meta.get("owner"),
            "criticality": "critical" if exposure_crit else "standard",
            "freshness_hours": None,
            "contract_id": None,
            "registered_by": "dbt_manifest",
            "description": (node.get("description") or "")[:500] or None,
        }
    for uid, src in manifest["sources"].items():
        g.node_by_unique_id[uid] = dataset_id_for_node(src)

    for uid, node in {**manifest["nodes"], **manifest["exposures"]}.items():
        if uid not in g.node_by_unique_id:
            continue
        for parent in node.get("depends_on", {}).get("nodes", []):
            if parent in coarse:
                continue  # replaced by declared per-table edges
            if parent in g.node_by_unique_id and g.node_by_unique_id[parent] in g.nodes:
                rel = "feeds_dashboard" if node["resource_type"] == "exposure" else "depends_on"
                g.edges.append(
                    Edge(g.node_by_unique_id[parent], g.node_by_unique_id[uid], "dbt_manifest", rel)
                )

    staging_ds = {
        t: g.node_by_unique_id.get(f"model.dataops.{m}")
        for m, t in declared["staging_inputs"].items()
    }
    for t in contracts:
        for path_name in ("batch", "cdc"):
            for up, down, rel in declared["paths"][path_name]:
                up_id = staging_ds[t] if up == "staging:{t}" else up.format(t=t)
                down_id = staging_ds[t] if down == "staging:{t}" else down.format(t=t)
                if up_id and down_id:
                    g.edges.append(Edge(up_id, down_id, "declared", rel))
    for up, down, rel in declared["paths"].get("extra", []):
        g.edges.append(Edge(up, down, "declared", rel))
    _ = staging_for  # mapping kept in YAML for readers; validated below
    missing = [
        m for m in declared["staging_inputs"] if f"model.dataops.{m}" not in manifest["nodes"]
    ]
    if missing:
        raise ValueError(f"declared staging models missing from the manifest: {missing}")
    g.edges = sorted(set(g.edges), key=lambda e: (e.upstream, e.downstream))
    return g


# ------------------------------------------------------------------- persist
def persist(g: Graph) -> str:
    """Upsert the dataset registry and store a new lineage version if the graph changed.
    Returns the current lineage_version."""
    checksum = g.checksum()
    with connect(Role.REL_APP, application_name="dataops-lineage") as conn, conn.transaction():
        for ds, a in g.nodes.items():
            conn.execute(
                "insert into reliability.datasets (dataset_id, system, kind, display_name, owner,"
                " description, criticality, freshness_hours, contract_id, registered_by)"
                " values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                " on conflict (dataset_id) do update set system=excluded.system,"
                " kind=excluded.kind,"
                " display_name=excluded.display_name, owner=excluded.owner,"
                " description=excluded.description, criticality=excluded.criticality,"
                " freshness_hours=excluded.freshness_hours, contract_id=excluded.contract_id,"
                " registered_by=excluded.registered_by, last_seen_at=clock_timestamp()",
                (
                    ds,
                    a["system"],
                    a["kind"],
                    a["display_name"],
                    a.get("owner"),
                    a.get("description"),
                    a["criticality"],
                    a.get("freshness_hours"),
                    a.get("contract_id"),
                    a["registered_by"],
                ),
            )
        latest = conn.execute(
            "select lineage_version, graph_checksum from reliability.lineage_observations"
            " order by observed_at desc limit 1"
        ).fetchone()
        if latest and latest[1] == checksum:
            return str(latest[0])
        version = str(uuid.uuid4())
        conn.execute(
            "insert into reliability.lineage_observations (lineage_version, sources, edge_count,"
            " graph_checksum) values (%s,%s,%s,%s)",
            (version, json.dumps(g.sources), len(g.edges), checksum),
        )
        with conn.cursor() as cur:
            cur.executemany(
                "insert into reliability.lineage_edges (lineage_version, upstream_id,"
                " downstream_id, extraction, relationship) values (%s,%s,%s,%s,%s)",
                [
                    (version, e.upstream, e.downstream, e.extraction, e.relationship)
                    for e in g.edges
                ],
            )
        return version


def load_latest(role: Role = Role.REL_READER) -> tuple[Graph, str | None]:
    """Graph of the latest lineage version (from the database, not from files)."""
    with connect(role, application_name="dataops-lineage") as conn:
        row = conn.execute(
            "select lineage_version from reliability.lineage_observations"
            " order by observed_at desc limit 1"
        ).fetchone()
        if not row:
            return Graph(), None
        version = str(row[0])
        g = Graph()
        for ds, system, kind, name, crit in conn.execute(
            "select dataset_id, system, kind, display_name, criticality from reliability.datasets"
        ).fetchall():
            g.nodes[ds] = {
                "system": system,
                "kind": kind,
                "display_name": name,
                "criticality": crit,
            }
        g.edges = [
            Edge(*r)
            for r in conn.execute(
                "select upstream_id, downstream_id, extraction, relationship"
                " from reliability.lineage_edges where lineage_version = %s",
                (version,),
            ).fetchall()
        ]
    return g, version


def impact(g: Graph, dataset: str) -> dict[str, Any]:
    """Downstream impact with dependency paths, dashboards first."""
    down = g.downstream(dataset)
    rows = [
        {
            "dataset_id": d,
            "kind": g.nodes.get(d, {}).get("kind"),
            "criticality": g.nodes.get(d, {}).get("criticality"),
            "hops": len(p) - 1,
            "path": p,
        }
        for d, p in down.items()
    ]
    rows.sort(key=lambda r: (r["kind"] != "exposure", r["hops"], r["dataset_id"]))
    return {
        "dataset_id": dataset,
        "downstream_count": len(rows),
        "dashboards": [r["dataset_id"] for r in rows if r["kind"] == "exposure"],
        "impacted": rows,
    }


def most_upstream(g: Graph, datasets: Iterable[str]) -> list[str]:
    """Members of `datasets` that have no other member upstream of them (root candidates)."""
    ds = [d for d in set(datasets) if d in g.nodes]
    roots = []
    for d in ds:
        ups = set(g.upstream(d))
        if not any(o in ups for o in ds if o != d):
            roots.append(d)
    return sorted(roots)
