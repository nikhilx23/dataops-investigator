"""DataOps Investigator dashboard (Streamlit).

Talks only to the API (no database credentials here). Write actions send the API token from
.env and the operator name typed in the sidebar; the API enforces approvals and
preconditions. Run: streamlit run apps/dashboard/app.py
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx
import pandas as pd
import streamlit as st

from dataops.config import load_dotenv

load_dotenv()
API = os.environ.get("DATAOPS_API_URL", "http://127.0.0.1:8000")
TOKEN = os.environ.get("DATAOPS_API_TOKEN", "")

st.set_page_config(page_title="DataOps Investigator", layout="wide")
SEV_ICON = {"sev1": "🔴", "sev2": "🟠", "sev3": "🟡"}


def get(path: str, **params: Any) -> Any:
    r = httpx.get(f"{API}{path}", params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def post(path: str, body: dict[str, Any] | None = None, timeout: float = 600) -> tuple[int, Any]:
    r = httpx.post(
        f"{API}{path}",
        json=body or {},
        timeout=timeout,
        headers={"Authorization": f"Bearer {TOKEN}", "X-Actor": st.session_state.get("actor", "")},
    )
    try:
        return r.status_code, r.json()
    except ValueError:
        return r.status_code, r.text


st.sidebar.title("DataOps Investigator")
st.sidebar.caption("Synthetic commerce platform — all data is generated.")
page = st.sidebar.radio("View", ["Overview", "Incidents", "Recovery history", "AI evaluation"])
st.session_state["actor"] = st.sidebar.text_input(
    "Operator name (for approvals)", st.session_state.get("actor", "")
)
try:
    st.sidebar.success(f"API: {get('/health')['status']}")
except Exception as exc:  # noqa: BLE001
    st.sidebar.error(f"API unreachable at {API}: {exc}")
    st.stop()


def overview() -> None:
    o = get("/overview")
    st.header("Platform health")
    active = o["active_incidents"]
    rec = o["reconciliation"]
    stale = len(o["freshness_stale"])
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Active incidents", len(active))
    c2.metric(
        "Reconciliation checks passing", f"{sum(r['status'] == 'pass' for r in rec)} / {len(rec)}"
    )
    c3.metric("Stale datasets", stale)
    c4.metric("Quality failures (7 days)", o["quality_failures_7d"][0]["n"])
    if active:
        st.subheader("Active incidents")
        st.dataframe(
            pd.DataFrame(active)[
                ["severity", "title", "status", "root_dataset_id", "signals", "detected_at"]
            ],
            hide_index=True,
            use_container_width=True,
        )
    st.subheader("Pipelines")
    st.dataframe(pd.DataFrame(o["pipelines"]), hide_index=True, use_container_width=True)
    st.subheader("Reconciliation (latest per check)")
    st.dataframe(pd.DataFrame(rec), hide_index=True, use_container_width=True)
    if stale:
        st.subheader("Stale datasets")
        st.dataframe(pd.DataFrame(o["freshness_stale"]), hide_index=True, use_container_width=True)


def render_investigation(out: dict[str, Any], validation: dict[str, Any] | None, mode: str) -> None:
    if mode == "fixture":
        st.warning("Fixture mode: deterministic rule-based harness — not AI inference.")
    if out.get("abstain"):
        st.info(f"Abstained: {out.get('abstain_reason')}")
    st.markdown(f"**Summary.** {out.get('summary')}")
    for c in out.get("likely_causes", []):
        st.markdown(
            f"**Likely cause (inferred, {c['confidence']} confidence):** {c['cause']}  \n"
            f"_Category_ `{c['category']}` · _dataset_ `{c.get('dataset_id')}` ·"
            f" {c['confidence_rationale']}"
        )
        st.caption("Supporting: " + ", ".join(c["supporting_evidence"]))
        if c.get("conflicting_evidence"):
            st.caption("Conflicting: " + ", ".join(c["conflicting_evidence"]))
        for note in c.get("conflict_notes") or []:
            st.caption(f"Does not fit: {note}")
    if out.get("observed_facts"):
        st.markdown("**Observed facts**")
        for f in out["observed_facts"]:
            st.markdown(
                f"- {f['statement']}  \n  <small>{', '.join(f['evidence_ids'])}</small>",
                unsafe_allow_html=True,
            )
    if out.get("recovery_options"):
        st.markdown(
            "**Suggested recovery options** (predefined actions; nothing runs without approval)"
        )
        for o in out["recovery_options"]:
            pre = (
                "; ".join(f"{p['description']}: **{p['status']}**" for p in o["preconditions"])
                or "—"
            )
            st.markdown(
                f"- `{o['action']}` — {o['scope']} — {o['rationale']}  \n  preconditions: {pre}"
            )
    for k, label in (
        ("recommended_checks", "Recommended checks"),
        ("missing_evidence", "Missing evidence"),
    ):
        if out.get(k):
            st.markdown(f"**{label}**")
            for x in out[k]:
                st.markdown(f"- {x}")
    if validation:
        st.caption(
            f"Validation:"
            f" {validation.get('citations_valid')}/{validation.get('citations_total')} citations"
            f" valid; removed facts {len(validation.get('removed_facts', []))}; removed causes"
            f" {len(validation.get('removed_causes', []))}; forced abstention"
            f" {validation.get('forced_abstention')}"
        )


def incidents_page() -> None:
    st.header("Incidents")
    show_all = st.toggle("Include resolved and closed", value=True)
    rows = get("/incidents", all=str(show_all).lower())
    if not rows:
        st.success("No incidents.")
        return
    labels = {}
    for r in rows:
        icon = SEV_ICON.get(r["severity"], "")
        labels[f"{icon} {r['status']:<9} {r['title']} ({r['incident_id'][:8]})"] = r["incident_id"]
    choice = st.selectbox("Incident", list(labels))
    inc = get(f"/incidents/{labels[choice]}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Severity", inc["severity"])
    c2.metric("Status", inc["status"])
    c3.metric("Root (cause status)", f"{inc['root_cause_status']}")
    c4.metric("Detected → verified (min)", inc["timeline"].get("to_verified_minutes") or "—")
    st.caption(
        f"Root dataset: `{inc.get('root_dataset_id')}` — inferred by the rule engine unless"
        f" confirmed"
        " by a verified recovery."
    )
    t1, t2, t3, t4, t5 = st.tabs(
        ["Signals & evidence", "Impact", "Investigation", "Recovery", "History"]
    )
    with t1:
        st.dataframe(pd.DataFrame(inc["events"]), hide_index=True, use_container_width=True)
        st.dataframe(pd.DataFrame(inc["evidence"]), hide_index=True, use_container_width=True)
    with t2:
        imp = get(f"/incidents/{inc['incident_id']}/impact")
        if imp.get("impacted") is not None:
            st.write(
                f"{imp['downstream_count']} downstream datasets; dashboards: "
                + (", ".join(imp["dashboards"]) or "none")
            )
            st.dataframe(
                pd.DataFrame(imp["impacted"])[["dataset_id", "kind", "hops", "path"]],
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info(imp.get("note") or "no impact information for this incident")
    with t3:
        mode = st.radio(
            "Mode",
            ["model", "fixture"],
            horizontal=True,
            help="model = real LLM (needs ANTHROPIC_API_KEY); fixture = labelled test harness",
        )
        if st.button("Run investigation"):
            with st.spinner("Investigating with read-only tools…"):
                code, res = post(f"/incidents/{inc['incident_id']}/investigations", {"mode": mode})
            if code == 200 and res.get("output"):
                render_investigation(res["output"], res.get("validation"), res["mode"])
            else:
                st.error(f"{code}: {res}")
        if inc["investigations"]:
            st.markdown("**Previous investigations**")
            st.dataframe(
                pd.DataFrame(inc["investigations"]), hide_index=True, use_container_width=True
            )
            pick = st.selectbox("Show", [i["investigation_id"] for i in inc["investigations"]])
            rec = get(f"/investigations/{pick}")
            if rec.get("output"):
                render_investigation(rec["output"], rec.get("validation"), rec["mode"])
    with t4:
        recovery_panel(inc)
    with t5:
        st.dataframe(pd.DataFrame(inc["history"]), hide_index=True, use_container_width=True)


def recovery_panel(inc: dict[str, Any]) -> None:
    cat = get("/recovery/catalog")
    st.markdown(
        "Predefined actions only. Each needs a named operator and a reason; preconditions are"
        " re-checked at approval and execution; verification re-evaluates the incident."
    )
    with st.form("propose"):
        action = st.selectbox(
            "Action", list(cat), format_func=lambda a: f"{a} — {cat[a]['description']}"
        )
        scope = st.text_area("Scope (JSON)", value='{"models": ["stg_payments"]}')
        if st.form_submit_button("Propose (runs preconditions and a dry run)"):
            try:
                code, res = post(
                    "/recovery",
                    {
                        "incident_id": inc["incident_id"],
                        "action": action,
                        "scope": json.loads(scope),
                    },
                )
                st.write(
                    code,
                    res
                    if code != 200
                    else {
                        k: res[k]
                        for k in ("action_id", "status", "preconditions", "dry_run_output")
                    },
                )
            except json.JSONDecodeError as exc:
                st.error(f"scope is not valid JSON: {exc}")
    for a in inc["recovery_actions"]:
        with st.expander(f"{a['action_type']} — {a['status']} ({a['action_id'][:8]})"):
            full = get(f"/recovery/{a['action_id']}")
            st.json(
                {
                    k: full[k]
                    for k in (
                        "scope",
                        "preconditions",
                        "dry_run_output",
                        "approved_by",
                        "approval_reason",
                        "execution_output",
                        "verification",
                    )
                }
            )
            st.dataframe(pd.DataFrame(full["history"]), hide_index=True, use_container_width=True)
            reason = st.text_input("Reason", key=f"r-{a['action_id']}")
            cols = st.columns(4)
            for col, verb in zip(cols, ("approve", "reject", "execute", "verify"), strict=True):
                if col.button(verb.capitalize(), key=f"{verb}-{a['action_id']}"):
                    body = {"reason": reason} if verb in ("approve", "reject") else None
                    code, res = post(f"/recovery/{a['action_id']}/{verb}", body)
                    (st.success if code == 200 else st.error)(
                        f"{code}: {res.get('status') if code == 200 else res}"
                    )


def history_page() -> None:
    st.header("Recovery history")
    rows = get("/recovery")
    if rows:
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)
    else:
        st.info("No recovery actions yet.")


def evaluation_page() -> None:
    from dataops.config import REPO_ROOT

    st.header("AI investigator evaluation")
    for mode in ("model", "fixture"):
        files = sorted((REPO_ROOT / "evaluations" / "results" / mode).glob("*.json"))
        st.subheader(mode)
        if not files:
            st.info("No run yet." + (" Needs ANTHROPIC_API_KEY." if mode == "model" else ""))
            continue
        rep = json.loads(files[-1].read_text())
        st.caption(rep["label"])
        m = rep["metrics"]
        st.dataframe(
            pd.DataFrame(
                [
                    {"metric": k, "rate": v.get("rate"), "n": v.get("n"), "d": v.get("d")}
                    for k, v in m.items()
                    if isinstance(v, dict) and "rate" in v
                ]
            ),
            hide_index=True,
            use_container_width=True,
        )


{
    "Overview": overview,
    "Incidents": incidents_page,
    "Recovery history": history_page,
    "AI evaluation": evaluation_page,
}[page]()
