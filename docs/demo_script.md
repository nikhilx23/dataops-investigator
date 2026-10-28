# Five-minute demo script

Prepared state: lightweight databases loaded (`scripts/demo_m1.sh` or `scripts/demo_m2.py`),
`python -m dataops runbooks index` done, API and dashboard running:

```bash
uvicorn dataops.api.app:app --host 127.0.0.1 --port 8000 &
streamlit run apps/dashboard/app.py &          # http://127.0.0.1:8501
docker compose --profile monitoring up -d       # optional: Grafana http://127.0.0.1:3000
```

| Time | Do | Say |
|---|---|---|
| 0:00 | Dashboard → Overview (all green) | "A synthetic commerce platform: batch and CDC ingestion, dbt marts, reconciliation against the source and an independent ledger. Everything you see is collected metadata." |
| 0:30 | Terminal: `python -m dataops scenario inject late_arriving_payments` then `python -m dataops scenario cycle` | "The payments service commits captures with timestamps behind our extraction watermark — a real failure mode of watermark-based extraction." |
| 1:00 | Dashboard → Overview: 1 sev1 incident; Incidents → Signals, Impact | "Reconciliation caught it, the engine opened one incident, lineage shows the revenue dashboard is affected. The root is *inferred* — the engine only sees the symptom at the mart." |
| 1:45 | Incidents → Investigation → Run (model mode if a key is configured, otherwise fixture and say so) | "The investigator can only call read-only tools. Every fact cites a stored record; numbers must appear in the cited evidence; anything else is removed by the validator, and it abstains when evidence is thin." |
| 2:45 | Recovery tab: propose `bounded_backfill` (orders+payments, the commit window from `scenario_state`) → approve with your name and reason → execute → verify | "Predefined actions only, preconditions re-checked, a named person approves. Verification after the backfill still fails — the marts are not rebuilt yet. That is the honest answer." |
| 3:45 | Propose `rebuild_models` `{"models":["stg_orders","stg_payments"],"full_refresh":true}` → approve → execute → verify | "Now no signal remains, so the incident is resolved and the cause becomes *confirmed* — only a verified fix can confirm a cause." |
| 4:30 | History tab; Grafana panel; `python -m dataops scenario cleanup late_arriving_payments` | "Every step is audited with who and why; Prometheus shows the incident open and closed. Cleanup restores the synthetic source." |

Shortcut: `python -m dataops scenario run late_arriving_payments --actor "<your name>"` does the
whole flow unattended and writes a report.
