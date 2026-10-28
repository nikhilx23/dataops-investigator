# Recovery guide

Recovery is a separate, controlled service (`src/dataops/recovery/`). The investigator only
*suggests* predefined actions; nothing runs until a named person approves it.

## Actions

| Action | Scope (validated) | Preconditions (checked at proposal, approval **and** execution) | Executes | Verification |
|---|---|---|---|---|
| `retry_failed_batch` | `batch_id` | batch failed/abandoned, or `running` with **no live loader session** (crashed process); no later batch already covers the window; no other running batch; source reachable | marks an orphaned batch `abandoned` (metadata only), re-extracts the same window as a new atomic batch | row-count reconciliation |
| `bounded_backfill` | `tables` (source tables only), `start`/`end` (tz-aware, ≤ 31 days, in the past) | no running batch; source schema matches contract v1 | append-only re-extraction, one window per day, no overlap | row-count reconciliation |
| `rebuild_models` | `models` (`stg_/int_/dim_/fct_/mart_` names that exist in the dbt project), `include_downstream`, `full_refresh` | every model is a derived dbt model; source schema matches contract | `dbt build --select <models>[+]` (tests included); dry run = `dbt ls` | dbt exit 0 + source↔mart reconciliation |
| `reprocess_quarantine` | `tables` | full-profile lakehouse present; source schema matches contract | deterministic Silver replay from Bronze | CDC source↔stream reconciliation |
| `escalate_to_owner` | `dataset_id`, `message` | dataset has an owner | records the escalation (no system change) | none (the owner's fix is verified by the follow-up action) |

Not available, by design: dropping/truncating data, deleting checkpoints or replication slots,
editing rows, resets, arbitrary SQL or shell commands.

## Lifecycle

```
proposed ──approve (named human + reason, all preconditions met)──► approved ──execute──► running
   │                                                                                         │
   └──reject──► rejected                                    succeeded / failed ◄─────────────┘
                                                                  │
                                                         verify ──┴─► verified | verification_failed
```

- Every transition is appended to `reliability.recovery_action_events` (who, when, why,
  details). A refused approval or execution is recorded too.
- Service identities (`investigator`, `system`, `ai`, …) cannot approve or execute.
- One action runs at a time (PostgreSQL advisory lock).
- **Verify** runs the action's own check, re-collects metadata (dbt, ingestion, schema,
  freshness), re-evaluates the incident rules and lists every signal that still fires for the
  incident's datasets. Only with none left — and only after a corrective (data-changing)
  action — is the incident set to `resolved` and its cause to `confirmed`. A backfill alone
  usually ends in `verification_failed` because the marts are not rebuilt yet; that is the
  intended, honest result.

## Interfaces

```bash
python -m dataops recovery actions
python -m dataops recovery propose --incident <id> --action rebuild_models \
    --scope '{"models": ["stg_payments"], "full_refresh": true}' --actor "Jane Doe"
python -m dataops recovery approve <action-id> --actor "Jane Doe" --reason "reviewed dry run"
python -m dataops recovery execute <action-id> --actor "Jane Doe"
python -m dataops recovery verify  <action-id> --actor "Jane Doe"
python -m dataops recovery timeline --incident <id>
```

API: `POST /recovery`, `/recovery/{id}/approve|reject|execute|verify` with
`Authorization: Bearer $DATAOPS_API_TOKEN` and `X-Actor: <name>`; the dashboard's Recovery tab
uses the same endpoints.
