"""Recovery scope validation and actor rules (no database)."""

from __future__ import annotations

import pytest

from dataops.recovery import service
from dataops.recovery.actions import SPECS


@pytest.mark.parametrize(
    ("action", "scope"),
    [
        (
            "bounded_backfill",
            {
                "tables": ["payments"],
                "start": "2026-01-01T00:00:00+00:00",
                "end": "2026-03-01T00:00:00+00:00",
            },
        ),  # > 31 days
        (
            "bounded_backfill",
            {"tables": ["payments"], "start": "2026-01-02T00:00:00", "end": "2026-01-03T00:00:00"},
        ),  # naive timestamps
        (
            "bounded_backfill",
            {
                "tables": ["pg_authid"],
                "start": "2026-01-01T00:00:00+00:00",
                "end": "2026-01-02T00:00:00+00:00",
            },
        ),  # not a source table
        ("rebuild_models", {"models": ["stg_payments; drop table raw.source_records"]}),
        ("rebuild_models", {"models": ["source:raw"]}),
        ("rebuild_models", {"models": ["stg_payments"], "command": "dbt run"}),  # extra field
        ("retry_failed_batch", {"batch_id": "1 or 1=1"}),
        ("drop_and_reload", {"table": "payments"}),
    ],
)
def test_invalid_scopes_are_refused(action, scope):
    with pytest.raises(service.RecoveryError):
        service._parse(action, scope)


def test_valid_scopes_parse():
    s = service._parse(
        "bounded_backfill",
        {
            "tables": ["payments", "refunds"],
            "start": "2026-03-01T00:00:00+00:00",
            "end": "2026-03-31T00:00:00+00:00",
        },
    )
    assert s.tables == ["payments", "refunds"]
    assert service._parse("rebuild_models", {"models": ["stg_payments"]}).include_downstream


@pytest.mark.parametrize("actor", ["", "  ", "investigator", "AI", "system", "incident_engine"])
def test_service_identities_cannot_approve_or_execute(actor):
    with pytest.raises(service.RecoveryError):
        service._human(actor)


def test_action_catalog_has_no_destructive_action():
    assert set(SPECS) == {
        "retry_failed_batch",
        "bounded_backfill",
        "rebuild_models",
        "reprocess_quarantine",
        "escalate_to_owner",
    }


def test_scenario_recovery_plans_use_only_catalog_actions():
    from dataops.scenarios import library

    assert {s.number for s in library.SCENARIOS.values()} == {2, 3, 4, 5, 6, 7, 8}
    for s in library.SCENARIOS.values():
        for step in s.recovery_plan:
            if step.get("cleanup_first"):
                continue
            assert step["action"] in SPECS, (s.id, step)
            scope = dict(step["scope"])
            if scope.pop("window", None):
                scope.update(start="2026-03-01T00:00:00+00:00", end="2026-03-02T00:00:00+00:00")
            if str(scope.get("batch_id", "")).startswith("latest_failed:"):
                scope["batch_id"] = "00000000-0000-0000-0000-000000000000"
            service._parse(step["action"], scope)
