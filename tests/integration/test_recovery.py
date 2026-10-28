"""Recovery lifecycle against PostgreSQL (*_test databases)."""

from __future__ import annotations

import pytest

from dataops.config import Role
from dataops.db import connect
from dataops.recovery import service
from tests.integration.test_investigator import prepared  # noqa: F401 - shared fixture

ACTOR = "test-operator"


def test_lifecycle_guards_and_audit(prepared):  # noqa: F811
    scope = {
        "dataset_id": "commerce_source.shop.payments",
        "message": "amount_cents changed type; please restore integer cents",
    }
    a = service.propose("escalate_to_owner", scope, incident_id=None, proposed_by="investigator")
    assert a["status"] == "proposed" and all(p["status"] == "met" for p in a["preconditions"])
    with pytest.raises(service.RecoveryError):
        service.execute(a["action_id"], ACTOR)  # not approved
    with pytest.raises(service.RecoveryError):
        service.approve(a["action_id"], "investigator", "looks right")  # not a human
    with pytest.raises(service.RecoveryError):
        service.approve(a["action_id"], ACTOR, "")  # no reason
    service.approve(a["action_id"], ACTOR, "owner must fix the source")
    with pytest.raises(service.RecoveryError):
        service.approve(a["action_id"], ACTOR, "twice")
    done = service.execute(a["action_id"], ACTOR)
    assert (
        done["status"] == "succeeded"
        and done["execution_output"]["escalated_to"] == "commerce-platform"
    )
    assert [h["to_status"] for h in done["history"]] == [
        "proposed",
        "approved",
        "running",
        "succeeded",
    ]
    assert done["approved_by"] == ACTOR and done["executed_by"] == ACTOR


def test_unmet_preconditions_block_approval(prepared):  # noqa: F811
    a = service.propose(
        "escalate_to_owner",
        {"dataset_id": "warehouse.marts.no_such_model", "message": "nobody owns this dataset"},
        incident_id=None,
        proposed_by="investigator",
    )
    assert a["preconditions"][0]["status"] == "unmet"
    with pytest.raises(service.RecoveryError, match="preconditions not met"):
        service.approve(a["action_id"], ACTOR, "try anyway")
    got = service.get(a["action_id"])
    assert got["status"] == "proposed" and "approval refused" in got["history"][-1]["reason"]


def test_reader_role_cannot_change_recovery_state(prepared):  # noqa: F811
    import psycopg

    with connect(Role.REL_READER) as conn, pytest.raises(psycopg.errors.InsufficientPrivilege):
        conn.execute("update reliability.recovery_actions set status='verified'")


def test_api_reads_and_metrics(prepared):  # noqa: F811
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient

    from dataops.api.app import app

    c = TestClient(app)
    assert c.get("/health").json()["status"] == "ok"
    assert "pipelines" in c.get("/overview").json()
    text = c.get("/metrics").text
    assert "dataops_metadata_up 1.0" in text and "dataops_active_incidents" in text
    assert "escalate_to_owner" in c.get("/recovery/catalog").json()
