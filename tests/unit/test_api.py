"""API write protection (no database needed: auth runs before any query)."""

from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from dataops.api.app import app  # noqa: E402

IID = "00000000-0000-0000-0000-000000000001"
client = TestClient(app)


def test_writes_disabled_without_configured_token(monkeypatch):
    monkeypatch.delenv("DATAOPS_API_TOKEN", raising=False)
    r = client.post(
        f"/incidents/{IID}/investigations",
        json={"mode": "fixture"},
        headers={"Authorization": "Bearer x", "X-Actor": "a"},
    )
    assert r.status_code == 403


@pytest.mark.parametrize(
    ("headers", "code"),
    [
        ({}, 401),
        ({"Authorization": "Bearer wrong", "X-Actor": "a"}, 401),
        ({"Authorization": "Bearer s3cret-token", "X-Actor": "  "}, 400),
    ],
)
def test_writes_need_token_and_named_actor(monkeypatch, headers, code):
    monkeypatch.setenv("DATAOPS_API_TOKEN", "s3cret-token")
    assert client.post(f"/recovery/{IID}/execute", headers=headers).status_code == code


def test_recovery_decision_needs_a_reason(monkeypatch):
    monkeypatch.setenv("DATAOPS_API_TOKEN", "s3cret-token")
    r = client.post(
        f"/recovery/{IID}/approve",
        json={"reason": ""},
        headers={"Authorization": "Bearer s3cret-token", "X-Actor": "op"},
    )
    assert r.status_code == 422


def test_investigation_mode_is_an_enum(monkeypatch):
    monkeypatch.setenv("DATAOPS_API_TOKEN", "s3cret-token")
    r = client.post(
        f"/incidents/{IID}/investigations",
        json={"mode": "shell"},
        headers={"Authorization": "Bearer s3cret-token", "X-Actor": "op"},
    )
    assert r.status_code == 422
