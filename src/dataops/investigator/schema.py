"""Structured investigation output. The model submits it through the `submit_investigation`
tool, so it arrives as JSON validated against this schema; the validator then checks every
citation, number and recovery option against the evidence."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

EvidenceIds = list[str]
ACTIONS = (
    "retry_failed_batch",
    "bounded_backfill",
    "rebuild_models",
    "reprocess_quarantine",
    "escalate_to_owner",
)
CATEGORIES = (
    "schema_change",
    "pipeline_execution",
    "source_freshness",
    "streaming",
    "data_quality",
    "reconciliation",
    "unknown",
)


class _M(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Fact(_M):
    statement: str = Field(max_length=400)
    evidence_ids: EvidenceIds = Field(min_length=1, max_length=8)


class Cause(_M):
    cause: str = Field(max_length=400, description="what most likely caused the incident")
    category: Literal[CATEGORIES]  # type: ignore[valid-type]
    dataset_id: str | None = Field(None, description="dataset where the cause originates")
    confidence: Literal["high", "medium", "low"] = Field(
        description="qualitative; never a percentage"
    )
    confidence_rationale: str = Field(max_length=300)
    supporting_evidence: EvidenceIds = Field(min_length=1, max_length=10)
    conflicting_evidence: EvidenceIds = Field(default_factory=list, max_length=10)


class AffectedPath(_M):
    dataset_id: str
    path: list[str] = Field(default_factory=list, max_length=15)
    evidence_ids: EvidenceIds = Field(default_factory=list, max_length=4)


class Precondition(_M):
    description: str = Field(max_length=200)
    status: Literal["met", "unmet", "unknown"]
    evidence_ids: EvidenceIds = Field(default_factory=list, max_length=4)


class RecoveryOption(_M):
    action: Literal[ACTIONS]  # type: ignore[valid-type]
    scope: str = Field(max_length=300, description="exact tables/models/window")
    rationale: str = Field(max_length=300)
    preconditions: list[Precondition] = Field(default_factory=list, max_length=6)
    evidence_ids: EvidenceIds = Field(default_factory=list, max_length=6)


class Investigation(_M):
    abstain: bool = Field(description="true when the evidence is insufficient to name a cause")
    abstain_reason: str | None = Field(None, max_length=400)
    summary: str = Field(max_length=800)
    observed_facts: list[Fact] = Field(default_factory=list, max_length=15)
    likely_causes: list[Cause] = Field(default_factory=list, max_length=4)
    affected: list[AffectedPath] = Field(default_factory=list, max_length=20)
    recommended_checks: list[str] = Field(default_factory=list, max_length=8)
    recovery_options: list[RecoveryOption] = Field(default_factory=list, max_length=5)
    missing_evidence: list[str] = Field(default_factory=list, max_length=8)
    evidence_references: EvidenceIds = Field(default_factory=list, max_length=40)
