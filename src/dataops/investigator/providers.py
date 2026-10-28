"""LLM providers.

- `AnthropicProvider`: real inference through the Anthropic Messages API with tool use.
  Model from `DATAOPS_LLM_MODEL`; key from `ANTHROPIC_API_KEY` (never logged or stored).
- `FixtureProvider`: a deterministic, rule-based script that drives the *same* loop (tool
  calls, validation, audit) for offline integration checks. It is not AI inference; its runs
  are recorded with mode='fixture' and are never mixed into model-quality metrics.
"""

from __future__ import annotations

import contextlib
import json
import os
from dataclasses import dataclass, field
from typing import Any, Protocol, cast


@dataclass
class Response:
    blocks: list[dict[str, Any]]
    stop_reason: str | None
    input_tokens: int = 0
    output_tokens: int = 0


class Provider(Protocol):
    mode: str
    name: str

    def respond(
        self,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        force_tool: str | None = None,
    ) -> Response: ...


class ProviderUnavailable(RuntimeError):
    pass


class AnthropicProvider:
    mode = "model"

    def __init__(self, model: str | None = None) -> None:
        key = os.environ.get("ANTHROPIC_API_KEY")
        if not key:
            raise ProviderUnavailable(
                "ANTHROPIC_API_KEY is not set. Real investigations need it; the offline"
                " fixture mode (--mode fixture) is a labelled test harness, not AI inference."
            )
        import anthropic

        self.model = model or os.environ.get("DATAOPS_LLM_MODEL", "claude-sonnet-5-5")
        self.name = f"anthropic:{self.model}"
        self._client = anthropic.Anthropic(
            api_key=key,
            max_retries=int(os.environ.get("DATAOPS_LLM_MAX_RETRIES", "2")),
            timeout=float(os.environ.get("DATAOPS_LLM_TIMEOUT_S", "90")),
        )
        self._max_tokens = int(os.environ.get("DATAOPS_LLM_MAX_TOKENS", "4096"))

    def respond(
        self,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        force_tool: str | None = None,
    ) -> Response:
        kwargs: dict[str, Any] = {}
        if force_tool:
            kwargs["tool_choice"] = {"type": "tool", "name": force_tool}
        r = self._client.messages.create(
            model=self.model,
            max_tokens=self._max_tokens,
            system=system,
            messages=cast(Any, messages),  # SDK TypedDicts; plain dicts are accepted
            tools=cast(Any, tools),
            **kwargs,
        )
        blocks: list[dict[str, Any]] = []
        for b in r.content:
            if b.type == "text":
                blocks.append({"type": "text", "text": b.text})
            elif b.type == "tool_use":
                blocks.append({"type": "tool_use", "id": b.id, "name": b.name, "input": b.input})
        return Response(blocks, r.stop_reason, r.usage.input_tokens, r.usage.output_tokens)


# --------------------------------------------------------------------- fixture
CAUSE_PRIORITY = [
    ("breaking_schema_change", "schema_change"),
    ("connector_failed", "streaming"),
    ("consumer_lag", "streaming"),
    ("contract_quarantine", "data_quality"),
    ("duplicate_business_keys", "data_quality"),
    ("orphan_foreign_keys", "data_quality"),
    ("failed_critical_task", "pipeline_execution"),
    ("stale_dataset", "source_freshness"),
    ("quality_check_failed", "data_quality"),
    ("reconciliation_failed", "reconciliation"),
    ("downstream_model_skipped", "pipeline_execution"),
]
RECOVERY = {
    "breaking_schema_change": ["escalate_to_owner", "bounded_backfill", "rebuild_models"],
    "connector_failed": ["escalate_to_owner"],
    "consumer_lag": ["escalate_to_owner"],
    "contract_quarantine": ["reprocess_quarantine"],
    "duplicate_business_keys": ["rebuild_models"],
    "orphan_foreign_keys": ["rebuild_models"],
    "failed_critical_task": ["retry_failed_batch"],
    "stale_dataset": ["bounded_backfill"],
    "quality_check_failed": ["rebuild_models"],
    "reconciliation_failed": ["rebuild_models"],
    "downstream_model_skipped": ["rebuild_models"],
}


def _tool_use(i: int, name: str, args: dict[str, Any]) -> dict[str, Any]:
    return {"type": "tool_use", "id": f"fx_{i}", "name": name, "input": args}


@dataclass
class FixtureProvider:
    """Deterministic scripted investigator (NOT AI): fixed tool plan + rule mapping."""

    mode: str = "fixture"
    name: str = "fixture:rule-based-v1"
    _step: int = 0
    _results: dict[str, Any] = field(default_factory=dict)
    _plan: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    _failed: list[str] = field(default_factory=list)

    def _collect(self, messages: list[dict[str, Any]]) -> None:
        names = {
            b["id"]: b["name"]
            for m in messages
            if m["role"] == "assistant"
            for b in m["content"]
            if isinstance(b, dict) and b.get("type") == "tool_use"
        }
        for m in messages:
            if m["role"] == "user" and isinstance(m["content"], list):
                for b in m["content"]:
                    if b.get("type") == "tool_result" and b.get("is_error"):
                        name = names.get(b["tool_use_id"])
                        if name and name not in self._failed:
                            self._failed.append(name)
                    elif b.get("type") == "tool_result":
                        key = f"{names.get(b['tool_use_id'])}#{b['tool_use_id']}"
                        with contextlib.suppress(TypeError, json.JSONDecodeError):
                            self._results[key] = json.loads(b["content"])

    def _get(self, prefix: str) -> list[Any]:
        return [v for k, v in sorted(self._results.items()) if k.startswith(prefix + "#")]

    def respond(
        self,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        force_tool: str | None = None,
    ) -> Response:
        self._collect(messages)
        self._step += 1
        if self._step == 1:
            incident_id = json.loads(messages[0]["content"])["incident_id"]
            return Response(
                [_tool_use(1, "get_incident", {"incident_id": incident_id})], "tool_use"
            )
        inc = (self._get("get_incident") or [{}])[0]
        if self._step == 2:
            if not inc.get("found"):
                return self._submit(inc)
            root = inc["incident"].get("root_dataset_id")
            rules = sorted({s["rule_id"] for s in inc["signals"]})
            plan: list[tuple[str, dict[str, Any]]] = []
            if root and root.startswith("commerce_source."):
                plan.append(("get_schema_diff", {"dataset_id": root}))
            plan += [
                ("get_failed_run", {}),
                ("get_quality_results", {}),
                ("get_reconciliation_results", {}),
            ]
            if "stale_dataset" in rules:
                plan.append(("get_dataset_freshness", {}))
            if {"consumer_lag", "connector_failed"} & set(rules):
                plan.append(("get_stream_health", {}))
            if root:
                plan.append(("get_lineage_impact", {"dataset_id": root}))
            plan.append(
                ("search_runbooks", {"query": " ".join(r.replace("_", " ") for r in rules)})
            )
            self._plan = plan
        if self._plan and not force_tool:
            calls = [
                _tool_use(10 + i + self._step * 20, n, a) for i, (n, a) in enumerate(self._plan)
            ]
            self._plan = []
            return Response(calls, "tool_use")
        return self._submit(inc)

    def _submit(self, inc: dict[str, Any]) -> Response:
        signals = inc.get("signals", []) if inc.get("found") else []
        rules = {s["rule_id"] for s in signals}
        facts = [
            {
                "statement": (
                    f"Signal {s['rule_id']} on {s['dataset_id'] or 'n/a'} ({s['severity']})."
                ),
                "evidence_ids": [s["evidence_id"], s["source_evidence_id"]],
            }
            for s in signals[:12]
        ]
        primary = next(((r, c) for r, c in CAUSE_PRIORITY if r in rules), None)
        lone_stale = rules == {"stale_dataset"} and len(signals) == 1
        refs = [i for f in facts for i in f["evidence_ids"]]
        hits = [h for r in self._get("search_runbooks") for h in r.get("hits", [])][:2]
        refs += [h["evidence_id"] for h in hits]
        if not primary or lone_stale:
            out = {
                "abstain": True,
                "abstain_reason": "Signals do not distinguish a cause"
                " (fixture rule: a single stale dataset or no signals).",
                "summary": "Fixture run: insufficient evidence to name a cause.",
                "observed_facts": facts,
                "likely_causes": [],
                "recommended_checks": [
                    "Compare the source high-water mark with the last load of the table."
                ],
                "recovery_options": [],
                "missing_evidence": ["source-side change activity"],
                "evidence_references": refs,
            }
        else:
            rule, cat = primary
            sig = [s for s in signals if s["rule_id"] == rule]
            ds = inc["incident"].get("root_dataset_id") or sig[0]["dataset_id"]
            supp = [s["source_evidence_id"] for s in sig][:6]
            schema_ok = None
            for r in self._get("get_schema_diff"):
                obs = r.get("observations") or []
                if obs:
                    schema_ok = obs[0]["compatibility"] != "breaking"
            options = []
            for action in RECOVERY[rule]:
                pre: list[dict[str, Any]] = []
                if (
                    action in ("reprocess_quarantine", "bounded_backfill", "rebuild_models")
                    and schema_ok is not None
                ):
                    pre.append(
                        {
                            "description": "source schema matches the contract again",
                            "status": "met" if schema_ok else "unmet",
                            "evidence_ids": [],
                        }
                    )
                options.append(
                    {
                        "action": action,
                        "scope": ds or "see affected datasets",
                        "rationale": f"fixture mapping for {rule}",
                        "preconditions": pre,
                        "evidence_ids": supp[:2],
                    }
                )
            out = {
                "abstain": False,
                "abstain_reason": None,
                "summary": f"Fixture run: rule-based mapping of signal {rule} on {ds}.",
                "observed_facts": facts,
                "likely_causes": [
                    {
                        "cause": f"{rule.replace('_', ' ')} at {ds}",
                        "category": cat,
                        "dataset_id": ds,
                        "confidence": "medium",
                        "confidence_rationale": "fixed rule priority, not reasoning",
                        "supporting_evidence": supp,
                        "conflicting_evidence": [],
                    }
                ],
                "affected": [],
                "recommended_checks": [],
                "recovery_options": options,
                "missing_evidence": [],
                "evidence_references": refs,
            }
        out["missing_evidence"] = list(out["missing_evidence"]) + [  # type: ignore[call-overload]
            f"tool {n} failed" for n in self._failed
        ]
        return Response([_tool_use(999, "submit_investigation", out)], "tool_use")


def get_provider(mode: str) -> Provider:
    if mode == "fixture":
        return FixtureProvider()
    if mode == "model":
        return AnthropicProvider()
    raise ValueError(f"unknown investigator mode {mode!r} (model | fixture)")
