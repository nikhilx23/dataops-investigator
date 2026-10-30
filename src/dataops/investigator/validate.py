"""Post-hoc validation of a submitted investigation.

The model's raw output is kept for audit; the *validated* output is what is displayed:
- citations that were not returned by a tool in this investigation, or do not resolve to a
  stored record, are removed (and counted);
- facts with no valid citation, or with numbers that do not appear in the cited tool results
  (allowing unit conversions s->min->h, cents->dollars), are removed;
- causes without valid supporting evidence are removed; causes are always 'inferred';
- recovery options are already restricted to the allowlist by the schema; text proposing
  forbidden operations (drop/truncate/delete checkpoints/arbitrary SQL or shell) is removed;
- percentages presented as confidence are removed;
- with no supported cause left, the result becomes an abstention.
"""

from __future__ import annotations

import logging
import re
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from dataops.investigator.evidence import resolve
from dataops.investigator.schema import Investigation
from dataops.investigator.tools import normalise_number

log = logging.getLogger(__name__)

FORBIDDEN = re.compile(
    r"\b(drop\s+(table|schema|database)|truncate\b|delete\s+from|rm\s+-rf|"
    r"delete\s+(the\s+)?(checkpoint|replication\s+slot)|drop\s+(the\s+)?(replication\s+)?slot|"
    r"pg_drop_replication_slot|reset\s+--hard|alter\s+table|update\s+\w+\s+set|"
    r"disable\s+(the\s+)?(contract|validation|tests?))",
    re.IGNORECASE,
)
CONF_PCT = re.compile(
    r"\d{1,3}(\.\d+)?\s*%\s*(confiden|likel|probab|certain|sure)|"
    r"(confiden\w*|probability|likelihood)\D{0,15}\d{1,3}(\.\d+)?\s*%",
    re.I,
)
_SKIP = re.compile(
    r"(ev:\S+|doc:\S+|\d{4}-\d{2}-\d{2}(T[\d:.+Z-]+)?|\b[A-Za-z_]+\d[\w.-]*|\b\d+[A-Za-z_][\w.-]*"
    r"|v\d+(\.\d+)*|RB-\d+)"
)
_NUM = re.compile(r"(?<![\w.])-?\d[\d,]*(?:\.\d+)?(?![\w])")
TRIVIAL_MAX = 10  # small integers are counts of listed items


def numbers_claimed(text: str) -> list[str]:
    return [n for n in _NUM.findall(_SKIP.sub(" ", text)) if n.strip(",")]


def grounded(n: str, pool: set[str]) -> bool:
    s = normalise_number(n)
    try:
        d = Decimal(s)
    except Exception:  # noqa: BLE001
        return False
    if s in pool or (d == d.to_integral_value() and abs(d) <= TRIVIAL_MAX):
        return True
    places = -d.as_tuple().exponent if d.as_tuple().exponent < 0 else 0  # type: ignore[operator]
    q = Decimal(1).scaleb(-places)
    for p in pool:
        try:
            base = Decimal(p)
        except ArithmeticError:  # not a number (e.g. a stray '-')
            log.debug("skipping non-numeric token %r", p)
            continue
        for div in (60, 3600, 86400, 100, 1000):
            if (base / div).quantize(q, rounding=ROUND_HALF_UP) == d:
                return True
    return False


def validate(
    raw: Investigation, seen: dict[str, set[str]], conn: Any = None
) -> tuple[Investigation, dict[str, Any]]:
    """`seen` maps evidence id -> normalised numbers of the tool result that returned it."""
    cited: list[str] = list(raw.evidence_references)
    for f in raw.observed_facts:
        cited += f.evidence_ids
    for c in raw.likely_causes:
        cited += c.supporting_evidence + c.conflicting_evidence
    for a in raw.affected:
        cited += a.evidence_ids
    for o in raw.recovery_options:
        cited += o.evidence_ids + [i for p in o.preconditions for i in p.evidence_ids]
    cited = list(dict.fromkeys(cited))
    resolves = resolve(cited, conn)
    valid = {i for i in cited if i in seen and resolves.get(i)}
    report: dict[str, Any] = {
        "citations_total": len(cited),
        "citations_valid": len(valid),
        "invalid_citations": sorted(set(cited) - valid),
        "not_returned_by_tools": sorted(i for i in cited if i not in seen),
        "unresolvable": sorted(i for i in cited if not resolves.get(i)),
        "removed_facts": [],
        "ungrounded_numbers": [],
        "removed_causes": [],
        "forbidden_text": [],
        "confidence_percentages": [],
        "forced_abstention": False,
    }
    all_numbers = set().union(*seen.values()) if seen else set()

    def ok_ids(ids: list[str]) -> list[str]:
        return [i for i in ids if i in valid]

    def scrub(text: str, where: str) -> str | None:
        if FORBIDDEN.search(text):
            report["forbidden_text"].append({"where": where, "text": text[:200]})
            return None
        if CONF_PCT.search(text):
            report["confidence_percentages"].append({"where": where, "text": text[:200]})
            return CONF_PCT.sub("[removed: numeric confidence]", text)
        return text

    out = raw.model_copy(deep=True)
    facts = []
    for f in out.observed_facts:
        ids = ok_ids(f.evidence_ids)
        pool = set().union(*(seen[i] for i in ids)) if ids else set()
        bad = [n for n in numbers_claimed(f.statement) if not grounded(n, pool)]
        text = scrub(f.statement, "fact")
        if not ids or bad or text is None:
            report["removed_facts"].append(
                {
                    "statement": f.statement[:200],
                    "no_valid_citation": not ids,
                    "ungrounded_numbers": bad,
                }
            )
            report["ungrounded_numbers"] += bad
            continue
        facts.append(f.model_copy(update={"statement": text, "evidence_ids": ids}))
    out.observed_facts = facts

    causes = []
    for c in out.likely_causes:
        sup = ok_ids(c.supporting_evidence)
        text = scrub(c.cause, "cause")
        rat = scrub(c.confidence_rationale, "confidence_rationale")
        bad = [n for n in numbers_claimed(c.cause) if not grounded(n, all_numbers)]
        if not sup or text is None or bad:
            report["removed_causes"].append(
                {"cause": c.cause[:200], "no_valid_support": not sup, "ungrounded_numbers": bad}
            )
            report["ungrounded_numbers"] += bad
            continue
        notes = []
        for note in c.conflict_notes:
            bad_note = [n for n in numbers_claimed(note) if not grounded(n, all_numbers)]
            clean = scrub(note, "conflict_note")
            if clean is None or bad_note:
                report["ungrounded_numbers"] += bad_note
                continue
            notes.append(clean)
        causes.append(
            c.model_copy(
                update={
                    "cause": text,
                    "supporting_evidence": sup,
                    "confidence_rationale": rat or "",
                    "conflicting_evidence": ok_ids(c.conflicting_evidence),
                    "conflict_notes": notes,
                }
            )
        )
    out.likely_causes = causes

    out.affected = [
        a.model_copy(update={"evidence_ids": ok_ids(a.evidence_ids)}) for a in out.affected
    ]
    opts = []
    for o in out.recovery_options:
        if scrub(f"{o.scope} {o.rationale}", f"recovery:{o.action}") is None:
            continue
        pre = [
            p.model_copy(update={"evidence_ids": ok_ids(p.evidence_ids)}) for p in o.preconditions
        ]
        opts.append(
            o.model_copy(update={"evidence_ids": ok_ids(o.evidence_ids), "preconditions": pre})
        )
    out.recovery_options = opts
    out.recommended_checks = [t for t in (scrub(c, "check") for c in out.recommended_checks) if t]
    out.evidence_references = sorted(i for i in cited if i in valid)

    summ = scrub(out.summary, "summary")
    bad_summary = [n for n in numbers_claimed(out.summary) if not grounded(n, all_numbers)]
    if summ is None or bad_summary:
        report["ungrounded_numbers"] += bad_summary
        out.summary = (
            "[summary withheld by validation: it contained "
            + ("a forbidden operation" if summ is None else "numbers not found in evidence")
            + "; see the validated facts]"
        )
    else:
        out.summary = summ

    if not out.abstain and not out.likely_causes:
        out.abstain = True
        out.abstain_reason = "No cause remained supported by valid evidence after validation."
        report["forced_abstention"] = True
    if out.abstain:
        out.likely_causes = []
        out.recovery_options = [o for o in out.recovery_options if o.action == "escalate_to_owner"]
    return out, report
