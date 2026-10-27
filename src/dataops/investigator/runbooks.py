"""Runbook ingestion, section-aware chunking, incremental indexing and hybrid search.

Indexing is incremental and fingerprinted:
- a document whose file fingerprint and embedding model are unchanged is skipped;
- a changed document has its chunks replaced in one transaction (update propagation);
- a document whose file disappeared is deleted with its chunks (delete propagation).
Only files under `runbooks/` are indexed; evaluation ground truth lives elsewhere and is
never ingested.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from dataops.config import Role
from dataops.db import connect
from dataops.investigator.embeddings import Embedder, get_embedder, to_pgvector, tokens

REPO_ROOT = Path(__file__).resolve().parents[3]
RUNBOOK_DIR = REPO_ROOT / "runbooks"
MAX_CHUNK_CHARS = 1200
REQUIRED_META = ("runbook_id", "title", "service", "incident_types", "version", "effective_date")


@dataclass
class Chunk:
    chunk_id: str
    section: str
    ordinal: int
    content: str

    @property
    def sha(self) -> str:
        return hashlib.sha256(f"{self.section}\n{self.content}".encode()).hexdigest()


@dataclass
class Document:
    doc_id: str
    path: str
    meta: dict[str, Any]
    sha: str
    chunks: list[Chunk] = field(default_factory=list)


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:40] or "section"


def parse(path: Path, root: Path = REPO_ROOT) -> Document:
    raw = path.read_text(encoding="utf-8")
    m = re.match(r"^---\n(.*?)\n---\n(.*)$", raw, re.S)
    if not m:
        raise ValueError(f"{path}: missing front matter")
    meta = yaml.safe_load(m.group(1)) or {}
    missing = [k for k in REQUIRED_META if k not in meta]
    if missing:
        raise ValueError(f"{path}: front matter lacks {missing}")
    doc_id = str(meta["runbook_id"])
    if not re.fullmatch(r"RB-\d{3}", doc_id):
        raise ValueError(f"{path}: runbook_id must look like RB-001")
    body = m.group(2)
    sections: list[tuple[str, str]] = []
    current = str(meta["title"])
    buf: list[str] = []
    for line in body.splitlines():
        if line.startswith("## "):
            sections.append((current, "\n".join(buf).strip()))
            current, buf = line[3:].strip(), []
        elif line.startswith("# "):
            continue  # document title; already in metadata
        else:
            buf.append(line)
    sections.append((current, "\n".join(buf).strip()))
    doc = Document(
        doc_id, str(path.relative_to(root)), meta, hashlib.sha256(raw.encode()).hexdigest()
    )
    ordinal = 0
    for name, text in sections:
        if not text:
            continue
        parts, cur = [], ""
        for para in re.split(r"\n\s*\n", text):
            if cur and len(cur) + len(para) + 2 > MAX_CHUNK_CHARS:
                parts.append(cur)
                cur = para
            else:
                cur = f"{cur}\n\n{para}" if cur else para
        parts.append(cur)
        for i, part in enumerate(parts):
            suffix = f"-{i + 1}" if len(parts) > 1 else ""
            doc.chunks.append(Chunk(f"{doc_id}#{_slug(name)}{suffix}", name, ordinal, part))
            ordinal += 1
    return doc


def discover(directory: Path = RUNBOOK_DIR) -> list[Document]:
    docs = [parse(p, directory.parent) for p in sorted(directory.glob("*.md"))]
    ids = [d.doc_id for d in docs]
    dupes = {i for i in ids if ids.count(i) > 1}
    if dupes:
        raise ValueError(f"duplicate runbook ids: {sorted(dupes)}")
    return docs


def index(directory: Path = RUNBOOK_DIR, embedder: Embedder | None = None) -> dict[str, Any]:
    emb = embedder or get_embedder()
    docs = discover(directory)
    stats: dict[str, Any] = {
        "embedding_model": emb.name,
        "added": [],
        "updated": [],
        "unchanged": [],
        "deleted": [],
        "chunks_written": 0,
    }
    with connect(Role.REL_APP, application_name="dataops-runbooks") as conn, conn.transaction():
        existing = {
            r[0]: (r[1], r[2])
            for r in conn.execute(
                "select doc_id, content_sha256, embedding_model from reliability.runbook_documents"
            ).fetchall()
        }
        for doc in docs:
            prev = existing.get(doc.doc_id)
            if prev == (doc.sha, emb.name):
                stats["unchanged"].append(doc.doc_id)
                continue
            vectors = emb.embed(
                [f"{doc.meta['title']}. {c.section}. {c.content}" for c in doc.chunks]
            )
            conn.execute("delete from reliability.runbook_documents where doc_id=%s", (doc.doc_id,))
            eff = doc.meta["effective_date"]
            conn.execute(
                "insert into reliability.runbook_documents (doc_id, path, title, service,"
                " incident_types, version, effective_date, owner, content_sha256, embedding_model,"
                " chunk_count) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    doc.doc_id,
                    doc.path,
                    doc.meta["title"],
                    doc.meta["service"],
                    [str(t) for t in doc.meta["incident_types"]],
                    str(doc.meta["version"]),
                    eff if isinstance(eff, date) else date.fromisoformat(str(eff)),
                    doc.meta.get("owner"),
                    doc.sha,
                    emb.name,
                    len(doc.chunks),
                ),
            )
            for c, v in zip(doc.chunks, vectors, strict=True):
                conn.execute(
                    "insert into reliability.runbook_chunks (chunk_id, doc_id, doc_version,"
                    " section, ordinal, content, content_sha256, metadata, embedding_model,"
                    " embedding) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::vector)",
                    (
                        c.chunk_id,
                        doc.doc_id,
                        str(doc.meta["version"]),
                        c.section,
                        c.ordinal,
                        c.content,
                        c.sha,
                        json.dumps(
                            {
                                "title": doc.meta["title"],
                                "service": doc.meta["service"],
                                "incident_types": doc.meta["incident_types"],
                                "effective_date": str(eff),
                                "path": doc.path,
                            }
                        ),
                        emb.name,
                        to_pgvector(v),
                    ),
                )
            stats["chunks_written"] += len(doc.chunks)
            stats["updated" if prev else "added"].append(doc.doc_id)
        present = {d.doc_id for d in docs}
        for doc_id in sorted(set(existing) - present):
            conn.execute("delete from reliability.runbook_documents where doc_id=%s", (doc_id,))
            stats["deleted"].append(doc_id)
    return stats


def citation_id(chunk_id: str, version: str) -> str:
    return f"doc:{chunk_id}@{version}"


def search(
    query: str, k: int = 4, incident_type: str | None = None, embedder: Embedder | None = None
) -> list[dict[str, Any]]:
    """Hybrid retrieval: pgvector cosine ranking + full-text ranking, fused with reciprocal
    rank fusion (k=60). Only chunks embedded with the active model are considered."""
    emb = embedder or get_embedder()
    k = max(1, min(int(k), 8))
    qv = to_pgvector(emb.embed([query])[0])
    toks = sorted(set(tokens(query)))[:32]
    tsq = " | ".join(toks)  # tokens are [a-z0-9_]+ only, so this is a safe tsquery
    filt = " and (%s::text is null or d.incident_types && array[%s::text, 'all'])"
    with connect(
        Role.REL_READER, application_name="dataops-runbooks", statement_timeout_s=10
    ) as conn:
        vec = conn.execute(
            "select c.chunk_id from reliability.runbook_chunks c"  # noqa: S608 - fixed text
            " join reliability.runbook_documents d using (doc_id) where c.embedding_model=%s"
            + filt
            + " order by c.embedding <=> %s::vector limit 20",
            (emb.name, incident_type, incident_type, qv),
        ).fetchall()
        lex = conn.execute(
            "select c.chunk_id from reliability.runbook_chunks c"  # noqa: S608 - fixed text
            " join reliability.runbook_documents d using (doc_id), to_tsquery('english', %s) q"
            " where c.tsv @@ q and c.embedding_model=%s"
            + filt
            + " order by ts_rank_cd(c.tsv, q) desc limit 20",
            (tsq or "x_no_terms_x", emb.name, incident_type, incident_type),
        ).fetchall()
        scores: dict[str, float] = {}
        for ranking in (vec, lex):
            for rank, (cid,) in enumerate(ranking):
                scores[cid] = scores.get(cid, 0.0) + 1.0 / (60 + rank + 1)
        top = sorted(scores, key=lambda c: (-scores[c], c))[:k]
        if not top:
            return []
        rows = conn.execute(
            "select c.chunk_id, c.doc_id, c.doc_version, c.section, c.content, d.title, d.path,"
            " d.effective_date, d.incident_types from reliability.runbook_chunks c join"
            " reliability.runbook_documents d using (doc_id) where c.chunk_id = any(%s)",
            (top,),
        ).fetchall()
    by_id = {r[0]: r for r in rows}
    vec_ids = [r[0] for r in vec]
    lex_ids = [r[0] for r in lex]
    out = []
    for cid in top:
        r = by_id[cid]
        out.append(
            {
                "evidence_id": citation_id(cid, r[2]),
                "runbook_id": r[1],
                "title": r[5],
                "section": r[3],
                "version": r[2],
                "effective_date": str(r[7]),
                "path": r[6],
                "incident_types": list(r[8]),
                "score_rrf": round(scores[cid], 5),
                "vector_rank": vec_ids.index(cid) + 1 if cid in vec_ids else None,
                "lexical_rank": lex_ids.index(cid) + 1 if cid in lex_ids else None,
                "content": r[4],
            }
        )
    return out
