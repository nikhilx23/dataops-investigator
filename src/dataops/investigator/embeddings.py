"""Embedding providers.

`DATAOPS_EMBEDDING_PROVIDER` selects one explicitly; nothing falls back silently.

- `hashing` (default): signed feature hashing of word unigrams and bigrams into 384
  dimensions, L2-normalised. Deterministic and offline. It is a *lexical* representation, not
  a neural embedding: it matches shared words, not paraphrases. Its name is stored with every
  chunk so results are never attributed to a semantic model.
- `sentence-transformers`: a local neural model (default `sentence-transformers/all-MiniLM-L6-v2`,
  384 dimensions). Needs the optional package and a one-time model download from Hugging Face.
"""

from __future__ import annotations

import hashlib
import math
import os
import re
from typing import Protocol

DIM = 384
_TOKEN = re.compile(r"[a-z0-9_]+")
_STOP = frozenset(
    [
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "by",
        "for",
        "from",
        "has",
        "have",
        "in",
        "is",
        "it",
        "of",
        "on",
        "or",
        "that",
        "the",
        "this",
        "to",
        "was",
        "were",
        "will",
        "with",
        "not",
        "no",
        "do",
        "does",
        "when",
        "then",
        "than",
    ]
)


class Embedder(Protocol):
    name: str
    neural: bool

    def embed(self, texts: list[str]) -> list[list[float]]: ...


def tokens(text: str) -> list[str]:
    return [t for t in _TOKEN.findall(text.lower()) if t not in _STOP]


class HashingEmbedder:
    name = "hashing-v1-384"
    neural = False

    def _vec(self, text: str) -> list[float]:
        v = [0.0] * DIM
        toks = tokens(text)
        feats = toks + [f"{a} {b}" for a, b in zip(toks, toks[1:], strict=False)]
        for f in feats:
            h = hashlib.blake2b(f.encode(), digest_size=8).digest()
            idx = int.from_bytes(h[:4], "little") % DIM
            sign = 1.0 if h[4] & 1 else -1.0
            v[idx] += sign * (1.0 if " " not in f else 0.5)
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]

    def embed(self, texts: list[str]) -> list[list[float]]:
        return [self._vec(t) for t in texts]


class SentenceTransformerEmbedder:
    neural = True

    def __init__(self, model: str) -> None:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:  # explicit: never fall back to another provider
            raise RuntimeError(
                "DATAOPS_EMBEDDING_PROVIDER=sentence-transformers needs the optional package:"
                " pip install sentence-transformers"
            ) from exc
        self._model = SentenceTransformer(model)
        if self._model.get_sentence_embedding_dimension() != DIM:
            raise RuntimeError(f"{model} does not produce {DIM}-dimensional vectors")
        self.name = f"st:{model}"

    def embed(self, texts: list[str]) -> list[list[float]]:
        out = self._model.encode(texts, normalize_embeddings=True)
        return [list(map(float, row)) for row in out]


def get_embedder() -> Embedder:
    provider = os.environ.get("DATAOPS_EMBEDDING_PROVIDER", "hashing")
    if provider == "hashing":
        return HashingEmbedder()
    if provider == "sentence-transformers":
        return SentenceTransformerEmbedder(
            os.environ.get("DATAOPS_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
        )
    raise ValueError(f"unknown DATAOPS_EMBEDDING_PROVIDER: {provider}")


def to_pgvector(v: list[float]) -> str:
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"
