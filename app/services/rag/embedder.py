from __future__ import annotations

import hashlib
from typing import Any


def _hash_to_unit_floats(seed: str, dims: int = 64) -> list[float]:
    out: list[float] = []
    payload = seed.encode("utf-8")
    while len(out) < dims:
        digest = hashlib.sha256(payload).digest()
        for i in range(0, len(digest), 4):
            if len(out) >= dims:
                break
            val = int.from_bytes(digest[i : i + 4], byteorder="big", signed=False)
            out.append((val % 1000000) / 1000000.0)
        payload = digest
    return out


def embed_text(text: str) -> list[float]:
    # Deterministic lightweight embedding fallback.
    return _hash_to_unit_floats(text, dims=64)


def embed_many(texts: list[str]) -> list[list[float]]:
    return [embed_text(t) for t in texts]
