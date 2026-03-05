from __future__ import annotations

import json

from app.services.rag.chunker import build_chunks_from_factpack
from app.services.rag.embedder import embed_many
from app.services.rag.pgvector_store import PGVectorStore


def build_chunks_and_index(process_date: str, factpack: dict) -> str:
    chunks = build_chunks_from_factpack(process_date, factpack)
    texts = [c["text"] for c in chunks]
    embeddings = embed_many(texts) if texts else []

    rows = []
    for c, emb in zip(chunks, embeddings):
        row = dict(c)
        row["embedding"] = "[" + ",".join(f"{x:.6f}" for x in emb) + "]"
        rows.append(row)

    store = PGVectorStore()
    if store.enabled():
        store.init()
        store.upsert_chunks(rows)

    return "\n".join(json.dumps(c, ensure_ascii=True, separators=(",", ":")) for c in chunks) + ("\n" if chunks else "")
