from __future__ import annotations

import json
from typing import Any

import psycopg

from app.config import get_settings


DDL = """
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS slw_chunks (
  id TEXT PRIMARY KEY,
  process_date TEXT NOT NULL,
  sport TEXT,
  doc TEXT NOT NULL,
  chunk_index INT NOT NULL,
  text TEXT NOT NULL,
  metadata JSONB NOT NULL,
  embedding vector(64)
);

CREATE INDEX IF NOT EXISTS slw_chunks_date_idx ON slw_chunks(process_date);
CREATE INDEX IF NOT EXISTS slw_chunks_vec_idx ON slw_chunks USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
"""


class PGVectorStore:
    def __init__(self) -> None:
        self.conninfo = get_settings().pgvector_conninfo

    def enabled(self) -> bool:
        return bool(self.conninfo)

    def init(self) -> None:
        if not self.conninfo:
            return
        with psycopg.connect(self.conninfo) as conn:
            conn.execute(DDL)
            conn.commit()

    def upsert_chunks(self, rows: list[dict[str, Any]]) -> None:
        if not self.conninfo or not rows:
            return
        sql = """
        INSERT INTO slw_chunks (id, process_date, sport, doc, chunk_index, text, metadata, embedding)
        VALUES (%(id)s, %(process_date)s, %(sport)s, %(doc)s, %(chunk_index)s, %(text)s, %(metadata)s::jsonb, %(embedding)s)
        ON CONFLICT (id) DO UPDATE SET
          text = EXCLUDED.text,
          metadata = EXCLUDED.metadata,
          embedding = EXCLUDED.embedding
        """
        with psycopg.connect(self.conninfo) as conn:
            for r in rows:
                payload = dict(r)
                payload["metadata"] = json.dumps(payload.get("metadata") or {}, separators=(",", ":"))
                conn.execute(sql, payload)
            conn.commit()

    def query(self, embedding: list[float], process_date: str, top_k: int) -> list[dict[str, Any]]:
        if not self.conninfo:
            return []
        sql = """
        SELECT id, text, metadata, 1 - (embedding <=> %(emb)s::vector) AS score
        FROM slw_chunks
        WHERE process_date = %(process_date)s
        ORDER BY embedding <=> %(emb)s::vector
        LIMIT %(top_k)s
        """
        emb_literal = "[" + ",".join(f"{x:.6f}" for x in embedding) + "]"
        with psycopg.connect(self.conninfo) as conn:
            cur = conn.execute(sql, {"emb": emb_literal, "process_date": process_date, "top_k": int(top_k)})
            out: list[dict[str, Any]] = []
            for row in cur.fetchall():
                out.append({"id": row[0], "text": row[1], "metadata": row[2], "score": float(row[3])})
            return out
