import hashlib
import json
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.config import get_settings


@contextmanager
def get_conn():
    cfg = get_settings()
    conn = psycopg.connect(
        host=cfg.pg_host,
        port=cfg.pg_port,
        user=cfg.pg_user,
        password=cfg.pg_password,
        dbname=cfg.pg_db,
        row_factory=dict_row,
    )
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def initialize_db() -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS ingest_raw (
        id TEXT PRIMARY KEY,
        run_date DATE NOT NULL,
        source TEXT NOT NULL,
        payload JSONB NOT NULL,
        payload_hash TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS normalized_events (
        event_id TEXT PRIMARY KEY,
        run_date DATE NOT NULL,
        league TEXT NOT NULL,
        event_type TEXT NOT NULL,
        title TEXT NOT NULL,
        summary TEXT NOT NULL,
        entities JSONB NOT NULL,
        metrics JSONB NOT NULL,
        citations JSONB NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS ranked_events (
        run_date DATE NOT NULL,
        event_id TEXT NOT NULL,
        score DOUBLE PRECISION NOT NULL,
        rationale TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (run_date, event_id)
    );

    CREATE TABLE IF NOT EXISTS pipeline_runs (
        run_date DATE PRIMARY KEY,
        status TEXT NOT NULL,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def upsert_ingest_row(row_id: str, run_date: str, source: str, payload: dict[str, Any]) -> None:
    payload_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    sql = """
    INSERT INTO ingest_raw (id, run_date, source, payload, payload_hash)
    VALUES (%s, %s::date, %s, %s::jsonb, %s)
    ON CONFLICT (id)
    DO UPDATE SET
      payload = EXCLUDED.payload,
      payload_hash = EXCLUDED.payload_hash,
      created_at = now();
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (row_id, run_date, source, json.dumps(payload), payload_hash))


def fetch_ingest_rows(run_date: str) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, source, payload FROM ingest_raw WHERE run_date = %s::date ORDER BY source, id",
                (run_date,),
            )
            return list(cur.fetchall())


def upsert_normalized_event(event: dict[str, Any]) -> None:
    sql = """
    INSERT INTO normalized_events
    (event_id, run_date, league, event_type, title, summary, entities, metrics, citations)
    VALUES (%(event_id)s, %(run_date)s::date, %(league)s, %(event_type)s, %(title)s,
            %(summary)s, %(entities)s::jsonb, %(metrics)s::jsonb, %(citations)s::jsonb)
    ON CONFLICT (event_id)
    DO UPDATE SET
      run_date = EXCLUDED.run_date,
      league = EXCLUDED.league,
      event_type = EXCLUDED.event_type,
      title = EXCLUDED.title,
      summary = EXCLUDED.summary,
      entities = EXCLUDED.entities,
      metrics = EXCLUDED.metrics,
      citations = EXCLUDED.citations,
      updated_at = now();
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                {
                    **event,
                    "entities": json.dumps(event["entities"]),
                    "metrics": json.dumps(event["metrics"]),
                    "citations": json.dumps(event["citations"]),
                },
            )


def fetch_normalized_events(run_date: str) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, league, event_type, title, summary, entities, metrics, citations
                FROM normalized_events
                WHERE run_date = %s::date
                """,
                (run_date,),
            )
            return list(cur.fetchall())


def delete_normalized_events(run_date: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM normalized_events WHERE run_date = %s::date", (run_date,))


def upsert_ranked_event(run_date: str, event_id: str, score: float, rationale: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ranked_events (run_date, event_id, score, rationale)
                VALUES (%s::date, %s, %s, %s)
                ON CONFLICT (run_date, event_id)
                DO UPDATE SET score = EXCLUDED.score, rationale = EXCLUDED.rationale, created_at = now();
                """,
                (run_date, event_id, score, rationale),
            )


def fetch_ranked_events(run_date: str) -> list[dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT re.event_id, re.score, re.rationale, ne.title, ne.summary, ne.league, ne.event_type, ne.metrics, ne.citations
                FROM ranked_events re
                JOIN normalized_events ne ON ne.event_id = re.event_id
                WHERE re.run_date = %s::date
                ORDER BY re.score DESC
                """,
                (run_date,),
            )
            return list(cur.fetchall())


def delete_ranked_events(run_date: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ranked_events WHERE run_date = %s::date", (run_date,))


def set_pipeline_status(run_date: str, status: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (run_date, status)
                VALUES (%s::date, %s)
                ON CONFLICT (run_date)
                DO UPDATE SET status = EXCLUDED.status, updated_at = now();
                """,
                (run_date, status),
            )
