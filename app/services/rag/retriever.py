from __future__ import annotations

from app.config import get_settings
from app.services.rag.embedder import embed_text
from app.services.rag.pgvector_store import PGVectorStore


def retrieve(process_date: str, query: str, top_k: int | None = None) -> list[dict]:
    cfg = get_settings()
    top_k = int(top_k or cfg.rag_top_k)
    store = PGVectorStore()
    if not store.enabled():
        return []
    emb = embed_text(query)
    rows = store.query(emb, process_date=process_date, top_k=top_k)
    min_score = float(cfg.rag_min_score)
    return [r for r in rows if float(r.get("score", 0.0)) >= min_score]


def retrieve_for_plan(process_date: str) -> dict[str, list[dict]]:
    return {
        "top_storylines": retrieve(process_date, "biggest storylines, injuries, trades, combine, award movement"),
        "top_games": retrieve(process_date, "closest games, upsets, playoff implications"),
        "today_watchlist": retrieve(process_date, "must watch matchups, seeding, standings pressure"),
        "team_trends": retrieve(process_date, "last 10 games, streaks, scoring trend"),
    }
