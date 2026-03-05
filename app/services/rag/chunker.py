from __future__ import annotations

from typing import Any


def _mk_chunk(
    *,
    chunk_id: str,
    process_date: str,
    sport: str,
    doc: str,
    chunk_index: int,
    text: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": chunk_id,
        "process_date": process_date,
        "sport": sport,
        "doc": doc,
        "chunk_index": chunk_index,
        "text": text,
        "metadata": metadata,
    }


def build_chunks_from_factpack(process_date: str, factpack: dict[str, Any]) -> list[dict[str, Any]]:
    chunks: list[dict[str, Any]] = []

    idx = 0
    for row in factpack.get("yesterday_results", []) or []:
        sport = str(row.get("sport", "unknown"))
        title = str(row.get("title", ""))
        summary = str(row.get("summary", ""))
        why = " | ".join(str(x) for x in (row.get("why_it_matters") or []) if str(x).strip())
        impact = row.get("impact_signals") or {}
        impact_text = (
            f"margin={impact.get('margin')} total_points={impact.get('total_points')} "
            f"phase={impact.get('phase')} priority={impact.get('priority_tier')}"
        )
        key_players = "; ".join(str(x.get("line", "")) for x in (row.get("key_players") or []) if isinstance(x, dict))
        text = " ".join([title, summary, impact_text, why, key_players]).strip()
        if not text:
            continue
        chunks.append(
            _mk_chunk(
                chunk_id=f"{process_date}:results:{idx}",
                process_date=process_date,
                sport=sport,
                doc="yesterday_results",
                chunk_index=idx,
                text=text,
                metadata={"source_id": row.get("source_id"), "citations": row.get("citations", [])},
            )
        )
        idx += 1

    idx = 0
    for row in factpack.get("major_news", []) or []:
        sport = str(row.get("sport", "unknown"))
        title = str(row.get("title", ""))
        summary = str(row.get("summary", ""))
        text = " ".join([title, summary]).strip()
        if not text:
            continue
        chunks.append(
            _mk_chunk(
                chunk_id=f"{process_date}:news:{idx}",
                process_date=process_date,
                sport=sport,
                doc="major_news",
                chunk_index=idx,
                text=text,
                metadata={"source_id": row.get("source_id"), "citations": row.get("citations", [])},
            )
        )
        idx += 1

    idx = 0
    for row in factpack.get("today_matchups", []) or []:
        sport = str(row.get("sport", "unknown"))
        title = str(row.get("title", ""))
        summary = str(row.get("summary", ""))
        ctx = row.get("matchup_context") or {}
        ctx_text = (
            f"home_record={ctx.get('home_record')} away_record={ctx.get('away_record')} "
            f"home_last10={ctx.get('home_last10')} away_last10={ctx.get('away_last10')} "
            f"home_net={ctx.get('home_point_diff')} away_net={ctx.get('away_point_diff')}"
        )
        text = " ".join([title, summary, ctx_text]).strip()
        if not text:
            continue
        chunks.append(
            _mk_chunk(
                chunk_id=f"{process_date}:matchups:{idx}",
                process_date=process_date,
                sport=sport,
                doc="today_matchups",
                chunk_index=idx,
                text=text,
                metadata={"source_id": row.get("source_id"), "citations": row.get("citations", [])},
            )
        )
        idx += 1

    return chunks
