from __future__ import annotations

import json

from prefect import flow, task

from app.clients.lakefs import LakeFSClient
from app.services.lakefs_paths import factpack_json, rag_chunks_jsonl
from app.services.rag.indexer import build_chunks_and_index


@task(retries=2, retry_delay_seconds=20, log_prints=True)
def rag_index_task(run_date: str, lakefs_branch: str) -> dict:
    lakefs = LakeFSClient()
    if not lakefs.enabled:
        return {"run_date": run_date, "status": "skipped", "reason": "lakefs_disabled"}

    factpack = json.loads(lakefs.download_object(branch=lakefs_branch, path=factpack_json(run_date)).decode("utf-8"))
    chunks_jsonl = build_chunks_and_index(run_date, factpack)
    lakefs.upload_object(
        branch=lakefs_branch,
        path=rag_chunks_jsonl(run_date),
        content=chunks_jsonl.encode("utf-8"),
        content_type="application/jsonl",
    )
    return {
        "run_date": run_date,
        "status": "indexed",
        "chunk_count": sum(1 for ln in chunks_jsonl.splitlines() if ln.strip()),
        "chunks_key": rag_chunks_jsonl(run_date),
    }


@flow(name="slw-dailycast-rag-index")
def rag_index_flow(run_date: str, lakefs_branch: str) -> dict:
    return rag_index_task(run_date, lakefs_branch)
