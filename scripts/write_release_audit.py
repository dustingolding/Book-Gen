#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from typing import Any

from app.storage import ObjectStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write/update BookGen release audit metadata in object storage.")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--run-date", required=True)
    parser.add_argument("--bookspec-key", required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--status", default="succeeded")
    parser.add_argument("--image", default="")
    parser.add_argument("--resource-profile", default="")
    parser.add_argument("--llm-chapter-limit", type=int, default=0)
    parser.add_argument("--wait-timeout-seconds", type=int, default=0)
    parser.add_argument("--pod-name", default="")
    parser.add_argument("--node-name", default="")
    parser.add_argument("--promoted-ref", default="")
    parser.add_argument("--promoted-dest-branch", default="")
    parser.add_argument("--started-at", default="")
    parser.add_argument("--finished-at", default="")
    parser.add_argument("--history-limit", type=int, default=25)
    return parser.parse_args()


def _load_existing(store: ObjectStore, key: str) -> dict[str, Any]:
    if not store.exists(key):
        return {}
    payload = store.get_json(key)
    return payload if isinstance(payload, dict) else {}


def main() -> int:
    args = parse_args()
    store = ObjectStore()
    key = f"runs/{args.project_id}/meta/release_audit.json"

    existing = _load_existing(store, key)
    history = existing.get("history")
    if not isinstance(history, list):
        history = []

    entry = {
        "event_at": _utc_now(),
        "project_id": args.project_id,
        "run_date": args.run_date,
        "bookspec_key": args.bookspec_key,
        "status": args.status,
        "job": {
            "name": args.job_name,
            "namespace": args.namespace,
            "pod_name": args.pod_name,
            "node_name": args.node_name,
            "image": args.image,
        },
        "runtime": {
            "resource_profile": args.resource_profile,
            "llm_chapter_limit": args.llm_chapter_limit,
            "wait_timeout_seconds": args.wait_timeout_seconds,
            "started_at": args.started_at or _utc_now(),
            "finished_at": args.finished_at or _utc_now(),
        },
        "promotion": {
            "dest_branch": args.promoted_dest_branch,
            "reference": args.promoted_ref,
            "performed": bool(args.promoted_ref),
        },
    }

    history.append(entry)
    if args.history_limit > 0 and len(history) > args.history_limit:
        history = history[-args.history_limit :]

    payload = {
        "project_id": args.project_id,
        "updated_at": _utc_now(),
        "latest": entry,
        "history": history,
    }
    store.put_json(key, payload)
    print(key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
