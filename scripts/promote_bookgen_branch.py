#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from urllib.parse import quote

import httpx

from app.clients.lakefs import LakeFSClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Promote a BookGen LakeFS branch into a destination branch (default: main).",
    )
    parser.add_argument("--project-id", required=True, help="BookGen project id")
    parser.add_argument("--source-branch", help="Source branch name. Defaults to <prefix>-<project-id>.")
    parser.add_argument("--dest-branch", default="main", help="Destination branch to merge into")
    parser.add_argument("--message", default="", help="Optional merge commit message")
    parser.add_argument(
        "--require-object-path",
        action="append",
        default=[],
        help="Require this object path to exist on source branch before merge (repeatable).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the resolved merge request without executing it",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = LakeFSClient()
    if not client.enabled:
        raise SystemExit("LakeFS is disabled; set LAKEFS_ENABLED=true and required credentials.")

    source_branch = args.source_branch or f"{client.bookgen_branch_prefix}-{args.project_id}"
    dest_branch = args.dest_branch
    message = args.message.strip() or f"Promote BookGen project {args.project_id} to {dest_branch}"
    required_paths = [str(path).strip() for path in args.require_object_path if str(path).strip()]

    for path in required_paths:
        if not client.object_exists(branch=source_branch, path=path):
            raise SystemExit(
                f"Required object missing on source branch '{source_branch}': {path}. Aborting promotion."
            )

    payload = {"message": message}
    repo = quote(client.repo, safe="")
    source_q = quote(source_branch, safe="")
    dest_q = quote(dest_branch, safe="")
    path = f"/api/v1/repositories/{repo}/refs/{source_q}/merge/{dest_q}"

    if args.dry_run:
        print(
            json.dumps(
                {
                    "endpoint": client.endpoint,
                    "repo": client.repo,
                    "source_branch": source_branch,
                    "dest_branch": dest_branch,
                    "path": path,
                    "payload": payload,
                },
                indent=2,
            )
        )
        return 0

    response = httpx.post(
        f"{client.endpoint}{path}",
        auth=(client.access_key, client.secret_key),
        json=payload,
        timeout=client.timeout,
    )
    response.raise_for_status()
    body = response.json()
    print(json.dumps(body, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
