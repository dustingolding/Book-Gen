#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.storage import ObjectStore


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a publish-candidate dossier from BookGen artifacts.")
    parser.add_argument("--project-id", required=True)
    parser.add_argument(
        "--candidate-key",
        default="",
        help="Output object key (default: runs/<project_id>/meta/publish_candidate.json)",
    )
    parser.add_argument(
        "--readiness-key",
        default="",
        help="Readiness report key (default: runs/<project_id>/meta/publish_readiness.json)",
    )
    parser.add_argument(
        "--checklist-output",
        default="",
        help="Optional local checklist output path (default: docs/bookgen/checklists/<project_id>.md)",
    )
    return parser.parse_args()


def _must_get_json(store: ObjectStore, key: str) -> dict[str, Any]:
    if not store.exists(key):
        raise RuntimeError(f"Required object missing: {key}")
    payload = store.get_json(key)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Object is not a JSON object: {key}")
    return payload


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _start_minio_port_forward(namespace: str, local_port: int = 19000) -> subprocess.Popen[str]:
    proc = subprocess.Popen(
        [
            "kubectl",
            "-n",
            namespace,
            "port-forward",
            "svc/minio",
            f"{local_port}:9000",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    for _ in range(20):
        if _port_open("127.0.0.1", local_port):
            return proc
        time.sleep(0.5)
    proc.terminate()
    raise RuntimeError("Failed to establish MinIO port-forward to 127.0.0.1:19000")


def _is_dns_resolution_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(
        marker in msg
        for marker in (
            "temporary failure in name resolution",
            "name or service not known",
            "failed to resolve",
            "nodename nor servname provided",
        )
    )


def _build_store_with_fallback(namespace: str) -> tuple[ObjectStore, subprocess.Popen[str] | None]:
    try:
        return ObjectStore(), None
    except Exception as exc:
        if not _is_dns_resolution_error(exc):
            raise
    os.environ["MINIO_ENDPOINT"] = "http://127.0.0.1:19000"
    if not os.getenv("MINIO_LOCAL_ENDPOINT"):
        os.environ["MINIO_LOCAL_ENDPOINT"] = "http://127.0.0.1:19000"
    pf = _start_minio_port_forward(namespace=namespace, local_port=19000)
    return ObjectStore(), pf


def _render_checklist(project_id: str, installment_id: str) -> str:
    template_path = ROOT / "docs" / "bookgen" / "publish_checklist.template.md"
    if not template_path.exists():
        raise RuntimeError(f"Checklist template not found: {template_path}")
    raw = template_path.read_text(encoding="utf-8")
    today = datetime.now(timezone.utc).date().isoformat()
    raw = raw.replace("<project_id>", project_id)
    raw = raw.replace("<installment_id>", installment_id or "unknown-installment")
    raw = raw.replace("<yyyy-mm-dd>", today)
    return raw


def main() -> int:
    args = parse_args()
    project_id = args.project_id
    namespace = str(os.getenv("BOOKGEN_NAMESPACE", "sideline-wire-dailycast")).strip() or "sideline-wire-dailycast"
    store, pf = _build_store_with_fallback(namespace=namespace)

    readiness_key = args.readiness_key.strip() or f"runs/{project_id}/meta/publish_readiness.json"
    candidate_key = args.candidate_key.strip() or f"runs/{project_id}/meta/publish_candidate.json"
    release_audit_key = f"runs/{project_id}/meta/release_audit.json"

    try:
        readiness = _must_get_json(store, readiness_key)
        status = str(readiness.get("status", "")).strip().lower()
        if status != "pass":
            failed = readiness.get("failed_checks", [])
            raise RuntimeError(f"Readiness is not pass for {project_id}. failed_checks={failed}")

        release_audit = _must_get_json(store, release_audit_key)
        latest = release_audit.get("latest")
        if not isinstance(latest, dict):
            raise RuntimeError(f"release_audit latest entry missing for project {project_id}")

        installment_id = str(readiness.get("installment_id", "")).strip()
        bookspec_key = str(latest.get("bookspec_key", "")).strip()
        promotion = latest.get("promotion") if isinstance(latest.get("promotion"), dict) else {}
        promotion_ref = str(promotion.get("reference", "")).strip()
        dest_branch = str(promotion.get("dest_branch", "")).strip()

        export_root = f"exports/{project_id}/{installment_id}" if installment_id else ""
        export_manifest_key = f"{export_root}/export_manifest.json" if export_root else ""
        manuscript_md_key = f"{export_root}/manuscript.md" if export_root else ""
        manuscript_docx_key = f"{export_root}/manuscript.docx" if export_root else ""

        candidate = {
            "generated_at": utc_now(),
            "project_id": project_id,
            "installment_id": installment_id,
            "status": "candidate",
            "source": {
                "readiness_key": readiness_key,
                "release_audit_key": release_audit_key,
                "bookspec_key": bookspec_key,
            },
            "promotion": {
                "reference": promotion_ref,
                "dest_branch": dest_branch,
            },
            "exports": {
                "root": export_root,
                "export_manifest_key": export_manifest_key,
                "manuscript_md_key": manuscript_md_key,
                "manuscript_docx_key": manuscript_docx_key,
            },
            "release": {
                "run_date": str(latest.get("run_date", "")),
                "job_name": ((latest.get("job") or {}) if isinstance(latest.get("job"), dict) else {}).get("name", ""),
                "node_name": ((latest.get("job") or {}) if isinstance(latest.get("job"), dict) else {}).get("node_name", ""),
            },
        }
        store.put_json(candidate_key, candidate)

        checklist_output = args.checklist_output.strip()
        if not checklist_output:
            checklist_output = str(ROOT / "docs" / "bookgen" / "checklists" / f"{project_id}.md")
        checklist_path = Path(checklist_output)
        checklist_path.parent.mkdir(parents=True, exist_ok=True)
        checklist_text = _render_checklist(project_id=project_id, installment_id=installment_id)
        checklist_path.write_text(checklist_text, encoding="utf-8")

        print(
            json.dumps(
                {
                    "candidate_key": candidate_key,
                    "checklist_path": str(checklist_path),
                    "project_id": project_id,
                    "installment_id": installment_id,
                },
                indent=2,
            )
        )
        return 0
    finally:
        if pf is not None:
            pf.terminate()


if __name__ == "__main__":
    raise SystemExit(main())
