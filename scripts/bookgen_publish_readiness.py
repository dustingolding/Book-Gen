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
    parser = argparse.ArgumentParser(description="Validate BookGen publish readiness for a project.")
    parser.add_argument("--project-id", required=True)
    parser.add_argument("--report-key", default="", help="Override output key (default: runs/<project>/meta/publish_readiness.json)")
    parser.add_argument(
        "--no-strict",
        dest="strict",
        action="store_false",
        help="Always exit zero even if readiness is fail",
    )
    parser.set_defaults(strict=True)
    parser.add_argument(
        "--require-promotion",
        action="store_true",
        help="Require promotion reference in release audit latest entry",
    )
    return parser.parse_args()


def _get_json_if_exists(store: ObjectStore, key: str) -> dict[str, Any] | None:
    if not store.exists(key):
        return None
    payload = store.get_json(key)
    return payload if isinstance(payload, dict) else None


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


def main() -> int:
    args = parse_args()
    project_id = args.project_id
    namespace = str(os.getenv("BOOKGEN_NAMESPACE", "sideline-wire-dailycast")).strip() or "sideline-wire-dailycast"
    store, pf = _build_store_with_fallback(namespace=namespace)
    try:
        release_audit_key = f"runs/{project_id}/meta/release_audit.json"
        planning_manifest_key = f"runs/{project_id}/meta/planning_manifest.json"
        assembly_stage_key = f"runs/{project_id}/meta/stages/assembly-export.json"
        assembly_artifacts_key = f"runs/{project_id}/meta/stages/assembly-export.artifacts.json"

        release_audit = _get_json_if_exists(store, release_audit_key)
        latest = (release_audit or {}).get("latest") if isinstance((release_audit or {}).get("latest"), dict) else {}
        bookspec_key = str(latest.get("bookspec_key", "")).strip()
        bookspec = _get_json_if_exists(store, bookspec_key) if bookspec_key else None
        planning_manifest = _get_json_if_exists(store, planning_manifest_key)

        installment_id = ""
        if isinstance(planning_manifest, dict):
            installment_id = str(planning_manifest.get("installment_id", "")).strip()
        if not installment_id and isinstance(bookspec, dict):
            installment_id = str(bookspec.get("installment_id", "")).strip()

        export_root = f"exports/{project_id}/{installment_id}" if installment_id else ""
        export_manifest_key = f"{export_root}/export_manifest.json" if export_root else ""
        manuscript_md_key = f"{export_root}/manuscript.md" if export_root else ""
        manuscript_docx_key = f"{export_root}/manuscript.docx" if export_root else ""

        checks: list[dict[str, Any]] = []

        def check_exists(name: str, key: str, *, required: bool = True) -> None:
            ok = bool(key) and store.exists(key)
            checks.append({"name": name, "ok": ok, "key": key, "required": required})

        check_exists("release_audit", release_audit_key, required=True)
        check_exists("planning_manifest", planning_manifest_key, required=True)
        check_exists("assembly_stage", assembly_stage_key, required=True)
        check_exists("assembly_artifacts", assembly_artifacts_key, required=False)
        check_exists("export_manifest", export_manifest_key, required=True)
        check_exists("manuscript_md", manuscript_md_key, required=True)
        check_exists("manuscript_docx", manuscript_docx_key, required=True)

        promotion_ref = ""
        if isinstance(latest, dict):
            promotion = latest.get("promotion")
            if isinstance(promotion, dict):
                promotion_ref = str(promotion.get("reference", "")).strip()

        if args.require_promotion:
            checks.append(
                {
                    "name": "promotion_reference_present",
                    "ok": bool(promotion_ref),
                    "value": promotion_ref,
                    "required": True,
                }
            )

        failed = [item for item in checks if bool(item.get("required", True)) and not bool(item.get("ok"))]
        status = "pass" if not failed else "fail"

        report = {
            "generated_at": utc_now(),
            "project_id": project_id,
            "status": status,
            "installment_id": installment_id,
            "promotion_reference": promotion_ref,
            "checks": checks,
            "failed_checks": [item.get("name", "") for item in failed],
        }

        report_key = args.report_key.strip() or f"runs/{project_id}/meta/publish_readiness.json"
        store.put_json(report_key, report)
        print(json.dumps({"report_key": report_key, "status": status, "failed_checks": report["failed_checks"]}, indent=2))

        if args.strict:
            return 0 if status == "pass" else 1
        return 0
    finally:
        if pf is not None:
            pf.terminate()


if __name__ == "__main__":
    raise SystemExit(main())
