#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export a local publishing bundle for a BookGen project.")
    parser.add_argument("--project-id", required=True)
    parser.add_argument(
        "--output-dir",
        default="",
        help="Local output directory (default: exports/publish-bundles/<project-id>)",
    )
    return parser.parse_args()


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
    kubeconfig_path = Path.home() / ".kube" / "config"
    if os.getenv("GITHUB_ACTIONS") == "true" and not kubeconfig_path.exists() and not os.getenv("KUBE_CONFIG_B64"):
        raise RuntimeError(
            "MinIO endpoint DNS is not resolvable from this runner and kubeconfig is unavailable. "
            "Use a self-hosted runner with cluster access, set KUBE_CONFIG_B64, or provide externally reachable MINIO_ENDPOINT."
        )
    os.environ["MINIO_ENDPOINT"] = "http://127.0.0.1:19000"
    if not os.getenv("MINIO_LOCAL_ENDPOINT"):
        os.environ["MINIO_LOCAL_ENDPOINT"] = "http://127.0.0.1:19000"
    pf = _start_minio_port_forward(namespace=namespace, local_port=19000)
    return ObjectStore(), pf


def _must_get_json(store: ObjectStore, key: str) -> dict[str, Any]:
    if not store.exists(key):
        raise RuntimeError(f"Missing required object: {key}")
    payload = store.get_json(key)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object at key: {key}")
    return payload


def _download_bytes(store: ObjectStore, key: str) -> bytes:
    response = store.client.get_object(store.bucket, key)
    data = response.read()
    response.close()
    response.release_conn()
    return data


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    project_id = args.project_id
    out_dir = Path(args.output_dir) if args.output_dir else (ROOT / "exports" / "publish-bundles" / project_id)
    namespace = str(os.getenv("BOOKGEN_NAMESPACE", "sideline-wire-dailycast")).strip() or "sideline-wire-dailycast"

    store, pf = _build_store_with_fallback(namespace=namespace)
    try:
        release_audit_key = f"runs/{project_id}/meta/release_audit.json"
        readiness_key = f"runs/{project_id}/meta/publish_readiness.json"
        candidate_key = f"runs/{project_id}/meta/publish_candidate.json"

        release_audit = _must_get_json(store, release_audit_key)
        readiness = _must_get_json(store, readiness_key)
        candidate = _must_get_json(store, candidate_key)

        exports = candidate.get("exports") if isinstance(candidate.get("exports"), dict) else {}
        manuscript_md_key = str(exports.get("manuscript_md_key", "")).strip()
        manuscript_docx_key = str(exports.get("manuscript_docx_key", "")).strip()
        export_manifest_key = str(exports.get("export_manifest_key", "")).strip()
        export_manifest = _must_get_json(store, export_manifest_key)

        # Write metadata artifacts
        _write_json(out_dir / "meta" / "release_audit.json", release_audit)
        _write_json(out_dir / "meta" / "publish_readiness.json", readiness)
        _write_json(out_dir / "meta" / "publish_candidate.json", candidate)
        _write_json(out_dir / "meta" / "export_manifest.json", export_manifest)

        # Write manuscript artifacts
        if not manuscript_md_key or not store.exists(manuscript_md_key):
            raise RuntimeError(f"Missing manuscript markdown artifact: {manuscript_md_key}")
        md_text = store.get_text(manuscript_md_key)
        md_path = out_dir / "manuscript" / "manuscript.md"
        md_path.parent.mkdir(parents=True, exist_ok=True)
        md_path.write_text(md_text, encoding="utf-8")

        if not manuscript_docx_key or not store.exists(manuscript_docx_key):
            raise RuntimeError(f"Missing manuscript docx artifact: {manuscript_docx_key}")
        docx_bytes = _download_bytes(store, manuscript_docx_key)
        docx_path = out_dir / "manuscript" / "manuscript.docx"
        docx_path.write_bytes(docx_bytes)

        summary = {
            "project_id": project_id,
            "bundle_dir": str(out_dir),
            "files": {
                "manuscript_md": str(md_path),
                "manuscript_docx": str(docx_path),
                "release_audit": str(out_dir / "meta" / "release_audit.json"),
                "publish_readiness": str(out_dir / "meta" / "publish_readiness.json"),
                "publish_candidate": str(out_dir / "meta" / "publish_candidate.json"),
                "export_manifest": str(out_dir / "meta" / "export_manifest.json"),
            },
        }
        print(json.dumps(summary, indent=2))
        return 0
    finally:
        if pf is not None:
            pf.terminate()


if __name__ == "__main__":
    raise SystemExit(main())
