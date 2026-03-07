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

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import get_settings
from app.storage import ObjectStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare a KDP-ready handoff folder from BookGen export artifacts.")
    parser.add_argument("--project-id", required=True, help="BookGen project id")
    parser.add_argument("--installment-id", default="book-01", help="Installment id (default: book-01)")
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory (default: exports/kdp-handoffs/<project-id>/<installment-id>)",
    )
    parser.add_argument(
        "--author",
        default="",
        help="Optional explicit author override for KDP metadata.",
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
    os.environ["MINIO_ENDPOINT"] = "http://127.0.0.1:19000"
    if not os.getenv("MINIO_LOCAL_ENDPOINT"):
        os.environ["MINIO_LOCAL_ENDPOINT"] = "http://127.0.0.1:19000"
    pf = _start_minio_port_forward(namespace=namespace, local_port=19000)
    get_settings.cache_clear()
    return ObjectStore(), pf


def _read_bytes(store: ObjectStore, key: str) -> bytes:
    response = store.client.get_object(store.bucket, key)
    data = response.read()
    response.close()
    response.release_conn()
    return data


def _must_get_json(store: ObjectStore, key: str) -> dict[str, Any]:
    if not store.exists(key):
        raise RuntimeError(f"Missing required JSON object: {key}")
    payload = store.get_json(key)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object at key: {key}")
    return payload


def _maybe_get_yaml(store: ObjectStore, key: str) -> dict[str, Any] | None:
    if not key or not store.exists(key):
        return None
    payload = store.get_yaml(key)
    if isinstance(payload, dict):
        return payload
    return None


def _safe_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _safe_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _author_from_project(store: ObjectStore, project_id: str) -> str:
    intake_key = f"runs/{project_id}/meta/intake.json"
    if store.exists(intake_key):
        intake = store.get_json(intake_key)
        bookspec_key = str(intake.get("bookspec_key", "")).strip()
        if bookspec_key and store.exists(bookspec_key):
            bookspec = store.get_json(bookspec_key)
            author = str(bookspec.get("author", "")).strip()
            if author:
                return author
    planning_key = f"runs/{project_id}/meta/planning_manifest.json"
    if store.exists(planning_key):
        planning = store.get_json(planning_key)
        constitution_key = str(planning.get("constitution_key", "")).strip()
        if constitution_key and store.exists(constitution_key):
            constitution = store.get_yaml(constitution_key)
            author = str((constitution or {}).get("author", "")).strip()
            if author:
                return author
    return ""


def main() -> int:
    args = parse_args()
    project_id = args.project_id.strip()
    installment_id = args.installment_id.strip() or "book-01"
    out_dir = (
        Path(args.output_dir)
        if args.output_dir
        else (ROOT / "exports" / "kdp-handoffs" / project_id / installment_id)
    )
    namespace = str(os.getenv("BOOKGEN_NAMESPACE", "sideline-wire-dailycast")).strip() or "sideline-wire-dailycast"

    store, pf = _build_store_with_fallback(namespace=namespace)
    try:
        export_manifest_key = f"exports/{project_id}/{installment_id}/export_manifest.json"
        export_manifest = _must_get_json(store, export_manifest_key)

        export_keys = [str(item).strip() for item in export_manifest.get("export_keys", []) if str(item).strip()]
        downloaded: dict[str, str] = {}

        for key in export_keys:
            lower = key.lower()
            payload = _read_bytes(store, key)
            if lower.endswith(".epub"):
                target = out_dir / "ebook" / "manuscript.epub"
            elif lower.endswith(".pdf"):
                target = out_dir / "paperback" / "interior" / "manuscript.pdf"
            elif lower.endswith(".docx"):
                target = out_dir / "source" / "manuscript.docx"
            elif lower.endswith(".md"):
                target = out_dir / "source" / "manuscript.md"
            else:
                target = out_dir / "source" / Path(key).name
            _safe_write_bytes(target, payload)
            downloaded[key] = str(target)

        metadata_pack_key = str(export_manifest.get("metadata_pack_key", "")).strip()
        metadata_pack = _maybe_get_yaml(store, metadata_pack_key) or {}
        publication_manifest_key = str(export_manifest.get("publication_manifest_key", "")).strip()
        publication_manifest = _maybe_get_yaml(store, publication_manifest_key) or {}

        author = args.author.strip() or _author_from_project(store, project_id)
        title = str(export_manifest.get("title", "")).strip()
        series_title = str(metadata_pack.get("series_title", "")).strip()
        blurb = str(export_manifest.get("blurb", "")).strip()
        keywords = [str(item).strip() for item in export_manifest.get("keywords", []) if str(item).strip()]
        categories = [str(item).strip() for item in export_manifest.get("categories", []) if str(item).strip()]

        kdp_metadata = {
            "project_id": project_id,
            "installment_id": installment_id,
            "title": title,
            "series_title": series_title,
            "author": author,
            "description": blurb,
            "keywords": keywords,
            "categories": categories,
            "language": "English",
            "ebook_file": str((out_dir / "ebook" / "manuscript.epub")),
            "paperback_interior_file": str((out_dir / "paperback" / "interior" / "manuscript.pdf")),
        }

        _safe_write_text(out_dir / "metadata" / "kdp_metadata.json", json.dumps(kdp_metadata, indent=2) + "\n")
        _safe_write_text(out_dir / "metadata" / "export_manifest.json", json.dumps(export_manifest, indent=2) + "\n")
        _safe_write_text(out_dir / "metadata" / "metadata_pack.yaml", yaml.safe_dump(metadata_pack, sort_keys=False, allow_unicode=False))
        _safe_write_text(
            out_dir / "metadata" / "publication_manifest.yaml",
            yaml.safe_dump(publication_manifest, sort_keys=False, allow_unicode=False),
        )

        ebook_ready = (out_dir / "ebook" / "manuscript.epub").exists()
        paperback_ready = (out_dir / "paperback" / "interior" / "manuscript.pdf").exists()
        metadata_ready = bool(title and author and blurb and keywords and categories)

        checklist = [
            "# KDP Upload Checklist",
            "",
            f"Project: `{project_id}`",
            f"Installment: `{installment_id}`",
            "",
            "## Files",
            f"- Ebook EPUB present: {'yes' if ebook_ready else 'no'}",
            f"- Paperback interior PDF present: {'yes' if paperback_ready else 'no'}",
            f"- Source DOCX present: {'yes' if (out_dir / 'source' / 'manuscript.docx').exists() else 'no'}",
            "",
            "## Metadata",
            f"- Title present: {'yes' if title else 'no'}",
            f"- Author present: {'yes' if author else 'no'}",
            f"- Description present: {'yes' if blurb else 'no'}",
            f"- Keywords present: {'yes' if keywords else 'no'}",
            f"- Categories present: {'yes' if categories else 'no'}",
            "",
            "## Required Manual Inputs",
            "- Ebook cover image (JPG/TIFF) must be prepared and uploaded in KDP.",
            "- Paperback full-wrap cover PDF must be prepared using KDP template (trim size + page count dependent).",
            "- Run KDP online previewer checks for both ebook and paperback before publishing.",
            "",
            f"Metadata completeness: {'ready' if metadata_ready else 'incomplete'}",
        ]
        _safe_write_text(out_dir / "checklists" / "kdp_upload_checklist.md", "\n".join(checklist) + "\n")

        summary = {
            "project_id": project_id,
            "installment_id": installment_id,
            "output_dir": str(out_dir),
            "downloaded_files": downloaded,
            "kdp_metadata_path": str(out_dir / "metadata" / "kdp_metadata.json"),
            "checklist_path": str(out_dir / "checklists" / "kdp_upload_checklist.md"),
            "ready_flags": {
                "ebook_epub": ebook_ready,
                "paperback_pdf": paperback_ready,
                "metadata_complete": metadata_ready,
            },
        }
        _safe_write_text(out_dir / "kdp_handoff_summary.json", json.dumps(summary, indent=2) + "\n")
        print(json.dumps(summary, indent=2))
        return 0
    finally:
        if pf is not None:
            pf.terminate()


if __name__ == "__main__":
    raise SystemExit(main())
