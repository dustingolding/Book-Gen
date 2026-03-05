#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.storage import ObjectStore


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload BookGen prompt-pack and rubric assets to object storage.")
    parser.add_argument(
        "--manifest",
        default="docs/bookgen/prompt_pack_manifest.juvenile_fiction.v1.json",
        help="Path to prompt pack manifest JSON file.",
    )
    parser.add_argument(
        "--rubric",
        default="docs/bookgen/rubric.juvenile_fiction.v1.json",
        help="Path to rubric JSON file.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print target object keys without writing to object storage.",
    )
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    rubric_path = Path(args.rubric)
    if not manifest_path.exists():
        raise RuntimeError(f"Manifest file does not exist: {manifest_path}")
    if not rubric_path.exists():
        raise RuntimeError(f"Rubric file does not exist: {rubric_path}")

    manifest = _load_json(manifest_path)
    rubric = _load_json(rubric_path)

    genre = str(manifest.get("genre", "")).strip().lower()
    manifest_version = str(manifest.get("version", "")).strip()
    rubric_genre = str(rubric.get("genre", "")).strip().lower()
    rubric_version = str(rubric.get("version", "")).strip()
    if not genre or not manifest_version:
        raise RuntimeError("Manifest must include non-empty 'genre' and 'version'.")
    if not rubric_genre or not rubric_version:
        raise RuntimeError("Rubric must include non-empty 'genre' and 'version'.")
    if genre != rubric_genre:
        raise RuntimeError(f"Genre mismatch: manifest={genre!r} rubric={rubric_genre!r}")

    manifest_key = f"prompt-packs/{genre}/{manifest_version}/manifest.json"
    rubric_key = f"rubrics/{genre}/{rubric_version}/rubric.json"
    payload = {
        "manifest_path": str(manifest_path),
        "rubric_path": str(rubric_path),
        "manifest_key": manifest_key,
        "rubric_key": rubric_key,
    }

    if args.dry_run:
        print(json.dumps({"status": "dry_run", **payload}, indent=2, ensure_ascii=True))
        return

    store = ObjectStore()
    store.put_json(manifest_key, manifest)
    store.put_json(rubric_key, rubric)
    print(json.dumps({"status": "uploaded", **payload}, indent=2, ensure_ascii=True))


if __name__ == "__main__":
    main()
