#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.bookgen import _load_bookspec_from_local
from app.storage import ObjectStore


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload a BookGen input file to object storage for in-cluster jobs.")
    parser.add_argument("--project-id", required=True, help="BookGen project id.")
    parser.add_argument("--input-path", required=True, help="Local path to markdown/json/yaml input file.")
    parser.add_argument(
        "--object-key",
        default="",
        help="Target object key. Defaults to inputs/<project-id>/bookspec.json.",
    )
    args = parser.parse_args()

    source = Path(args.input_path).expanduser().resolve()
    if not source.exists():
        raise RuntimeError(f"Input file does not exist: {source}")

    key = str(args.object_key).strip()
    if not key:
        key = f"inputs/{args.project_id}/bookspec.json"

    suffix = source.suffix.lower()
    if suffix == ".json":
        payload_obj = json.loads(source.read_text(encoding="utf-8"))
    elif suffix in {".md", ".markdown", ".yaml", ".yml"}:
        payload_obj = _load_bookspec_from_local(str(source))
    else:
        raise RuntimeError(f"Unsupported input format: {source.suffix}. Use .json, .md, .yaml, or .yml.")

    payload = json.dumps(payload_obj, ensure_ascii=False, indent=2)
    store = ObjectStore()
    store.put_text(key, payload, content_type="application/json")
    print(key)


if __name__ == "__main__":
    main()
