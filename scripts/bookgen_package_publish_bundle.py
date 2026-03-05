#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import zipfile


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Package a BookGen publish bundle into a zip with integrity metadata.")
    parser.add_argument("--project-id", required=True)
    parser.add_argument(
        "--bundle-dir",
        default="",
        help="Path to publish bundle (default: exports/publish-bundles/<project-id>)",
    )
    parser.add_argument(
        "--output-dir",
        default="",
        help="Output directory for packaged artifacts (default: exports/publish-packages/<project-id>)",
    )
    return parser.parse_args()


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    args = parse_args()
    project_id = args.project_id
    repo_root = Path(__file__).resolve().parents[1]

    bundle_dir = Path(args.bundle_dir) if args.bundle_dir else (repo_root / "exports" / "publish-bundles" / project_id)
    if not bundle_dir.exists():
        raise RuntimeError(f"Bundle directory not found: {bundle_dir}")

    out_dir = Path(args.output_dir) if args.output_dir else (repo_root / "exports" / "publish-packages" / project_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted([p for p in bundle_dir.rglob("*") if p.is_file()])
    if not files:
        raise RuntimeError(f"No files found in bundle directory: {bundle_dir}")

    package_name = f"{project_id}-publish-bundle.zip"
    package_path = out_dir / package_name

    with zipfile.ZipFile(package_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for file_path in files:
            arcname = file_path.relative_to(bundle_dir).as_posix()
            zf.write(file_path, arcname=arcname)

    entries: list[dict[str, object]] = []
    for file_path in files:
        rel = file_path.relative_to(bundle_dir).as_posix()
        entries.append(
            {
                "path": rel,
                "size_bytes": file_path.stat().st_size,
                "sha256": _sha256(file_path),
            }
        )

    package_sha = _sha256(package_path)
    # Keep SHA256SUMS self-verifiable from the package directory.
    sha_sums_path = out_dir / "SHA256SUMS.txt"
    sha_sums_path.write_text(f"{package_sha}  {package_name}\n", encoding="utf-8")

    # Preserve source-file checksums as metadata for deep validation after unzip.
    bundle_sha_sums_path = out_dir / "BUNDLE_SHA256SUMS.txt"
    bundle_sha_lines = [f"{entry['sha256']}  {entry['path']}" for entry in entries]
    bundle_sha_sums_path.write_text("\n".join(bundle_sha_lines) + "\n", encoding="utf-8")

    manifest = {
        "project_id": project_id,
        "bundle_dir": str(bundle_dir),
        "package_path": str(package_path),
        "package_sha256": package_sha,
        "sha256sums_path": str(sha_sums_path),
        "bundle_sha256sums_path": str(bundle_sha_sums_path),
        "file_count": len(entries),
        "files": entries,
    }
    manifest_path = out_dir / "bundle_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "project_id": project_id,
                "package_path": str(package_path),
                "package_sha256": package_sha,
                "sha_sums_path": str(sha_sums_path),
                "bundle_sha_sums_path": str(bundle_sha_sums_path),
                "manifest_path": str(manifest_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
