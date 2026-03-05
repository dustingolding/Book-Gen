from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from app.config import get_settings
from app.storage import ObjectStore

logger = logging.getLogger(__name__)


class FilesystemPublisher:
    def __init__(self) -> None:
        cfg = get_settings()
        self.output_dir = Path(cfg.article_output_dir)
        self.store: ObjectStore | None = None
        try:
            self.store = ObjectStore()
        except Exception as exc:
            logger.warning("filesystem_publisher_object_store_unavailable: %s", exc)

    def publish(self, *, run_date: str, article: dict[str, Any]) -> dict[str, Any]:
        sport = str(article.get("sport", "unknown"))
        slug = str(article.get("slug", "article"))
        local_dir = self.output_dir / run_date / sport
        local_dir.mkdir(parents=True, exist_ok=True)
        for stale in local_dir.glob("*.md"):
            stale.unlink(missing_ok=True)
        for stale in local_dir.glob("*.json"):
            stale.unlink(missing_ok=True)
        md_path = local_dir / f"{slug}.md"
        meta_path = local_dir / f"{slug}.json"
        md_path.write_text(str(article.get("markdown", "")), encoding="utf-8")
        meta_path.write_text(json.dumps(article, ensure_ascii=True, indent=2), encoding="utf-8")
        if self.store is not None:
            try:
                self.store.put_text(f"articles/{run_date}/{sport}/{slug}.md", str(article.get("markdown", "")))
                self.store.put_json(
                    f"articles/{run_date}/{sport}/{slug}.json",
                    article,
                )
                self.store.put_json(
                    f"articles/archive/{sport}/{run_date}-{slug}.json",
                    {
                        "run_date": run_date,
                        "title": article.get("title"),
                        "slug": slug,
                        "central_thesis": article.get("central_thesis"),
                        "markdown": article.get("markdown"),
                        "sport": sport,
                    },
                )
            except Exception as exc:
                logger.warning("filesystem_publisher_object_store_mirror_failed: %s", exc)
        return {
            "sport": sport,
            "slug": slug,
            "markdown_path": str(md_path),
            "metadata_path": str(meta_path),
            "object_prefix": f"articles/{run_date}/{sport}/{slug}" if self.store is not None else None,
        }

    def clear_run(self, run_date: str) -> None:
        run_dir = self.output_dir / run_date
        if run_dir.exists():
            for child in run_dir.iterdir():
                if child.is_dir():
                    for nested in child.iterdir():
                        nested.unlink(missing_ok=True)
                    child.rmdir()
                else:
                    child.unlink(missing_ok=True)
