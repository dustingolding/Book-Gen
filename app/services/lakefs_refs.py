from __future__ import annotations

from datetime import datetime
from uuid import uuid4
from zoneinfo import ZoneInfo

from app.config import get_settings

ET = ZoneInfo("America/New_York")


def dailycast_run_branch(process_date: str) -> str:
    cfg = get_settings()
    prefix = cfg.lakefs_dailycast_branch_prefix.strip().strip("/") or "run"
    run_id = uuid4().hex[:10]
    safe_date = process_date.replace("/", "-")
    return f"{prefix}-{safe_date}-{run_id}"


def stage_commit_message(process_date: str, stage: str) -> str:
    ts = datetime.now(tz=ET).strftime("%Y-%m-%d %H:%M ET")
    return f"DailyCast {process_date} - {stage} ({ts})"
