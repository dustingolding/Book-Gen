from __future__ import annotations

import json

from prefect import flow, task

from app.clients.lakefs import LakeFSClient
from app.services.elevenlabs_v4 import build_ssml
from app.services.lakefs_paths import audio_mp3, script_final_json, ssml_xml


@task(retries=1, retry_delay_seconds=10, log_prints=True)
def elevenlabs_task(run_date: str, lakefs_branch: str) -> dict:
    lakefs = LakeFSClient()
    if not lakefs.enabled:
        return {"run_date": run_date, "status": "skipped", "reason": "lakefs_disabled"}

    script = json.loads(lakefs.download_object(branch=lakefs_branch, path=script_final_json(run_date)).decode("utf-8"))
    ssml = build_ssml(script)

    ssml_key = ssml_xml(run_date)
    lakefs.upload_object(
        branch=lakefs_branch,
        path=ssml_key,
        content=ssml.encode("utf-8"),
        content_type="application/ssml+xml",
    )

    # Placeholder output to keep stage contracts stable until TTS synthesis is enabled.
    audio_key = audio_mp3(run_date)
    lakefs.upload_object(
        branch=lakefs_branch,
        path=audio_key,
        content=b"",
        content_type="audio/mpeg",
    )

    return {"run_date": run_date, "status": "ssml_ready", "ssml_key": ssml_key, "audio_key": audio_key}


@flow(name="slw-dailycast-elevenlabs")
def elevenlabs_flow(run_date: str, lakefs_branch: str) -> dict:
    return elevenlabs_task(run_date, lakefs_branch)
