from __future__ import annotations


def build_manifest(*, run_date: str, branch: str, status: str = "published") -> dict:
    return {
        "run_date": run_date,
        "pipeline": "slw-dailycast-v4",
        "lakefs_branch": branch,
        "status": status,
        "artifacts": {
            "factpack": f"factpacks/{run_date}/factpack.json",
            "notebooklm_prefix": f"notebooklm/{run_date}/",
            "rag_chunks": f"rag/{run_date}/chunks.jsonl",
            "script_draft": f"scripts/{run_date}/script.draft.json",
            "script_realism": f"scripts/{run_date}/script.realism.json",
            "script_final": f"scripts/{run_date}/script.final.json",
            "qa_report": f"quality/{run_date}/qa_report.json",
            "ssml": f"audio/{run_date}/episode.ssml",
            "audio": f"audio/{run_date}/episode.mp3",
        },
    }
