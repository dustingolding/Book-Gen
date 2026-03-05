from __future__ import annotations


def factpack_json(process_date: str) -> str:
    return f"factpacks/{process_date}/factpack.json"


def notebooklm_dir(process_date: str) -> str:
    return f"notebooklm/{process_date}/"


def rag_chunks_jsonl(process_date: str) -> str:
    return f"rag/{process_date}/chunks.jsonl"


def script_draft_json(process_date: str) -> str:
    return f"scripts/{process_date}/script.draft.json"


def script_realism_json(process_date: str) -> str:
    return f"scripts/{process_date}/script.realism.json"


def script_final_json(process_date: str) -> str:
    return f"scripts/{process_date}/script.final.json"


def qa_report_json(process_date: str) -> str:
    return f"quality/{process_date}/qa_report.json"


def ssml_xml(process_date: str) -> str:
    return f"audio/{process_date}/episode.ssml"


def audio_mp3(process_date: str) -> str:
    return f"audio/{process_date}/episode.mp3"


def publish_manifest(process_date: str) -> str:
    return f"publish/{process_date}/manifest.v4.json"
