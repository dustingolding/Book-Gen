from app.storage import ObjectStore


def run(run_date: str) -> dict:
    store = ObjectStore()
    factpack = store.get_json(f"factpacks/{run_date}/factpack.json")
    verification = store.get_json(f"factpacks/{run_date}/verification.json")

    manifest = {
        "run_date": run_date,
        "factpack_key": f"factpacks/{run_date}/factpack.json",
        "verification_key": f"factpacks/{run_date}/verification.json",
        "notebooklm_prefix": f"notebooklm/{run_date}/",
        "markdown_key": f"publish/{run_date}/dailycast.md",
        "verified": bool(verification.get("approved", False)),
        "citation_coverage": float(verification.get("citation_coverage", 0.0)),
        "result_count": len(factpack.get("yesterday_results", [])),
        "matchup_count": len(factpack.get("today_matchups", [])),
        "news_count": len(factpack.get("major_news", [])),
    }
    store.put_json(f"publish/{run_date}/manifest.json", manifest)
    return manifest
