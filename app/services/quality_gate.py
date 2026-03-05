# app/services/quality_gate.py

from app.storage import ObjectStore

def run(run_date: str) -> dict:
    store = ObjectStore()
    report = store.get_json(f"factpacks/{run_date}/verification.json")
    coverage = float(report.get("citation_coverage", 0.0))
    issues = list(report.get("issues", []))

    if coverage < 0.9:
        raise RuntimeError(f"Quality gate failed: citation coverage {coverage:.2f} < 0.90")
    if issues:
        raise RuntimeError(f"Quality gate failed: verifier rejected factpack (issues={issues})")

    return {
        "status": "passed",
        "citation_coverage": coverage,
        "issues": issues,
        "item_count": int(report.get("item_count", 0)),
        "items_with_citation": int(report.get("items_with_citation", 0)),
    }
