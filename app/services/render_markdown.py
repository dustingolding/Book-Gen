from app.storage import ObjectStore


def run(run_date: str) -> dict:
    store = ObjectStore()
    factpack = store.get_json(f"factpacks/{run_date}/factpack.json")
    verification = store.get_json(f"factpacks/{run_date}/verification.json")
    identity = factpack.get("show_identity", {})
    lines = [
        f"# {identity.get('name', 'SideLine Wire DailyCast')} - Fact Pack",
        "",
        f"Run Date: {run_date}",
        f"Target runtime: {factpack.get('meta', {}).get('runtime_target_minutes', 40)} min",
        f"Yesterday ET window: {factpack.get('meta', {}).get('yesterday_window_et', {}).get('start', '')} -> {factpack.get('meta', {}).get('yesterday_window_et', {}).get('end', '')}",
        "",
        "## Locked Identity",
        "",
        f"- Show: {identity.get('name', 'unknown')}",
        f"- Primary hosts: {', '.join(identity.get('hosts', {}).get('primary', []))}",
    ]

    lines.extend(["", "## Coverage Allocations"])
    for allocation in factpack.get("coverage", {}).get("allocations", []):
        lines.append(
            f"- {allocation.get('sport', '')}: {float(allocation.get('minutes', 0.0)):.1f} min ({allocation.get('phase', '')})"
        )

    lines.extend(["", "## Source Snapshot"])
    lines.append(f"- Yesterday results: {len(factpack.get('yesterday_results', []))}")
    lines.append(f"- Today matchups: {len(factpack.get('today_matchups', []))}")
    lines.append(f"- Major news items: {len(factpack.get('major_news', []))}")

    lines.extend(
        [
            "",
            "## Verification",
            f"- Citation coverage: {float(verification.get('citation_coverage', 0.0)):.2f}",
            f"- Item count: {int(verification.get('item_count', 0))}",
            f"- Items with citations: {int(verification.get('items_with_citation', 0))}",
            f"- Approved: {bool(verification.get('approved', False))}",
        ]
    )

    rendered = "\n".join(lines) + "\n"
    key = f"publish/{run_date}/dailycast.md"
    store.put_text(key, rendered, content_type="text/markdown")

    return {"key": key, "bytes": len(rendered.encode("utf-8"))}
