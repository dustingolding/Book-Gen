from __future__ import annotations

from collections import Counter
import re
from typing import Any

from app.storage import ObjectStore


def _is_example_source(url: str) -> bool:
    u = (url or "").lower()
    return ".example/" in u or ".example." in u


def _collect_items(factpack: dict[str, Any]) -> list[dict[str, Any]]:
    return (
        list(factpack.get("yesterday_results", []))
        + list(factpack.get("today_matchups", []))
        + list(factpack.get("major_news", []))
    )


PLAYER_LINE_RE = re.compile(r"^.+ - \d+ PTS, \d+ REB, \d+ AST(?:, \d+ STL)?(?:, \d+ BLK)?$")
PLACEHOLDER_PLAYER_TOKENS = (
    "primary rotation",
    "counter-adjustment",
    "primary group",
    "game participants",
)
WHY_BOILERPLATE_PATTERNS = (
    "outcome signal:",
    "result context:",
    "scoreline takeaway:",
    "player drivers:",
    "editorial handling:",
    "segment cue:",
    "trend-setter game",
    "supporting context, not the lead segment",
    "editorial angle:",
    "keep this in",
    "lead segment",
    "framing note",
)
MATCHUP_BOILERPLATE_PATTERNS = (
    "scheduled matchup with in-season relevance",
    "seeding pressure is increasing",
    "use standings movement and recent form as the segment lens",
    "verified matchup on schedule; standings context unavailable",
    "verified schedule spot, but standing-form data is incomplete",
    "schedule is verified, but recent-form context is incomplete",
    "standings movement and playoff positioning are live",
    "standings position and tiebreak pressure are both in play",
    "meaningful positioning game",
    "full standings-form record is unavailable",
    "full l10 context unavailable",
    "context unavailable for this card",
)
NEWS_THIN_PATTERNS = (
    "who could",
    "the key names to monitor are",
    "let's look back",
    "remember that guy",
    "key development:",
    "which nfl draft prospects should",
    "scheduled for",
)
NEWS_GAMEISH_PATTERNS = (
    "preview",
    "recap",
    "highlights",
    "best bets",
    "prediction",
    "odds",
    "spread",
)
INSTRUCTIONAL_PATTERNS = (
    "editorial",
    "framing note",
    "lead segment",
    "production note",
    "segment cue",
)
LAST10_RE = re.compile(r"\b(\d+)-(\d+)\b")


def _normalized_text(text: str) -> str:
    return re.sub(r"[^a-z0-9 ]+", "", re.sub(r"\s+", " ", str(text or "").strip().lower())).strip()


def _has_repeated_summary_clause(summary: str) -> bool:
    clauses = [c.strip(" .") for c in re.split(r"(?<=[.!?])\s+|;\s+", str(summary or "").strip()) if c.strip()]
    seen: set[str] = set()
    for clause in clauses:
        norm = _normalized_text(clause)
        if not norm:
            continue
        if norm in seen:
            return True
        seen.add(norm)
    return False


def run(run_date: str) -> dict[str, Any]:
    store = ObjectStore()
    factpack = store.get_json(f"factpacks/{run_date}/factpack.json")

    items = _collect_items(factpack)
    with_citation = 0
    example_citation_hits = 0
    for item in items:
        citations = [c for c in (item.get("citations") or []) if isinstance(c, str)]
        if citations:
            with_citation += 1
        example_citation_hits += sum(1 for c in citations if _is_example_source(c))

    total_items = len(items)
    coverage = (with_citation / total_items) if total_items else 0.0

    identity = factpack.get("show_identity", {})
    identity_ok = (
        identity.get("name") == "SideLine Wire DailyCast"
        and "Evan Cole" in (identity.get("hosts", {}).get("primary") or [])
        and "Marcus Reed" in (identity.get("hosts", {}).get("primary") or [])
    )

    allocations = factpack.get("coverage", {}).get("allocations", [])
    allocation_share_sum = sum(float(a.get("share", 0.0) or 0.0) for a in allocations)

    issues: list[str] = []
    if not identity_ok:
        issues.append("identity_contract_missing_or_invalid")
    if coverage < 0.9:
        issues.append("citation_coverage_below_threshold")
    if example_citation_hits > 0:
        issues.append("example_citations_present")
    if abs(allocation_share_sum - 1.0) > 0.05:
        issues.append("coverage_allocations_not_normalized")

    # Yesterday result quality checks.
    yesterday = list(factpack.get("yesterday_results", []))
    bad_key_player_rows = 0
    missing_impact_signals = 0
    why_lines: list[str] = []
    boilerplate_hits = 0
    for item in yesterday:
        sport = str(item.get("sport", "")).strip()
        raw_players = list(item.get("key_players") or [])
        key_players: list[dict[str, Any]] = [p for p in raw_players if isinstance(p, dict)]
        if sport in {"nba", "college_basketball", "womens_college_basketball", "wnba"}:
            if len(key_players) < 2:
                bad_key_player_rows += 1
            else:
                normalized_ok = 0
                for kp in key_players:
                    line = str(kp.get("line", "")).strip()
                    low = line.lower()
                    if any(t in low for t in PLACEHOLDER_PLAYER_TOKENS):
                        continue
                    has_name = bool(str(kp.get("name", "")).strip())
                    has_trend = bool(str(kp.get("trend", "")).strip())
                    has_impact = isinstance(kp.get("impact_score"), (int, float))
                    if has_name and has_trend and has_impact and PLAYER_LINE_RE.match(line):
                        normalized_ok += 1
                if normalized_ok < 2:
                    bad_key_player_rows += 1
        impact = item.get("impact_signals") if isinstance(item.get("impact_signals"), dict) else {}
        if not impact or "margin" not in impact or "total_points" not in impact:
            missing_impact_signals += 1

        for w in (item.get("why_it_matters") or []):
            line = str(w).strip()
            if not line:
                continue
            low = line.lower()
            why_lines.append(low)
            if any(p in low for p in WHY_BOILERPLATE_PATTERNS):
                boilerplate_hits += 1

    if bad_key_player_rows > 0:
        issues.append("key_players_not_normalized")
    if missing_impact_signals > 0:
        issues.append("impact_signals_missing_or_incomplete")

    if why_lines:
        repeats = Counter(why_lines)
        repeated_count = sum(1 for _, c in repeats.items() if c >= 3)
        # Guard against robotic delivery where many lines start with the same phrase.
        starters = Counter()
        for line in why_lines:
            words = line.split()
            if words:
                starters[" ".join(words[:2])] += 1
        dominant_starter = max(starters.values()) if starters else 0
        if (
            boilerplate_hits >= max(2, int(len(why_lines) * 0.2))
            or repeated_count > 0
            or dominant_starter > max(4, int(len(why_lines) * 0.2))
        ):
            issues.append("repetitive_why_it_matters_detected")

    # Matchup quality checks.
    matchup_summaries = [str(m.get("summary", "")).strip().lower() for m in (factpack.get("today_matchups") or [])]
    missing_matchup_context = 0
    matchup_boilerplate_hits = 0
    bad_last10_hits = 0
    repeated_matchup_suffix_hits = 0
    suffix_counter = Counter()
    for s in matchup_summaries:
        if not s:
            matchup_boilerplate_hits += 1
            continue
        if any(p in s for p in MATCHUP_BOILERPLATE_PATTERNS):
            matchup_boilerplate_hits += 1
        if "standings context is pending" in s or "context unavailable" in s or "schedule-confirmed card" in s:
            matchup_boilerplate_hits += 1
        tail = s.split(";")[-1].strip()
        if tail:
            suffix_counter[tail] += 1
        if "last10" in s:
            for a, b in LAST10_RE.findall(s):
                if int(a) + int(b) != 10:
                    bad_last10_hits += 1
    for m in (factpack.get("today_matchups") or []):
        ctx = m.get("matchup_context") if isinstance(m.get("matchup_context"), dict) else {}
        if not ctx:
            missing_matchup_context += 1
            continue
        if not ctx.get("home_last10") or not ctx.get("away_last10"):
            if not ctx.get("home_recent_record") or not ctx.get("away_recent_record"):
                missing_matchup_context += 1
                continue
        hwt = str(ctx.get("home_window_type", "")).strip()
        awt = str(ctx.get("away_window_type", "")).strip()
        if hwt not in {"last10", "season_start_window"} or awt not in {"last10", "season_start_window"}:
            missing_matchup_context += 1
        if str(ctx.get("home_record", "")).strip().upper() in {"", "N/A"} or str(ctx.get("away_record", "")).strip().upper() in {"", "N/A"}:
            missing_matchup_context += 1
    if suffix_counter:
        repeated_matchup_suffix_hits = max(suffix_counter.values())
        if repeated_matchup_suffix_hits > max(2, int(len(matchup_summaries) * 0.2)):
            matchup_boilerplate_hits += 1
    if matchup_boilerplate_hits > 0:
        issues.append("boilerplate_matchup_summaries_detected")
    if bad_last10_hits > 0:
        issues.append("invalid_last10_values_detected")
    if missing_matchup_context > 0:
        issues.append("matchup_context_missing_or_incomplete")

    # Team trends should provide true L10 for in-season sports.
    trend_bad_last10 = 0
    trend_na_record = 0
    for sport, payload in (factpack.get("season_stats") or {}).items():
        if sport not in {"nba", "nhl", "college_basketball", "womens_college_basketball", "wnba"}:
            continue
        for row in (payload or {}).get("teams", []) or []:
            rec = str(row.get("record", "")).strip().upper()
            if rec in {"", "N/A", "NA"}:
                trend_na_record += 1
            if str(row.get("window_type", "")).strip() == "season_start_window":
                recent_games = int(row.get("recent_games", 0) or 0)
                token = str(row.get("recent_record", "")).strip()
                m = LAST10_RE.search(token)
                if recent_games > 0 and m and (int(m.group(1)) + int(m.group(2)) == recent_games):
                    continue
            token = str(row.get("last10", "")).strip()
            m = LAST10_RE.search(token)
            if not m:
                trend_bad_last10 += 1
                continue
            if int(m.group(1)) + int(m.group(2)) != 10:
                trend_bad_last10 += 1
    if trend_bad_last10 > 0:
        issues.append("team_trends_missing_true_last10")
    if trend_na_record > 0:
        issues.append("team_trends_missing_overall_record")

    # News quality checks (activity-aware).
    news = list(factpack.get("major_news") or [])
    yesterday_games_count = len(yesterday)
    news_count = len(news)
    thin_news_hits = 0
    duplicate_news_hits = 0
    gameish_news_hits = 0
    unknown_sport_news_hits = 0
    nfl_draftish_hits = 0
    by_sport = Counter()
    for n in news:
        sport = str(n.get("sport", "")).strip().lower()
        if sport:
            by_sport[sport] += 1
        if sport in {"", "unknown", "multi"}:
            unknown_sport_news_hits += 1
        title = str(n.get("title", "")).strip().lower().rstrip(".")
        summary = str(n.get("summary", "")).strip().lower()
        raw_summary = str(n.get("summary", "")).strip()
        fact_points = [str(x).strip() for x in (n.get("fact_points") or []) if str(x).strip()]
        if any(p in f"{title} {summary}" for p in NEWS_GAMEISH_PATTERNS):
            gameish_news_hits += 1
        if sport == "nfl" and ("draft" in f"{title} {summary}" or "combine" in f"{title} {summary}"):
            nfl_draftish_hits += 1
        if not summary or len(summary.split()) < 12:
            thin_news_hits += 1
            continue
        if title and summary.rstrip(".") == title:
            thin_news_hits += 1
            continue
        if any(p in summary for p in NEWS_THIN_PATTERNS):
            thin_news_hits += 1
            continue
        if _has_repeated_summary_clause(raw_summary):
            duplicate_news_hits += 1
            continue
        if len(fact_points) < 1:
            thin_news_hits += 1
            continue
        # Require concrete, factual framing rather than generic topic teases.
        if not re.search(
            r"\b(trade|combine|draft|injury|contract|free agency|deadline|waived|signed|re-sign|extension|survey|report card|dies|died|death|lawsuit|medal|olympic|suspension|hired|fired)\b",
            summary,
        ):
            thin_news_hits += 1
    # Context-aware enforcement:
    # - Low-activity day (few completed games): require at least one substantive news item.
    # - High-activity day (many completed games): allow thinner news density if game slate carries the show.
    if yesterday_games_count < 5:
        if news_count == 0:
            issues.append("major_news_too_thin")
        elif thin_news_hits >= news_count:
            issues.append("major_news_too_thin")
    else:
        if news_count == 0:
            issues.append("major_news_too_thin")
        elif thin_news_hits > int(news_count * 0.4):
            issues.append("major_news_too_thin")

    # Hard product requirement: minimum 8 daily major-news cards.
    if news_count < 8:
        issues.append("major_news_below_daily_minimum")
    elif thin_news_hits > 0:
        issues.append("major_news_too_thin")
    if duplicate_news_hits > 0:
        issues.append("major_news_duplicate_or_redundant")
    elif thin_news_hits > max(1, int(news_count * 0.25)):
        issues.append("major_news_too_thin")
    if gameish_news_hits > max(1, int(news_count * 0.25)):
        issues.append("major_news_gameish_contamination")
    if unknown_sport_news_hits > 0:
        issues.append("major_news_unknown_sport_labels")
    if by_sport:
        dominant = max(by_sport.values())
        if dominant > int(news_count * 0.8):
            issues.append("major_news_single_sport_overconcentrated")
    if news_count >= 8 and nfl_draftish_hits > int(news_count * 0.7):
        issues.append("major_news_nfl_draft_overconcentrated")

    # During combine window, require at least one combine-specific news item.
    try:
        y, m, d = [int(x) for x in str(run_date).split("-")]
        in_combine_window = (m == 2 and d >= 20) or (m == 3 and d <= 10)
    except Exception:
        in_combine_window = False
    if in_combine_window:
        has_combine = False
        for n in news:
            text = f"{str(n.get('title', ''))} {str(n.get('summary', ''))}".lower()
            if "combine" in text or "pro day" in text:
                has_combine = True
                break
        if not has_combine:
            issues.append("combine_coverage_missing_in_window")

    # Instructional language should not appear in factual payload fields.
    instructional_hits = 0
    for item in yesterday:
        for line in (item.get("why_it_matters") or []):
            low = str(line).strip().lower()
            if any(t in low for t in INSTRUCTIONAL_PATTERNS):
                instructional_hits += 1
    for m in (factpack.get("today_matchups") or []):
        low = str(m.get("summary", "")).strip().lower()
        if any(t in low for t in INSTRUCTIONAL_PATTERNS):
            instructional_hits += 1
    for n in news:
        low = str(n.get("summary", "")).strip().lower()
        if any(t in low for t in INSTRUCTIONAL_PATTERNS):
            instructional_hits += 1
    if instructional_hits > 0:
        issues.append("instructional_language_detected")

    report = {
        "run_date": run_date,
        "approved": len(issues) == 0,
        "issues": issues,
        "citation_coverage": coverage,
        "item_count": total_items,
        "items_with_citation": with_citation,
        "example_citation_hits": example_citation_hits,
        "identity_ok": identity_ok,
        "allocation_share_sum": allocation_share_sum,
        "bad_key_player_rows": bad_key_player_rows,
        "missing_impact_signals": missing_impact_signals,
        "boilerplate_why_hits": boilerplate_hits,
        "boilerplate_matchup_hits": matchup_boilerplate_hits,
        "repeated_matchup_suffix_hits": repeated_matchup_suffix_hits,
        "invalid_last10_hits": bad_last10_hits,
        "missing_matchup_context": missing_matchup_context,
        "thin_news_hits": thin_news_hits,
        "duplicate_news_hits": duplicate_news_hits,
        "instructional_hits": instructional_hits,
        "trend_bad_last10": trend_bad_last10,
        "trend_na_record": trend_na_record,
        "gameish_news_hits": gameish_news_hits,
        "unknown_sport_news_hits": unknown_sport_news_hits,
        "nfl_draftish_hits": nfl_draftish_hits,
    }
    store.put_json(f"factpacks/{run_date}/verification.json", report)
    return report
