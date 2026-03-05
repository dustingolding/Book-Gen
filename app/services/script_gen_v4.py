from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any

import jsonschema

BOILERPLATE_PHRASES = (
    "coverage threshold",
    "confidence threshold",
    "runbook",
    "verified slate",
    "highest-leverage matchups",
    "pivots to watch",
    "underlying trend sample",
    "editorial filter",
)


def _source_ids(item: dict[str, Any]) -> list[str]:
    citations = [str(c).strip() for c in (item.get("citations") or []) if str(c).strip()]
    if citations:
        return citations[:4]
    sid = str(item.get("source_id", "")).strip()
    return [sid] if sid else []


def _clean_text(text: str) -> str:
    txt = re.sub(r"\s+", " ", str(text or "")).strip()
    txt = re.sub(r"\s+([,.;:])", r"\1", txt)
    return txt


def _ensure_terminal_punctuation(text: str) -> str:
    txt = _clean_text(text)
    if not txt:
        return ""
    if txt[-1] not in ".!?":
        txt = f"{txt}."
    return txt


def _site_label(source_id: str) -> str:
    raw = _clean_text(source_id)
    if not raw:
        return ""
    host = re.sub(r"^https?://", "", raw).split("/", 1)[0].lower()
    host = re.sub(r"^www\.", "", host)
    labels = {
        "site.api.espn.com": "ESPN",
        "espn.com": "ESPN",
        "mlbtraderumors.com": "MLB Trade Rumors",
        "mlb.com": "MLB.com",
        "sports.yahoo.com": "Yahoo Sports",
        "cbssports.com": "CBS Sports",
        "apnews.com": "AP",
        "reuters.com": "Reuters",
        "theathletic.com": "The Athletic",
        "nhl.com": "NHL.com",
    }
    return labels.get(host, host)


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w']+\b", str(text or "")))


def _turn_words(turns: list[dict[str, Any]]) -> int:
    return sum(_word_count(str(turn.get("text", ""))) for turn in turns)


def _dedupe_sentences(*parts: str) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        prepared = _ensure_terminal_punctuation(part)
        for sentence in re.split(r"(?<=[.!?])\s+", prepared):
            s = sentence.strip()
            if not s:
                continue
            norm = re.sub(r"[^a-z0-9]+", "", s.lower())
            if norm in seen:
                continue
            seen.add(norm)
            out.append(s)
    return " ".join(out)


def _section(name: str, turns: list[dict[str, Any]], beats: list[dict[str, Any]]) -> dict[str, Any]:
    return {"name": name, "turns": turns, "beats": beats}


def _nonempty_turns(turns: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [turn for turn in turns if _clean_text(str(turn.get("text", "")))]


def _parse_matchup_title(title: str) -> tuple[str, str] | None:
    raw = re.sub(r"^Upcoming:\s*", "", str(title or "").strip(), flags=re.IGNORECASE)
    m = re.match(r"^(.*?)\s+at\s+(.*)$", raw, flags=re.IGNORECASE)
    if not m:
        return None
    away = m.group(1).strip()
    home = m.group(2).strip()
    if not away or not home:
        return None
    return away, home


def _result_sentence(item: dict[str, Any]) -> str:
    title = str(item.get("title", "")).strip()
    pair = _parse_matchup_title(title)
    impact = item.get("impact_signals") if isinstance(item.get("impact_signals"), dict) else {}
    winner_score = impact.get("winner_score")
    loser_score = impact.get("loser_score")
    margin = impact.get("margin")
    if pair and isinstance(winner_score, int) and isinstance(loser_score, int):
        away, home = pair
        return f"{away} at {home} finished {winner_score}-{loser_score}, a {int(margin or abs(winner_score - loser_score))}-point decision."
    return _clean_text(str(item.get("summary", "")))


def _player_sentence(item: dict[str, Any], limit: int = 2) -> str:
    players = [p for p in (item.get("key_players") or []) if isinstance(p, dict)]
    lines = [str(p.get("line", "")).strip() for p in players if str(p.get("line", "")).strip()][:limit]
    if not lines:
        return ""
    if len(lines) == 1:
        return f"The main box-score driver was {lines[0]}."
    return f"The top stat lines were {lines[0]}; {lines[1]}."


def _supporting_fact_sentence(item: dict[str, Any], start_index: int = 1) -> str:
    why = [str(x).strip() for x in (item.get("why_it_matters") or []) if str(x).strip()]
    fact_points = [str(x).strip() for x in (item.get("fact_points") or []) if str(x).strip()]
    for candidate in why[start_index:]:
        if candidate:
            return candidate
    for candidate in fact_points[start_index:]:
        if candidate:
            return candidate
    return ""


def _impact_sentence(item: dict[str, Any]) -> str:
    impact = item.get("impact_signals") if isinstance(item.get("impact_signals"), dict) else {}
    margin = impact.get("margin")
    total = impact.get("total_points")
    top_players = [str(x).strip() for x in (impact.get("top_players") or []) if str(x).strip()][:2]
    why = [str(x).strip() for x in (item.get("why_it_matters") or []) if str(x).strip()]
    title = str(item.get("title", "")).strip()
    parts: list[str] = []
    if isinstance(margin, int) and isinstance(total, int):
        profile = "high-scoring" if total >= 220 else "moderate-scoring" if total >= 190 else "lower-scoring"
        if title:
            parts.append(f"{title} produced {total} total points and finished with a {margin}-point margin in a {profile} game.")
        else:
            parts.append(f"It produced {total} total points and finished with a {margin}-point margin in a {profile} game.")
    if top_players:
        parts.append(f"The lead performance came from {', '.join(top_players)}.")
    if why:
        parts.append(why[0])
    return _dedupe_sentences(*parts)


def _score_context_sentence(item: dict[str, Any]) -> str:
    players = [p for p in (item.get("key_players") or []) if isinstance(p, dict)]
    impact = item.get("impact_signals") if isinstance(item.get("impact_signals"), dict) else {}
    phase = str(impact.get("phase", "")).strip().replace("_", " ").lower()
    title = str(item.get("title", "")).strip()
    third_line = ""
    if len(players) >= 3:
        third_line = str(players[2].get("line", "")).strip()
    parts: list[str] = []
    if third_line:
        parts.append(f"A supporting stat line came from {third_line}.")
    if phase in {"postseason", "championship window"}:
        if title:
            parts.append(f"{title} lands in postseason play, so the margin and rotation profile matter more than raw volume.")
        else:
            parts.append("This lands in postseason play, so the margin and rotation profile matter more than raw volume.")
    return _dedupe_sentences(*parts)


def _score_followup_sentence(item: dict[str, Any]) -> str:
    players = [p for p in (item.get("key_players") or []) if isinstance(p, dict)]
    why = [str(x).strip() for x in (item.get("why_it_matters") or []) if str(x).strip()]
    fact = _supporting_fact_sentence(item, start_index=1)
    parts: list[str] = []
    if len(players) >= 3:
        line = str(players[2].get("line", "")).strip()
        if line:
            parts.append(f"The secondary box-score lift came from {line}.")
    if fact:
        parts.append(fact)
    elif len(why) >= 2:
        parts.append(why[1])
    return _dedupe_sentences(*parts)


def _cold_open_turns(host_a: str, host_b: str, scores: list[dict[str, Any]], news: list[dict[str, Any]], matchups: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    beats: list[dict[str, Any]] = []
    turns: list[dict[str, Any]] = []
    top_score = scores[0] if scores else None
    top_news = news[0] if news else None
    top_matchup = matchups[0] if matchups else None
    if top_score:
        text = _dedupe_sentences(_result_sentence(top_score), _player_sentence(top_score, limit=1))
        turns.append({"speaker": host_a, "text": text, "source_ids": _source_ids(top_score)})
        beats.append({"kind": "score", "title": str(top_score.get("title", "")).strip(), "source_ids": _source_ids(top_score)})
        cold_score_context = _dedupe_sentences(
            _impact_sentence(top_score),
            (top_score.get("why_it_matters") or [""])[0],
        )
        if cold_score_context:
            turns.append({"speaker": host_b, "text": cold_score_context, "source_ids": _source_ids(top_score)})
    if top_news:
        summary = _clean_text(str(top_news.get("summary", "")))
        fact_points = [str(x).strip() for x in (top_news.get("fact_points") or []) if str(x).strip()]
        text = _dedupe_sentences(summary, fact_points[0] if fact_points else "")
        turns.append({"speaker": host_a if top_score else host_b, "text": text, "source_ids": _source_ids(top_news)})
        beats.append({"kind": "news", "title": str(top_news.get("title", "")).strip(), "source_ids": _source_ids(top_news)})
        news_context = _dedupe_sentences(_news_source_sentence(top_news), _supporting_fact_sentence(top_news, start_index=2))
        if news_context:
            turns.append({"speaker": host_b if top_score else host_a, "text": news_context, "source_ids": _source_ids(top_news)})
    if top_matchup:
        preview = _dedupe_sentences(_matchup_preview_sentence(top_matchup), _matchup_context_sentence(top_matchup))
        turns.append({"speaker": host_b, "text": preview, "source_ids": _source_ids(top_matchup)})
        beats.append({"kind": "matchup", "title": str(top_matchup.get("title", "")).strip(), "source_ids": _source_ids(top_matchup)})
        matchup_numbers = _matchup_numbers_sentence(top_matchup)
        if matchup_numbers:
            turns.append({"speaker": host_a, "text": matchup_numbers, "source_ids": _source_ids(top_matchup)})
    return turns, beats


def _used_titles(beats: list[dict[str, Any]], kind: str) -> set[str]:
    return {
        str(beat.get("title", "")).strip()
        for beat in beats
        if str(beat.get("kind", "")).strip() == kind and str(beat.get("title", "")).strip()
    }


def _news_source_sentence(item: dict[str, Any]) -> str:
    source_ids = _source_ids(item)
    source_label = _site_label(source_ids[0]) if source_ids else ""
    priority = str(item.get("priority_reason", "")).strip().replace("_", " ")
    title = str(item.get("title", "")).strip()
    fact_points = [str(x).strip() for x in (item.get("fact_points") or []) if str(x).strip()]
    parts: list[str] = []
    if priority and source_label:
        parts.append(f"This card sits in the {priority} lane, and the reporting here is from {source_label}.")
    elif source_label:
        parts.append(f"The reporting here is from {source_label}.")
    if title and source_label:
        if priority:
            parts.append(f"{source_label}'s reporting keeps this {priority} card centered on {title}.")
        else:
            parts.append(f"{source_label}'s reporting keeps the focus on {title}.")
    return _dedupe_sentences(*parts)


def _news_extra_fact_sentence(item: dict[str, Any]) -> str:
    fact_points = [str(x).strip() for x in (item.get("fact_points") or []) if str(x).strip()]
    return _dedupe_sentences(*fact_points[2:5])


def _news_turn_pair(item: dict[str, Any], host_a: str, host_b: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    summary = _clean_text(str(item.get("summary", "")))
    fact_points = [str(x).strip() for x in (item.get("fact_points") or []) if str(x).strip()]
    title = str(item.get("title", "")).strip()
    sport = str(item.get("sport", "")).strip().upper()
    title_norm = re.sub(r"[^a-z0-9]+", "", title.lower())
    summary_norm = re.sub(r"[^a-z0-9]+", "", summary.lower())
    if title_norm and title_norm in summary_norm:
        lead = f"{sport}: {summary}"
    else:
        lead = _dedupe_sentences(f"{sport}: {title}.", summary)
    support = _dedupe_sentences(*fact_points[:2])
    turns = [{"speaker": host_a, "text": lead, "source_ids": _source_ids(item)}]
    if support:
        turns.append({"speaker": host_b, "text": support, "source_ids": _source_ids(item)})
    trailing = _news_source_sentence(item)
    if trailing:
        turns.append({"speaker": host_a, "text": trailing, "source_ids": _source_ids(item)})
    extra = _news_extra_fact_sentence(item)
    if extra:
        turns.append({"speaker": host_b, "text": extra, "source_ids": _source_ids(item)})
    beat = {"kind": "news", "title": title, "source_ids": _source_ids(item), "sport": sport}
    return _nonempty_turns(turns), beat


def _score_turn_pair(item: dict[str, Any], host_a: str, host_b: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    turns = [
        {"speaker": host_a, "text": _dedupe_sentences(_result_sentence(item), _player_sentence(item)), "source_ids": _source_ids(item)},
        {"speaker": host_b, "text": _impact_sentence(item), "source_ids": _source_ids(item)},
    ]
    context = _score_context_sentence(item)
    if context:
        turns.append({"speaker": host_a, "text": context, "source_ids": _source_ids(item)})
    followup = _score_followup_sentence(item)
    if followup:
        turns.append({"speaker": host_b, "text": followup, "source_ids": _source_ids(item)})
    beat = {"kind": "score", "title": str(item.get("title", "")).strip(), "source_ids": _source_ids(item), "sport": str(item.get("sport", "")).strip()}
    return _nonempty_turns(turns), beat


def _matchup_preview_sentence(item: dict[str, Any]) -> str:
    title = re.sub(r"^Upcoming:\s*", "", str(item.get("title", "")).strip(), flags=re.IGNORECASE)
    ctx = item.get("matchup_context") if isinstance(item.get("matchup_context"), dict) else {}
    away = str(ctx.get("away_team", "")).strip() or (title.split(" at ")[0].strip() if " at " in title else title)
    home = str(ctx.get("home_team", "")).strip() or (title.split(" at ")[-1].strip() if " at " in title else "")
    away_record = str(ctx.get("away_record", "")).strip()
    home_record = str(ctx.get("home_record", "")).strip()
    away_recent = str(ctx.get("away_last10", "")).strip() or str(ctx.get("away_recent_record", "")).strip()
    home_recent = str(ctx.get("home_last10", "")).strip() or str(ctx.get("home_recent_record", "")).strip()
    away_streak = str(ctx.get("away_streak", "")).strip()
    home_streak = str(ctx.get("home_streak", "")).strip()
    away_diff = ctx.get("away_point_diff")
    home_diff = ctx.get("home_point_diff")
    summary = [f"{away} ({away_record}) goes to {home} ({home_record})."]
    if away_recent and home_recent:
        summary.append(f"Over the last 10, {away} is {away_recent} and {home} is {home_recent}.")
    if away_streak or home_streak:
        summary.append(f"The current streaks are {away} {away_streak} and {home} {home_streak}.")
    if isinstance(home_diff, (int, float)) and isinstance(away_diff, (int, float)):
        if float(home_diff) > float(away_diff):
            summary.append(f"Recent point differential favors {home}, {float(home_diff):.1f} per game to {away}'s {float(away_diff):.1f}.")
        elif float(away_diff) > float(home_diff):
            summary.append(f"Recent point differential favors {away}, {float(away_diff):.1f} per game to {home}'s {float(home_diff):.1f}.")
        else:
            summary.append(f"Both teams are even on recent point differential at {float(home_diff):.1f} per game.")
    return _dedupe_sentences(*summary)


def _matchup_context_sentence(item: dict[str, Any]) -> str:
    ctx = item.get("matchup_context") if isinstance(item.get("matchup_context"), dict) else {}
    phase = str(ctx.get("phase", "")).strip().upper()
    away_streak = str(ctx.get("away_streak", "")).strip()
    home_streak = str(ctx.get("home_streak", "")).strip()
    away_recent = str(ctx.get("away_recent_record", "")).strip() or str(ctx.get("away_last10", "")).strip()
    home_recent = str(ctx.get("home_recent_record", "")).strip() or str(ctx.get("home_last10", "")).strip()
    away = str(ctx.get("away_team", "")).strip()
    home = str(ctx.get("home_team", "")).strip()
    parts: list[str] = []
    if phase in {"POSTSEASON", "CHAMPIONSHIP_WINDOW"}:
        parts.append("This one sits in postseason context, so rotation depth and late-game leverage matter more than schedule volume.")
    return _dedupe_sentences(*parts)


def _matchup_summary_sentence(item: dict[str, Any]) -> str:
    summary = _clean_text(str(item.get("summary", "")))
    ctx = item.get("matchup_context") if isinstance(item.get("matchup_context"), dict) else {}
    home_diff = ctx.get("home_point_diff")
    away_diff = ctx.get("away_point_diff")
    home = str(ctx.get("home_team", "")).strip()
    away = str(ctx.get("away_team", "")).strip()
    parts: list[str] = []
    if summary:
        parts.append(summary)
    if isinstance(home_diff, (int, float)) and isinstance(away_diff, (int, float)) and home and away:
        edge = home if float(home_diff) > float(away_diff) else away if float(away_diff) > float(home_diff) else ""
        if edge:
            parts.append(f"Recent differential gives the edge to {edge}.")
    return _dedupe_sentences(*parts)


def _matchup_numbers_sentence(item: dict[str, Any]) -> str:
    ctx = item.get("matchup_context") if isinstance(item.get("matchup_context"), dict) else {}
    home = str(ctx.get("home_team", "")).strip()
    away = str(ctx.get("away_team", "")).strip()
    home_record = str(ctx.get("home_record", "")).strip()
    away_record = str(ctx.get("away_record", "")).strip()
    home_diff = ctx.get("home_point_diff")
    away_diff = ctx.get("away_point_diff")
    parts: list[str] = []
    if home and away and home_record and away_record:
        parts.append(f"On the season line, {away} is {away_record} and {home} is {home_record}.")
    if home and away and isinstance(home_diff, (int, float)) and isinstance(away_diff, (int, float)):
        parts.append(f"Recent differential sits at {away} {float(away_diff):.1f} per game versus {home} {float(home_diff):.1f}.")
    return _dedupe_sentences(*parts)


def _matchup_stakes_sentence(item: dict[str, Any]) -> str:
    ctx = item.get("matchup_context") if isinstance(item.get("matchup_context"), dict) else {}
    away = str(ctx.get("away_team", "")).strip()
    home = str(ctx.get("home_team", "")).strip()
    away_recent = str(ctx.get("away_recent_record", "")).strip() or str(ctx.get("away_last10", "")).strip()
    home_recent = str(ctx.get("home_recent_record", "")).strip() or str(ctx.get("home_last10", "")).strip()
    away_streak = str(ctx.get("away_streak", "")).strip()
    home_streak = str(ctx.get("home_streak", "")).strip()
    summary = _clean_text(str(item.get("summary", "")))
    parts: list[str] = []
    if away and away_recent and away_streak:
        parts.append(f"{away} brings a {away_recent} recent run with a current streak of {away_streak}.")
    if home and home_recent and home_streak:
        parts.append(f"{home} counters with a {home_recent} recent run and a {home_streak} streak.")
    if summary:
        parts.append(summary)
    return _dedupe_sentences(*parts)


def _matchup_pressure_sentence(item: dict[str, Any]) -> str:
    ctx = item.get("matchup_context") if isinstance(item.get("matchup_context"), dict) else {}
    away = str(ctx.get("away_team", "")).strip()
    home = str(ctx.get("home_team", "")).strip()
    away_record = str(ctx.get("away_record", "")).strip()
    home_record = str(ctx.get("home_record", "")).strip()
    away_recent_games = int(ctx.get("away_recent_games", 0) or 0)
    home_recent_games = int(ctx.get("home_recent_games", 0) or 0)
    away_streak = str(ctx.get("away_streak", "")).strip()
    home_streak = str(ctx.get("home_streak", "")).strip()
    parts: list[str] = []
    if away and home and away_record and home_record:
        parts.append(f"The full-season board has {away} at {away_record} against {home} at {home_record}.")
    if away and home and away_recent_games and home_recent_games:
        parts.append(f"The recent form sample covers {away_recent_games} games for {away} and {home_recent_games} for {home}.")
    if away and home and away_streak and home_streak:
        parts.append(f"That leaves {away} on {away_streak} against {home} on {home_streak}.")
    return _dedupe_sentences(*parts)


def _matchup_turn_pair(item: dict[str, Any], host_a: str, host_b: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    turns = [
        {"speaker": host_a, "text": _matchup_preview_sentence(item), "source_ids": _source_ids(item)},
        {"speaker": host_b, "text": _matchup_context_sentence(item), "source_ids": _source_ids(item)},
    ]
    trailing = _matchup_summary_sentence(item)
    if trailing:
        turns.append({"speaker": host_a, "text": trailing, "source_ids": _source_ids(item)})
    numbers = _matchup_numbers_sentence(item)
    if numbers:
        turns.append({"speaker": host_b, "text": numbers, "source_ids": _source_ids(item)})
    stakes = _matchup_stakes_sentence(item)
    if stakes:
        turns.append({"speaker": host_a, "text": stakes, "source_ids": _source_ids(item)})
    pressure = _matchup_pressure_sentence(item)
    if pressure:
        turns.append({"speaker": host_b, "text": pressure, "source_ids": _source_ids(item)})
    beat = {"kind": "matchup", "title": str(item.get("title", "")).strip(), "source_ids": _source_ids(item), "sport": str(item.get("sport", "")).strip()}
    return _nonempty_turns(turns), beat


def _allocation_weights(factpack: dict[str, Any]) -> list[str]:
    allocs = [a for a in (factpack.get("coverage", {}).get("allocations") or []) if isinstance(a, dict)]
    ordered = [str(a.get("sport", "")).strip() for a in sorted(allocs, key=lambda a: float(a.get("minutes", 0.0)), reverse=True) if str(a.get("sport", "")).strip()]
    return ordered


def _ordered_by_allocations(items: list[dict[str, Any]], factpack: dict[str, Any]) -> list[dict[str, Any]]:
    sport_order = _allocation_weights(factpack)
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    fallback: list[dict[str, Any]] = []
    for item in items:
        sport = str(item.get("sport", "")).strip()
        if sport:
            buckets[sport].append(item)
        else:
            fallback.append(item)
    ordered: list[dict[str, Any]] = []
    while True:
        progressed = False
        for sport in sport_order:
            if buckets.get(sport):
                ordered.append(buckets[sport].pop(0))
                progressed = True
        if not progressed:
            break
    for sport in sport_order:
        ordered.extend(buckets.get(sport, []))
    for sport, bucket in buckets.items():
        if sport not in sport_order:
            ordered.extend(bucket)
    ordered.extend(fallback)
    return ordered


def _render_pairs(items: list[dict[str, Any]], renderer, host_a: str, host_b: str, budget_words: int, factpack: dict[str, Any], minimum_items: int = 0) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    turns: list[dict[str, Any]] = []
    beats: list[dict[str, Any]] = []
    speakers = [host_a, host_b]
    idx = 0
    running_words = 0
    ordered_items = _ordered_by_allocations(items, factpack)
    for item in ordered_items:
        pair_turns, beat = renderer(item, speakers[idx % 2], speakers[(idx + 1) % 2])
        turns.extend(pair_turns)
        beats.append(beat)
        running_words += _turn_words(pair_turns)
        idx += 1
        if idx >= minimum_items and running_words >= budget_words:
            break
    return turns, beats


def _closing_turns(host_a: str, host_b: str, factpack: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    allocs = [a for a in (factpack.get("coverage", {}).get("allocations") or []) if isinstance(a, dict)]
    leaders = [str(a.get("sport", "")).replace("_", " ") for a in allocs[:3] if str(a.get("sport", "")).strip()]
    summary = f"The priority sports this run were {', '.join(leaders[:3])}." if leaders else "The next run will follow the same verified factpack-first order."
    source_counts = {
        "scores": len(list(factpack.get("yesterday_results", []) or [])),
        "news": len(list(factpack.get("major_news", []) or [])),
        "matchups": len(list(factpack.get("today_matchups", []) or [])),
    }
    turns = [
        {"speaker": host_a, "text": summary, "source_ids": []},
        {"speaker": host_b, "text": f"The compiled board carried {source_counts['scores']} score cards, {source_counts['news']} news cards, and {source_counts['matchups']} preview cards.", "source_ids": []},
        {"speaker": host_a, "text": "The script stayed inside the deterministic, source-linked path all the way through verification.", "source_ids": []},
        {"speaker": host_b, "text": "That keeps the show anchored to the verified board instead of filling transitions with unsupported recall.", "source_ids": []},
        {"speaker": host_a, "text": "That is the verified board for today. Evan Cole and Marcus Reed will pick it up again on the next slate.", "source_ids": []},
    ]
    beats = [{"kind": "closing", "title": "coverage_summary", "source_ids": []}]
    return turns, beats


def build_script(process_date: str, factpack: dict[str, Any]) -> dict[str, Any]:
    identity = factpack.get("show_identity", {})
    show_name = identity.get("name", "SideLine Wire DailyCast")
    hosts = (identity.get("hosts", {}) or {}).get("primary", ["Evan Cole", "Marcus Reed"])
    host_a = hosts[0] if hosts else "Evan Cole"
    host_b = hosts[1] if len(hosts) > 1 else "Marcus Reed"

    scores = list(factpack.get("yesterday_results", []) or [])
    news = list(factpack.get("major_news", []) or [])
    matchups = list(factpack.get("today_matchups", []) or [])
    target_runtime_minutes = float(factpack.get("meta", {}).get("runtime_target_minutes", 40.0))
    wpm_baseline = int(factpack.get("meta", {}).get("wpm_baseline", 145))
    target_words = max(900, int(target_runtime_minutes * wpm_baseline))

    sections: list[dict[str, Any]] = []

    cold_turns, cold_beats = _cold_open_turns(host_a, host_b, scores, news, matchups)
    if cold_turns:
        sections.append(_section("cold_open", cold_turns, cold_beats))
    used_score_titles = _used_titles(cold_beats, "score")
    used_news_titles = _used_titles(cold_beats, "news")
    used_matchup_titles = _used_titles(cold_beats, "matchup")
    if used_score_titles:
        scores = [item for item in scores if str(item.get("title", "")).strip() not in used_score_titles]
    if used_news_titles:
        news = [item for item in news if str(item.get("title", "")).strip() not in used_news_titles]
    if used_matchup_titles:
        matchups = [item for item in matchups if str(item.get("title", "")).strip() not in used_matchup_titles]

    score_turns, score_beats = _render_pairs(
        scores,
        _score_turn_pair,
        host_a,
        host_b,
        budget_words=int(target_words * 0.58),
        factpack=factpack,
        minimum_items=min(14, len(scores)),
    )
    if score_turns:
        sections.append(_section("scoreboard_roundup", score_turns, score_beats))

    news_turns, news_beats = _render_pairs(
        news,
        _news_turn_pair,
        host_a,
        host_b,
        budget_words=int(target_words * 0.30),
        factpack=factpack,
        minimum_items=min(9, len(news)),
    )
    if news_turns:
        sections.append(_section("major_storylines", news_turns, news_beats))

    matchup_turns, matchup_beats = _render_pairs(
        matchups,
        _matchup_turn_pair,
        host_a,
        host_b,
        budget_words=int(target_words * 0.28),
        factpack=factpack,
        minimum_items=min(8, len(matchups)),
    )
    if matchup_turns:
        sections.append(_section("today_matchups", matchup_turns, matchup_beats))

    closing_turns, closing_beats = _closing_turns(host_a, host_b, factpack)
    sections.append(_section("closing", closing_turns, closing_beats))

    script = {
        "meta": {
            "process_date": process_date,
            "show_name": show_name,
            "hosts": hosts,
            "target_runtime_minutes": target_runtime_minutes,
            "wpm_baseline": wpm_baseline,
            "deterministic": True,
            "source_counts": {
                "scores": len(scores),
                "news": len(news),
                "matchups": len(matchups),
            },
        },
        "sections": sections,
    }

    schema = json.loads(open("schemas/script.schema.json", "r", encoding="utf-8").read())
    jsonschema.validate(script, schema)
    return script
