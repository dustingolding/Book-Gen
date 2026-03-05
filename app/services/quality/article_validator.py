from __future__ import annotations

import re
from typing import Any

BANNED_PHRASES = {
    "standings pressure",
    "from an editorial standpoint",
    "keep this in the lead",
    "what to watch",
    "setting the tone",
    "statement win",
    "game 1",
    "why it matters",
    "the reason this item leads the file",
    "the value in ",
    "a secondary pressure point sits nearby",
    "another useful supporting line came from",
    "point environment",
    "point swing",
    "set the pace for the",
    "the useful read comes from placing that score",
    "this piece starts with the lead result",
    "the larger signal sits in the form line",
    "the larger signal is the result itself",
    "the shape of the game matters almost as much as the final score",
    "another live storyline was",
    "another live thread is",
    "confirmation pressure rather than as a clean narrative reset",
    "the supporting context matters too",
    "the immediate nba takeaway is",
    "matters in nfl terms because",
    "the women’s tournament implication is",
    "that is the difference between a recap and an article",
    "nightly rotation debate",
    "controlled the nba texture",
    "in nba terms, that matters because",
    "the useful offseason frame is",
    "lands inside the",
    "the next thing to watch is whether",
    "that is why this article is built around stakes instead of summary",
    "in this part of the women’s season",
    "that matters in women’s college basketball because",
    "the useful question now is",
    "the focus here is the downstream effect",
    "changes the next league cycle",
    "stayed in play alongside the lead story",
    "remained relevant to the night and added a second angle",
    "adds more context here",
    "produced a result strong enough to move the nightly conference conversation",
    "the score is the entry point",
    "which is the kind of shape that changes seeding conversations",
    "the next nba window matters because",
    "sits inside the",
    "the next checkpoint is whether",
    "added another result that strengthens its profile",
}

JUNK_MARKERS = {
    "fantasy baseball",
    "portable cordless",
    "air compressor",
    "promo code",
    "tragic accident",
    "dies at age",
    "shopping",
    "fantasy hockey",
    "mock draft",
    "roundup",
}


def _ngrams(text: str, n: int = 3) -> set[str]:
    words = re.findall(r"[a-z0-9']+", str(text).lower())
    if len(words) < n:
        return {" ".join(words)} if words else set()
    return {" ".join(words[i : i + n]) for i in range(len(words) - n + 1)}


def _similarity(a: str, b: str) -> float:
    ga = _ngrams(a)
    gb = _ngrams(b)
    if not ga or not gb:
        return 0.0
    return len(ga & gb) / max(1, len(ga | gb))


def validate_article(article: dict[str, Any], recent_articles: list[str]) -> list[str]:
    issues: list[str] = []
    body = str(article.get("markdown", ""))
    title = str(article.get("title", ""))
    words = re.findall(r"\b\w+\b", body)
    article_type = str(article.get("article_type", "")).strip()
    min_words = 650 if article_type == "news_focus" else 800
    if len(words) < min_words or len(words) > 1500:
        issues.append("article_word_count_out_of_range")
    low = body.lower()
    if any(token in low for token in BANNED_PHRASES):
        issues.append("article_contains_banned_phrase")
    if "n/a" in low:
        issues.append("article_contains_na_placeholder")
    if any(marker in low for marker in JUNK_MARKERS):
        issues.append("article_contains_unrelated_or_junk_context")
    paragraphs = [p.strip() for p in body.split("\n\n") if p.strip() and not p.strip().startswith("#")]
    if paragraphs:
        numeric = sum(1 for p in paragraphs if re.search(r"\d", p))
        required_numeric_ratio = 0.40 if article_type == "news_focus" else 0.60
        if numeric / max(1, len(paragraphs)) < required_numeric_ratio:
            issues.append("article_sections_light_on_stats")
    if "next" not in low and "upcoming" not in low and "ahead" not in low and "coming" not in low:
        issues.append("article_missing_forward_look")
    if article_type == "top_games":
        title_match = re.search(r":\s+(.+?)\s+vs\.\s+(.+?)\s+on\s+", title)
        first_story_heading = next((line for line in body.splitlines() if line.startswith("## ") and line != "## The Story"), "")
        if title_match and first_story_heading:
            team_a = title_match.group(1).strip().lower()
            team_b = title_match.group(2).strip().lower()
            heading_low = first_story_heading.lower()
            if team_a not in heading_low and team_b not in heading_low:
                issues.append("article_title_lede_story_incoherent")
        non_header_lines = [line.strip() for line in body.splitlines() if line.strip() and not line.startswith("#")]
        lead_paragraph = non_header_lines[0].lower() if non_header_lines else ""
        if title_match and lead_paragraph:
            team_a = title_match.group(1).strip().lower()
            team_b = title_match.group(2).strip().lower()
            if team_a not in lead_paragraph and team_b not in lead_paragraph:
                issues.append("article_title_lede_story_incoherent")
    for match in re.finditer(r"\b[A-Za-z .'-]+\s+beat\s+[A-Za-z .'-]+\s+(\d+)-(\d+)\b", body):
        left = int(match.group(1))
        right = int(match.group(2))
        if left < right:
            issues.append("article_score_orientation_invalid")
            break
        if left == right:
            issues.append("article_score_orientation_invalid")
            break
    if re.search(r"\b(?:mlb|nhl)\b", low):
        if "point environment" in low or "point swing" in low or re.search(r"\b\d+-point\b", low):
            issues.append("article_sport_language_invalid")
    if any(_similarity(body, prev) >= 0.75 for prev in recent_articles[-30:]):
        issues.append("article_too_similar_to_recent_archive")
    return sorted(set(issues))
