from datetime import date
import logging
from typing import Any

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

LOW_VALUE_DOMAINS = (
    "nypost.com",
    "outkick.com",
    "steelersdepot.com",
    "elevenwarriors.com",
)


class NewsClient:
    def __init__(self) -> None:
        cfg = get_settings()
        self.api_url = (cfg.news_api_url or "").rstrip("/")
        self.api_key = cfg.news_api_key or ""
        self.newsapi_url = (cfg.newsapi_url or "https://newsapi.org/v2").rstrip("/")
        self.newsapi_api_key = cfg.newsapi_api_key or ""
        self.gnews_url = (cfg.gnews_url or "https://gnews.io/api/v4").rstrip("/")
        self.gnews_api_key = cfg.gnews_api_key or ""
        self.espn_site_api_url = (cfg.espn_site_api_url or "").rstrip("/")
        self.allow_synthetic_fallback = cfg.allow_synthetic_fallback

    def _should_use_generic(self) -> bool:
        # Avoid calling generic `/headlines` when URL is actually a provider base URL.
        if not self.api_url:
            return False
        known = ("newsapi.org", "gnews.io", "site.api.espn.com")
        return not any(k in self.api_url for k in known)

    def _fallback_headlines(self, run_date: str) -> list[dict]:
        d = date.fromisoformat(run_date)
        return [
            {
                "id": f"headline-{d.isoformat()}-1",
                "title": "All-Star guard returns from injury ahead of playoff push",
                "summary": "A key conference contender activated its leading guard after a 3-week absence.",
                "published_at": f"{d.isoformat()}T08:00:00Z",
                "source_url": "https://www.espn.com",
            },
            {
                "id": f"headline-{d.isoformat()}-2",
                "title": "Franchise tags expected for top free-agent class",
                "summary": "Multiple front offices expected to use franchise tags before deadline.",
                "published_at": f"{d.isoformat()}T10:30:00Z",
                "source_url": "https://www.espn.com/nfl/",
            },
            {
                "id": f"headline-{d.isoformat()}-3",
                "title": "Ace pitcher sets opening-day milestone",
                "summary": "Veteran starter named opening-day pitcher for the 10th consecutive season.",
                "published_at": f"{d.isoformat()}T12:00:00Z",
                "source_url": "https://www.mlb.com/news",
            },
        ]

    def _headers(self) -> dict:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
            headers["x-api-key"] = self.api_key
        return headers

    @staticmethod
    def _is_sports_relevant(row: dict) -> bool:
        text = f"{row.get('title', '')} {row.get('summary', '')} {row.get('source_url', '')}".lower()
        keywords = (
            "nfl",
            "nba",
            "mlb",
            "nhl",
            "mls",
            "ncaa",
            "football",
            "basketball",
            "baseball",
            "hockey",
            "soccer",
            "olympic",
            "world cup",
            "playoff",
            "combine",
            "scouting combine",
            "nfl draft",
            "draft stock",
            "pro day",
            "trade",
            "coach",
            "matchup",
            "score",
            "athlete",
            "team",
            "game",
        )
        return any(k in text for k in keywords)

    @staticmethod
    def _looks_synthetic(row: dict) -> bool:
        src = str(row.get("source_url") or "")
        return "sportsnews.example" in src or "api.sports.example" in src or ".example/" in src or ".example." in src

    @staticmethod
    def _is_low_value_domain(row: dict) -> bool:
        src = str(row.get("source_url") or "").lower()
        return any(domain in src for domain in LOW_VALUE_DOMAINS)

    @staticmethod
    def _dedupe(rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            if NewsClient._looks_synthetic(row):
                continue
            if NewsClient._is_low_value_domain(row):
                continue
            if not NewsClient._is_sports_relevant(row):
                continue
            row_id = str(row.get("id", "")).strip()
            title = str(row.get("title", "")).strip().lower()
            key = str(row.get("source_url", "")).strip() or title or row_id
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(row)
        return out

    @staticmethod
    def _as_iso(v: Any) -> str:
        s = str(v or "").strip()
        if not s:
            return ""
        return s

    def _fetch_generic_headlines(self, run_date: str) -> list[dict]:
        if not self.api_url:
            return []
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(
                f"{self.api_url}/headlines",
                params={"date": run_date},
                headers=self._headers(),
            )
            resp.raise_for_status()
            payload = resp.json()
        rows = payload.get("headlines", payload)
        if not isinstance(rows, list):
            raise ValueError("news payload must be list-like")
        return rows

    def _fetch_newsapi(self, run_date: str) -> list[dict]:
        if not self.newsapi_api_key:
            return []
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(
                f"{self.newsapi_url}/everything",
                params={
                    "q": '(sports OR "nfl combine" OR "scouting combine" OR "nfl draft" OR "pro day")',
                    "from": run_date,
                    "to": run_date,
                    "language": "en",
                    "sortBy": "publishedAt",
                    "pageSize": 50,
                    "apiKey": self.newsapi_api_key,
                },
            )
            resp.raise_for_status()
            payload = resp.json()
            articles = payload.get("articles", [])
            if not articles:
                # Broaden retrieval if strict single-day query yields no rows.
                resp = client.get(
                    f"{self.newsapi_url}/top-headlines",
                    params={
                        "category": "sports",
                        "language": "en",
                        "pageSize": 50,
                        "apiKey": self.newsapi_api_key,
                    },
                )
                resp.raise_for_status()
                payload = resp.json()
        rows: list[dict] = []
        for idx, a in enumerate(payload.get("articles", []), start=1):
            title = a.get("title")
            if not title:
                continue
            url = a.get("url") or ""
            rows.append(
                {
                    "id": f"newsapi-{run_date}-{idx}",
                    "title": title,
                    "summary": a.get("description") or a.get("content") or "No summary provided.",
                    "published_at": self._as_iso(a.get("publishedAt")),
                    "source_url": url,
                }
            )
        return rows

    def _fetch_gnews(self, run_date: str) -> list[dict]:
        if not self.gnews_api_key:
            return []
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(
                f"{self.gnews_url}/search",
                params={
                    "q": "sports",
                    "from": f"{run_date}T00:00:00Z",
                    "to": f"{run_date}T23:59:59Z",
                    "lang": "en",
                    "max": 50,
                    "token": self.gnews_api_key,
                },
            )
            resp.raise_for_status()
            payload = resp.json()
        rows: list[dict] = []
        for idx, a in enumerate(payload.get("articles", []), start=1):
            title = a.get("title")
            if not title:
                continue
            rows.append(
                {
                    "id": f"gnews-{run_date}-{idx}",
                    "title": title,
                    "summary": a.get("description") or "No summary provided.",
                    "published_at": self._as_iso(a.get("publishedAt")),
                    "source_url": a.get("url") or "",
                }
            )
        return rows

    def _fetch_espn_news(self, run_date: str) -> list[dict]:
        if not self.espn_site_api_url:
            return []
        rows: list[dict] = []
        fallback_rows: list[dict] = []
        sports = ["football/nfl", "basketball/nba", "baseball/mlb", "hockey/nhl"]
        with httpx.Client(timeout=20.0) as client:
            for sport in sports:
                try:
                    resp = client.get(
                        f"{self.espn_site_api_url}/apis/site/v2/sports/{sport}/news",
                        params={"limit": 25},
                    )
                    resp.raise_for_status()
                    payload = resp.json()
                except Exception as exc:
                    logger.info("espn_news_sport_fetch_failed", extra={"sport": sport, "error": str(exc)})
                    continue

                for item in payload.get("articles", []):
                    published_at = self._as_iso(item.get("published"))
                    title = item.get("headline")
                    if not title:
                        continue
                    links = item.get("links", {}).get("web", {})
                    row = {
                        "id": f"espn-news-{sport.replace('/', '-')}-{item.get('id')}",
                        "title": title,
                        "summary": item.get("description") or "No summary provided.",
                        "published_at": published_at,
                        "source_url": links.get("href") or "",
                    }
                    if published_at and published_at.startswith(run_date):
                        rows.append(row)
                    else:
                        fallback_rows.append(row)
        rows = self._dedupe(rows)
        if rows:
            return rows
        # If no same-day stories are available, use freshest available ESPN items.
        return self._dedupe(fallback_rows)[:20]

    def fetch_headlines(self, run_date: str) -> list[dict]:
        rows: list[dict] = []
        if self._should_use_generic():
            try:
                rows.extend(self._fetch_generic_headlines(run_date))
            except Exception as exc:
                logger.info("news_generic_provider_failed", extra={"error": str(exc)})

        try:
            rows.extend(self._fetch_newsapi(run_date))
        except Exception as exc:
            logger.info("newsapi_provider_failed", extra={"error": str(exc)})

        try:
            rows.extend(self._fetch_gnews(run_date))
        except Exception as exc:
            logger.info("gnews_provider_failed", extra={"error": str(exc)})

        try:
            rows.extend(self._fetch_espn_news(run_date))
        except Exception as exc:
            logger.info("espn_news_provider_failed", extra={"error": str(exc)})

        rows = self._dedupe(rows)
        if rows:
            return rows
        if self.allow_synthetic_fallback:
            logger.warning("news_api_fallback_headlines_synthetic", extra={"error": "all providers empty"})
            return self._fallback_headlines(run_date)
        raise RuntimeError("news ingest returned no real provider headlines; synthetic fallback disabled")
