import logging
from datetime import datetime, timezone

import requests

from .db import get_conn, insert_article

log = logging.getLogger(__name__)

GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def _parse_seendate(s):
    # GDELT format: 20260420T143000Z
    if not s:
        return datetime.now(timezone.utc).isoformat()
    try:
        return datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return datetime.now(timezone.utc).isoformat()


def fetch_headlines(topic):
    params = {
        "query": f'"{topic}" sourcelang:eng',
        "mode": "ArtList",
        "format": "JSON",
        "maxrecords": 75,
        "sort": "DateDesc",
        "timespan": "14d",
    }
    try:
        resp = requests.get(GDELT_URL, params=params, headers={"User-Agent": UA}, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        log.exception("collector: GDELT fetch failed for topic=%s", topic)
        return 0

    articles = data.get("articles", []) or []
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    with get_conn() as conn:
        for a in articles:
            inserted += insert_article(
                conn,
                topic=topic,
                title=a.get("title", "") or "",
                summary="",
                url=a.get("url", "") or "",
                source=a.get("domain", "") or "",
                published_at=_parse_seendate(a.get("seendate")),
                collected_at=now,
            )
    log.info("collector: topic=%s fetched=%d new=%d", topic, len(articles), inserted)
    return inserted


def collect_all_topics(topics):
    return {t: fetch_headlines(t) for t in topics}
