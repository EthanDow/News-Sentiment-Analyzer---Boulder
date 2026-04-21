import logging
from datetime import datetime, timezone
from urllib.parse import quote_plus

import feedparser
import requests

from .db import get_conn, insert_article

log = logging.getLogger(__name__)

RSS_TEMPLATE = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"


def _parse_published(entry):
    if getattr(entry, "published_parsed", None):
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()


def fetch_headlines(topic):
    url = RSS_TEMPLATE.format(query=quote_plus(topic))
    try:
        resp = requests.get(url, headers={"User-Agent": UA, "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8"}, timeout=15)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception:
        log.exception("collector: HTTP fetch failed for topic=%s", topic)
        return 0
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    with get_conn() as conn:
        for entry in feed.entries:
            source = entry.get("source", {}).get("title") if isinstance(entry.get("source"), dict) else None
            inserted += insert_article(
                conn,
                topic=topic,
                title=entry.get("title", ""),
                summary=entry.get("summary", ""),
                url=entry.get("link", ""),
                source=source,
                published_at=_parse_published(entry),
                collected_at=now,
            )
    log.info("collector: topic=%s fetched=%d new=%d", topic, len(feed.entries), inserted)
    return inserted


def collect_all_topics(topics):
    return {t: fetch_headlines(t) for t in topics}
