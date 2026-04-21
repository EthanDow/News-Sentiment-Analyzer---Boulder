import logging
import re
from datetime import datetime, timezone

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from .db import fetch_unanalyzed, get_conn, insert_sentiment

log = logging.getLogger(__name__)

_analyzer = SentimentIntensityAnalyzer()
_html_tag = re.compile(r"<[^>]+>")


def _label(score):
    if score >= 0.05:
        return "positive"
    if score <= -0.05:
        return "negative"
    return "neutral"


def analyze_pending(batch_size=500):
    now = datetime.now(timezone.utc).isoformat()
    analyzed = 0
    while True:
        with get_conn() as conn:
            rows = fetch_unanalyzed(conn, batch_size)
            if not rows:
                break
            for row in rows:
                text = f"{row['title'] or ''}. {_html_tag.sub('', row['summary'] or '')}"
                score = _analyzer.polarity_scores(text)["compound"]
                insert_sentiment(conn, row["id"], score, _label(score), now)
                analyzed += 1
    log.info("analyzer: analyzed=%d", analyzed)
    return analyzed
