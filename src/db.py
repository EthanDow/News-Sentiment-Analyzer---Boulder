import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.environ.get("NEWS_DB_PATH", "news.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    url TEXT UNIQUE NOT NULL,
    source TEXT,
    published_at TEXT NOT NULL,
    collected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sentiments (
    article_id INTEGER PRIMARY KEY REFERENCES articles(id),
    score REAL NOT NULL,
    label TEXT NOT NULL,
    analyzed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_articles_topic ON articles(topic);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);
"""


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA)


def insert_article(conn, topic, title, summary, url, source, published_at, collected_at):
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO articles (topic, title, summary, url, source, published_at, collected_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (topic, title, summary, url, source, published_at, collected_at),
    )
    return cur.rowcount


def fetch_unanalyzed(conn, limit=100):
    return conn.execute(
        """
        SELECT a.id, a.title, a.summary
        FROM articles a
        LEFT JOIN sentiments s ON s.article_id = a.id
        WHERE s.article_id IS NULL
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def insert_sentiment(conn, article_id, score, label, analyzed_at):
    conn.execute(
        """
        INSERT OR REPLACE INTO sentiments (article_id, score, label, analyzed_at)
        VALUES (?, ?, ?, ?)
        """,
        (article_id, score, label, analyzed_at),
    )


def list_topics(conn):
    rows = conn.execute("SELECT DISTINCT topic FROM articles ORDER BY topic").fetchall()
    return [r["topic"] for r in rows]


def daily_sentiment(conn, topic, days):
    return conn.execute(
        """
        SELECT substr(a.published_at, 1, 10) AS day,
               AVG(s.score) AS avg_score,
               COUNT(*) AS n
        FROM articles a
        JOIN sentiments s ON s.article_id = a.id
        WHERE a.topic = ?
          AND a.published_at >= date('now', ?)
        GROUP BY day
        ORDER BY day
        """,
        (topic, f"-{int(days)} days"),
    ).fetchall()


def recent_articles(conn, topic, limit=25):
    return conn.execute(
        """
        SELECT a.title, a.url, a.source, a.published_at,
               s.score, s.label
        FROM articles a
        LEFT JOIN sentiments s ON s.article_id = a.id
        WHERE a.topic = ?
        ORDER BY a.published_at DESC
        LIMIT ?
        """,
        (topic, limit),
    ).fetchall()
