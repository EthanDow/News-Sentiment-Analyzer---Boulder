#!/usr/bin/env python3
import logging
import os

from flask import Flask, jsonify, render_template, request

from .analyzer import analyze_pending
from .collector import fetch_headlines
from .db import daily_sentiment, get_conn, init_db, list_topics, recent_articles
from .scheduler import TOPICS, run_cycle, start_scheduler

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(__name__)

app = Flask(__name__)

init_db()

if os.environ.get("WERKZEUG_RUN_MAIN") != "false":
    try:
        start_scheduler()
        run_cycle()
    except Exception:
        log.exception("startup: failed to seed initial data")


@app.route("/")
def dashboard():
    selected = request.args.get("topic") or (TOPICS[0] if TOPICS else "")
    days = int(request.args.get("days", 14))
    with get_conn() as conn:
        topics = list_topics(conn) or list(TOPICS)
        if selected not in topics and topics:
            selected = topics[0]
        articles = recent_articles(conn, selected, limit=25) if selected else []
    return render_template(
        "dashboard.html",
        topics=topics,
        selected=selected,
        days=days,
        articles=articles,
    )


@app.route("/api/sentiments")
def api_sentiments():
    topic = request.args.get("topic", "")
    days = int(request.args.get("days", 14))
    with get_conn() as conn:
        rows = daily_sentiment(conn, topic, days)
    return jsonify(
        {
            "topic": topic,
            "days": days,
            "points": [
                {"day": r["day"], "avg_score": r["avg_score"], "count": r["n"]}
                for r in rows
            ],
        }
    )


@app.route("/collect", methods=["POST"])
def collect():
    topic = request.args.get("topic", "").strip()
    if not topic:
        return jsonify({"error": "missing topic"}), 400
    inserted = fetch_headlines(topic)
    analyzed = analyze_pending()
    return jsonify({"topic": topic, "inserted": inserted, "analyzed": analyzed})


@app.route("/health")
def health():
    return {"status": "ok"}
