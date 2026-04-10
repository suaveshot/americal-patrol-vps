"""
Call Intelligence — SQLite Database
Schema, connection management, and CRUD helpers.
"""

import json
import sqlite3
from pathlib import Path

from call_intelligence.config import DB_FILE, DATA_DIR

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS calls (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ghl_message_id      TEXT UNIQUE NOT NULL,
    ghl_contact_id      TEXT,
    ghl_conversation_id TEXT,
    ghl_opportunity_id  TEXT,
    direction           TEXT,
    duration_seconds    INTEGER,
    call_status         TEXT,
    caller_phone        TEXT,
    contact_name        TEXT,
    company_name        TEXT,
    call_timestamp      TEXT,
    recording_path      TEXT,
    created_at          TEXT
);

CREATE TABLE IF NOT EXISTS transcripts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id         INTEGER REFERENCES calls(id) ON DELETE CASCADE,
    full_transcript TEXT,
    source          TEXT,
    word_count      INTEGER,
    transcribed_at  TEXT
);

CREATE TABLE IF NOT EXISTS call_scores (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id                     INTEGER REFERENCES calls(id) ON DELETE CASCADE,
    composite_score             REAL,
    talk_listen_ratio           REAL,
    question_count              INTEGER,
    longest_monologue_seconds   INTEGER,
    conversation_switches       INTEGER,
    filler_word_count           INTEGER,
    filler_words_per_minute     REAL,
    next_steps_defined          INTEGER,
    discovery_completeness      REAL,
    sentiment_start             REAL,
    sentiment_end               REAL,
    sentiment_trajectory        TEXT,
    scored_at                   TEXT
);

CREATE TABLE IF NOT EXISTS call_analysis (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    call_id             INTEGER REFERENCES calls(id) ON DELETE CASCADE,
    call_type           TEXT,
    methodology_detected TEXT,
    questions_asked     TEXT,
    objections_raised   TEXT,
    objection_responses TEXT,
    techniques_used     TEXT,
    competitor_mentions TEXT,
    buying_signals      TEXT,
    disinterest_signals TEXT,
    key_topics          TEXT,
    outcome_prediction  TEXT,
    coachable_moments   TEXT,
    analyzed_at         TEXT
);

CREATE TABLE IF NOT EXISTS deals (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    ghl_opportunity_id  TEXT UNIQUE,
    ghl_contact_id      TEXT,
    contact_name        TEXT,
    company_name        TEXT,
    deal_value          REAL,
    deal_type           TEXT,
    pipeline_stage      TEXT,
    outcome             TEXT,
    won_at              TEXT,
    lost_at             TEXT,
    loss_reason         TEXT,
    total_calls         INTEGER DEFAULT 0,
    total_call_minutes  INTEGER DEFAULT 0,
    avg_call_score      REAL,
    synced_at           TEXT
);

CREATE TABLE IF NOT EXISTS analysis_reports (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    report_type     TEXT,
    report_date     TEXT,
    report_data     TEXT,
    generated_at    TEXT
);

CREATE TABLE IF NOT EXISTS battle_cards (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    competitor_name         TEXT,
    mention_count           INTEGER DEFAULT 0,
    win_rate_when_mentioned REAL,
    best_responses          TEXT,
    contexts                TEXT,
    updated_at              TEXT
);

CREATE INDEX IF NOT EXISTS idx_calls_contact    ON calls(ghl_contact_id);
CREATE INDEX IF NOT EXISTS idx_calls_timestamp  ON calls(call_timestamp);
CREATE INDEX IF NOT EXISTS idx_deals_contact    ON deals(ghl_contact_id);
CREATE INDEX IF NOT EXISTS idx_deals_outcome    ON deals(outcome);
CREATE INDEX IF NOT EXISTS idx_analysis_type    ON call_analysis(call_type);
CREATE INDEX IF NOT EXISTS idx_scores_composite ON call_scores(composite_score);
"""


def get_connection(db_path=None) -> sqlite3.Connection:
    """Open (or create) the calls database. Returns a connection with WAL mode."""
    path = Path(db_path) if db_path else DB_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn


# ── Calls ────────────────────────────────────────────────────────

def get_call_by_message_id(conn, ghl_message_id: str):
    """Return call row or None."""
    cur = conn.execute(
        "SELECT * FROM calls WHERE ghl_message_id = ?", (ghl_message_id,)
    )
    return cur.fetchone()


def insert_call(conn, **kwargs) -> int:
    """Insert a call record. Returns the new row id."""
    cols = [
        "ghl_message_id", "ghl_contact_id", "ghl_conversation_id",
        "ghl_opportunity_id", "direction", "duration_seconds", "call_status",
        "caller_phone", "contact_name", "company_name", "call_timestamp",
        "recording_path", "created_at",
    ]
    vals = [kwargs.get(c) for c in cols]
    placeholders = ", ".join("?" for _ in cols)
    col_str = ", ".join(cols)
    cur = conn.execute(
        f"INSERT OR IGNORE INTO calls ({col_str}) VALUES ({placeholders})", vals
    )
    return cur.lastrowid


# ── Transcripts ──────────────────────────────────────────────────

def insert_transcript(conn, *, call_id, full_transcript, source, word_count,
                      transcribed_at) -> int:
    cur = conn.execute(
        "INSERT INTO transcripts (call_id, full_transcript, source, word_count, transcribed_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (call_id, full_transcript, source, word_count, transcribed_at),
    )
    return cur.lastrowid


# ── Call Scores ──────────────────────────────────────────────────

def insert_call_scores(conn, *, call_id, scores: dict, scored_at) -> int:
    cur = conn.execute(
        "INSERT INTO call_scores "
        "(call_id, composite_score, talk_listen_ratio, question_count, "
        "longest_monologue_seconds, conversation_switches, filler_word_count, "
        "filler_words_per_minute, next_steps_defined, discovery_completeness, "
        "sentiment_start, sentiment_end, sentiment_trajectory, scored_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            call_id,
            scores.get("composite_score", 0),
            scores.get("talk_listen_ratio"),
            scores.get("question_count"),
            scores.get("longest_monologue_seconds"),
            scores.get("conversation_switches"),
            scores.get("filler_word_count"),
            scores.get("filler_words_per_minute"),
            1 if scores.get("next_steps_defined") else 0,
            scores.get("discovery_completeness"),
            scores.get("sentiment_start"),
            scores.get("sentiment_end"),
            scores.get("sentiment_trajectory"),
            scored_at,
        ),
    )
    return cur.lastrowid


# ── Call Analysis ────────────────────────────────────────────────

def insert_call_analysis(conn, *, call_id, analysis: dict, analyzed_at) -> int:
    def _json(val):
        return json.dumps(val) if isinstance(val, (list, dict)) else val

    cur = conn.execute(
        "INSERT INTO call_analysis "
        "(call_id, call_type, methodology_detected, questions_asked, "
        "objections_raised, objection_responses, techniques_used, "
        "competitor_mentions, buying_signals, disinterest_signals, "
        "key_topics, outcome_prediction, coachable_moments, analyzed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            call_id,
            analysis.get("call_type"),
            analysis.get("methodology_detected"),
            _json(analysis.get("questions_asked", [])),
            _json(analysis.get("objections_raised", [])),
            _json(analysis.get("objection_responses", [])),
            _json(analysis.get("techniques_used", [])),
            _json(analysis.get("competitor_mentions", [])),
            _json(analysis.get("buying_signals", [])),
            _json(analysis.get("disinterest_signals", [])),
            _json(analysis.get("key_topics", [])),
            analysis.get("outcome_prediction"),
            _json(analysis.get("coachable_moments", [])),
            analyzed_at,
        ),
    )
    return cur.lastrowid


# ── Deals ────────────────────────────────────────────────────────

def upsert_deal(conn, **kwargs) -> int:
    """Insert or update a deal by ghl_opportunity_id."""
    cur = conn.execute(
        "INSERT INTO deals "
        "(ghl_opportunity_id, ghl_contact_id, contact_name, company_name, "
        "deal_value, deal_type, pipeline_stage, outcome, won_at, lost_at, "
        "loss_reason, synced_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(ghl_opportunity_id) DO UPDATE SET "
        "pipeline_stage=excluded.pipeline_stage, outcome=excluded.outcome, "
        "deal_value=excluded.deal_value, won_at=excluded.won_at, "
        "lost_at=excluded.lost_at, loss_reason=excluded.loss_reason, "
        "synced_at=excluded.synced_at",
        (
            kwargs.get("ghl_opportunity_id"),
            kwargs.get("ghl_contact_id"),
            kwargs.get("contact_name"),
            kwargs.get("company_name"),
            kwargs.get("deal_value"),
            kwargs.get("deal_type"),
            kwargs.get("pipeline_stage"),
            kwargs.get("outcome"),
            kwargs.get("won_at"),
            kwargs.get("lost_at"),
            kwargs.get("loss_reason"),
            kwargs.get("synced_at"),
        ),
    )
    return cur.lastrowid


def link_calls_to_deal(conn, ghl_contact_id: str, ghl_opportunity_id: str) -> int:
    """Link unlinked calls to a deal by contact ID. Returns rows updated."""
    cur = conn.execute(
        "UPDATE calls SET ghl_opportunity_id = ? "
        "WHERE ghl_contact_id = ? AND ghl_opportunity_id IS NULL",
        (ghl_opportunity_id, ghl_contact_id),
    )
    return cur.rowcount


def recalculate_deal_stats(conn, ghl_opportunity_id: str) -> None:
    """Recalculate aggregate call stats for a deal."""
    row = conn.execute(
        "SELECT COUNT(*) as cnt, "
        "COALESCE(SUM(c.duration_seconds), 0) / 60 as total_min, "
        "AVG(s.composite_score) as avg_score "
        "FROM calls c "
        "LEFT JOIN call_scores s ON c.id = s.call_id "
        "WHERE c.ghl_opportunity_id = ?",
        (ghl_opportunity_id,),
    ).fetchone()
    if row:
        conn.execute(
            "UPDATE deals SET total_calls = ?, total_call_minutes = ?, "
            "avg_call_score = ? WHERE ghl_opportunity_id = ?",
            (row["cnt"], row["total_min"], row["avg_score"], ghl_opportunity_id),
        )
