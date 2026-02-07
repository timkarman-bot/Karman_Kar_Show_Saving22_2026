import os
import sqlite3
from typing import Dict

DB_PATH = os.getenv("DB_PATH", "votes.db")


def _conn():
    # check_same_thread=False helps when Flask handles requests across threads
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = _conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            car_id INTEGER NOT NULL,
            branch TEXT NOT NULL,
            vote_amount INTEGER NOT NULL,
            votes INTEGER NOT NULL,
            stripe_session_id TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    def init_settings():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def get_setting(key: str, default: str) -> str:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key: str, value: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
    """, (key, value))
    conn.commit()
    conn.close()

    

    conn.commit()
    conn.close()


def record_vote(car_id: int, branch: str, vote_amount: int, votes: int, stripe_session_id: str):
    """
    Records a completed checkout session as ONE row (with the quantity stored in `votes`).
    Uses stripe_session_id as UNIQUE to prevent double-counting if Stripe retries webhooks.
    """
    conn = _conn()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            INSERT INTO votes (car_id, branch, vote_amount, votes, stripe_session_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (car_id, branch, vote_amount, votes, stripe_session_id),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # Duplicate stripe_session_id: already recorded (safe to ignore)
        pass
    finally:
        conn.close()


def get_totals() -> Dict[str, Dict[str, int]]:
    """
    Returns:
      {
        "Army": { "1": 10, "2": 3, ... },
        "People’s Choice": { "12": 7, ... }
      }
    Keys are strings for easy use in Jinja templates.
    """
    conn = _conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT branch, car_id, SUM(votes) as total_votes
        FROM votes
        GROUP BY branch, car_id
        ORDER BY branch, total_votes DESC
    """)

    totals: Dict[str, Dict[str, int]] = {}
    for branch, car_id, total_votes in cur.fetchall():
        totals.setdefault(branch, {})
        totals[branch][str(car_id)] = int(total_votes or 0)

    conn.close()
    return totals
