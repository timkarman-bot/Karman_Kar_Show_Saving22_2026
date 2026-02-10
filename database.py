import os
import sqlite3
import secrets
from typing import Any, Dict, List, Optional, Tuple

DB_PATH = os.getenv("DB_PATH", "app.db")


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _conn()
    cur = conn.cursor()

    # Shows (configurable; one can be active)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            date TEXT,
            time TEXT,
            location_name TEXT,
            address TEXT,
            benefiting TEXT,
            suggested_donation TEXT,
            description TEXT,
            voting_open INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # People (registration contacts)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT NOT NULL,
            opt_in_future INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # Cars registered for a specific show (car number is per-show)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS show_cars (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            person_id INTEGER NOT NULL,
            car_number INTEGER NOT NULL,
            car_token TEXT NOT NULL UNIQUE,
            year TEXT NOT NULL,
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(person_id) REFERENCES people(id),
            UNIQUE(show_id, car_number)
        )
    """)

    # Votes recorded after payment
    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            show_car_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            vote_qty INTEGER NOT NULL,
            amount_cents INTEGER NOT NULL,
            stripe_session_id TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(show_car_id) REFERENCES show_cars(id)
        )
    """)

    conn.commit()
    conn.close()


# ----------------------------
# Shows
# ----------------------------
def ensure_default_show(default_show: Dict[str, Any]) -> None:
    """
    Ensures a default show exists (safe to run on every startup).
    If no active show exists, sets this one active.
    """
    conn = _conn()
    cur = conn.cursor()

    # Insert if missing
    cur.execute("SELECT id FROM shows WHERE slug = ?", (default_show["slug"],))
    row = cur.fetchone()
    if not row:
        cur.execute("""
            INSERT INTO shows (
                slug, title, date, time, location_name, address,
                benefiting, suggested_donation, description, voting_open, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
        """, (
            default_show["slug"],
            default_show["title"],
            default_show.get("date"),
            default_show.get("time"),
            default_show.get("location_name"),
            default_show.get("address"),
            default_show.get("benefiting"),
            default_show.get("suggested_donation"),
            default_show.get("description"),
        ))

    # If no active show, set default active
    cur.execute("SELECT id FROM shows WHERE is_active = 1 LIMIT 1")
    active = cur.fetchone()
    if not active:
        cur.execute("UPDATE shows SET is_active = 0")
        cur.execute("UPDATE shows SET is_active = 1 WHERE slug = ?", (default_show["slug"],))

    conn.commit()
    conn.close()


def get_active_show() -> Optional[sqlite3.Row]:
    conn = _conn()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM shows WHERE is_active = 1 LIMIT 1").fetchone()
    conn.close()
    return row


def get_show_by_slug(slug: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    cur = conn.cursor()
    row = cur.execute("SELECT * FROM shows WHERE slug = ? LIMIT 1", (slug,)).fetchone()
    conn.close()
    return row


def set_show_voting_open(show_id: int, voting_open: bool) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE shows SET voting_open = ? WHERE id = ?", (1 if voting_open else 0, show_id))
    conn.commit()
    conn.close()


def toggle_show_voting(show_id: int) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("UPDATE shows SET voting_open = CASE voting_open WHEN 1 THEN 0 ELSE 1 END WHERE id = ?", (show_id,))
    conn.commit()
    conn.close()


# ----------------------------
# Registration
# ----------------------------
def create_person(name: str, phone: str, email: str, opt_in_future: bool) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO people (name, phone, email, opt_in_future)
        VALUES (?, ?, ?, ?)
    """, (name, phone, email, 1 if opt_in_future else 0))
    conn.commit()
    pid = int(cur.lastrowid)
    conn.close()
    return pid


def _new_car_token() -> str:
    # URL-safe token for QR; stable for the car in that show
    return secrets.token_urlsafe(12)


def create_show_car(
    show_id: int,
    person_id: int,
    car_number: int,
    year: str,
    make: str,
    model: str,
) -> Tuple[int, str]:
    """
    Returns (show_car_id, car_token).
    Ensures car_number is unique per show.
    """
    conn = _conn()
    cur = conn.cursor()

    # Generate token and insert
    token = _new_car_token()
    try:
        cur.execute("""
            INSERT INTO show_cars (show_id, person_id, car_number, car_token, year, make, model)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (show_id, person_id, car_number, token, year, make, model))
        conn.commit()
        scid = int(cur.lastrowid)
        conn.close()
        return scid, token
    except sqlite3.IntegrityError as e:
        conn.close()
        raise ValueError("That car number is already registered for this show.") from e


def get_show_car_by_token(show_id: int, car_token: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    cur = conn.cursor()
    row = cur.execute("""
        SELECT sc.*, p.name as owner_name, p.phone as owner_phone, p.email as owner_email, p.opt_in_future
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ? AND sc.car_token = ?
        LIMIT 1
    """, (show_id, car_token)).fetchone()
    conn.close()
    return row


# ----------------------------
# Voting
# ----------------------------
def record_paid_votes(
    show_id: int,
    show_car_id: int,
    category: str,
    vote_qty: int,
    amount_cents: int,
    stripe_session_id: str
) -> None:
    """
    Records one Stripe session as one row (vote_qty can be > 1).
    stripe_session_id UNIQUE prevents double counting.
    """
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO votes (show_id, show_car_id, category, vote_qty, amount_cents, stripe_session_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (show_id, show_car_id, category, vote_qty, amount_cents, stripe_session_id))
        conn.commit()
    except sqlite3.IntegrityError:
        # already recorded
        pass
    finally:
        conn.close()


def reset_votes_for_show(show_id: int) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM votes WHERE show_id = ?", (show_id,))
    conn.commit()
    conn.close()


def export_votes_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT
            v.created_at,
            v.category,
            v.vote_qty,
            v.amount_cents,
            v.stripe_session_id,
            sc.car_number,
            sc.year,
            sc.make,
            sc.model,
            p.name as owner_name,
            p.phone as owner_phone,
            p.email as owner_email,
            p.opt_in_future
        FROM votes v
        JOIN show_cars sc ON sc.id = v.show_car_id
        JOIN people p ON p.id = sc.person_id
        WHERE v.show_id = ?
        ORDER BY v.created_at ASC
    """, (show_id,)).fetchall()
    conn.close()
    return rows


def leaderboard_by_category(show_id: int) -> Dict[str, List[Tuple[int, int]]]:
    """
    Returns {category: [(car_number, total_votes), ...]} sorted desc.
    """
    conn = _conn()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT v.category, sc.car_number, SUM(v.vote_qty) as total_votes
        FROM votes v
        JOIN show_cars sc ON sc.id = v.show_car_id
        WHERE v.show_id = ?
        GROUP BY v.category, sc.car_number
        ORDER BY v.category ASC, total_votes DESC, sc.car_number ASC
    """, (show_id,)).fetchall()
    conn.close()

    out: Dict[str, List[Tuple[int, int]]] = {}
    for r in rows:
        out.setdefault(r["category"], [])
        out[r["category"]].append((int(r["car_number"]), int(r["total_votes"] or 0)))
    return out


def leaderboard_overall(show_id: int) -> List[Tuple[int, int]]:
    """
    Returns [(car_number, total_votes_all_categories), ...] sorted desc.
    """
    conn = _conn()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT sc.car_number, SUM(v.vote_qty) as total_votes
        FROM votes v
        JOIN show_cars sc ON sc.id = v.show_car_id
        WHERE v.show_id = ?
        GROUP BY sc.car_number
        ORDER BY total_votes DESC, sc.car_number ASC
    """, (show_id,)).fetchall()
    conn.close()
    return [(int(r["car_number"]), int(r["total_votes"] or 0)) for r in rows]
