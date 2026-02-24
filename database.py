import os
import sqlite3
import secrets
import io
import csv
import zipfile

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

DB_PATH = os.getenv("DB_PATH", "app.db")


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _conn()
    cur = conn.cursor()

    # Shows
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

    # People (car owners)
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

    # Cars in a show
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

    # Votes
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

    # Sponsors master
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sponsors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            logo_path TEXT,
            website_url TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # Sponsors attached to a show
    cur.execute("""
        CREATE TABLE IF NOT EXISTS show_sponsors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            sponsor_id INTEGER NOT NULL,
            placement TEXT NOT NULL DEFAULT 'standard', -- 'title' or 'standard'
            sort_order INTEGER NOT NULL DEFAULT 100,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(show_id, sponsor_id),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(sponsor_id) REFERENCES sponsors(id)
        )
    """)

    # Attendees (spectators/supporters)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            phone TEXT,
            email TEXT,
            zip TEXT,
            sponsor_opt_in INTEGER NOT NULL DEFAULT 0,
            updates_opt_in INTEGER NOT NULL DEFAULT 0,
            consent_text TEXT,
            consent_version TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
    """)

    # Donations (optional; $0 allowed)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS donations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            attendee_id INTEGER,
            amount_cents INTEGER NOT NULL DEFAULT 0,
            currency TEXT NOT NULL DEFAULT 'USD',
            status TEXT NOT NULL, -- 'skipped' | 'pending' | 'paid' | 'failed'
            stripe_session_id TEXT UNIQUE,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(attendee_id) REFERENCES attendees(id)
        )
    """)

    # Field metrics (to answer "how many skip phone/email")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS field_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,      -- 'phone' | 'email'
            was_provided INTEGER NOT NULL, -- 1/0
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
    """)

    # Paper waiver tracking for cars
    try:
        cur.execute("ALTER TABLE show_cars ADD COLUMN waiver_received INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE show_cars ADD COLUMN waiver_received_at TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE show_cars ADD COLUMN waiver_received_by TEXT")
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()


# ----------------------------
# Shows
# ----------------------------
def ensure_default_show(default_show: Dict[str, Any]) -> None:
    conn = _conn()
    cur = conn.cursor()

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
# Registration / People
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


def update_person(person_id: int, name: str, phone: str, email: str, opt_in_future: bool) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE people
        SET name = ?, phone = ?, email = ?, opt_in_future = ?
        WHERE id = ?
    """, (name, phone, email, 1 if opt_in_future else 0, person_id))
    conn.commit()
    conn.close()


def _new_car_token() -> str:
    return secrets.token_urlsafe(12)


def create_show_car(
    show_id: int,
    person_id: int,
    car_number: int,
    year: str,
    make: str,
    model: str,
) -> Tuple[int, str]:
    conn = _conn()
    cur = conn.cursor()

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


def update_show_car_details(show_car_id: int, year: str, make: str, model: str) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE show_cars
        SET year = ?, make = ?, model = ?
        WHERE id = ?
    """, (year, make, model, show_car_id))
    conn.commit()
    conn.close()


def get_show_car_public_by_token(show_id: int, car_token: str) -> Optional[sqlite3.Row]:
    """
    Public-safe view (NO phone/email).
    """
    conn = _conn()
    cur = conn.cursor()
    row = cur.execute("""
        SELECT
            sc.*,
            p.name as owner_name
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ? AND sc.car_token = ?
        LIMIT 1
    """, (show_id, car_token)).fetchone()
    conn.close()
    return row


def get_show_car_private_by_token(show_id: int, car_token: str) -> Optional[sqlite3.Row]:
    """
    Private view (includes phone/email) for admin/checkin/export.
    """
    conn = _conn()
    cur = conn.cursor()
    row = cur.execute("""
        SELECT
            sc.*,
            p.name as owner_name,
            p.phone as owner_phone,
            p.email as owner_email,
            p.opt_in_future
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ? AND sc.car_token = ?
        LIMIT 1
    """, (show_id, car_token)).fetchone()
    conn.close()
    return row


def get_show_car_by_number(show_id: int, car_number: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    cur = conn.cursor()
    row = cur.execute("""
        SELECT sc.*, p.name as owner_name
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ? AND sc.car_number = ?
        LIMIT 1
    """, (show_id, car_number)).fetchone()
    conn.close()
    return row


def list_show_cars_public(show_id: int) -> List[sqlite3.Row]:
    """
    Used by show page and admin placeholders.
    Includes waiver fields but not phone/email.
    """
    conn = _conn()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT
            sc.id,
            sc.car_number,
            sc.year,
            sc.make,
            sc.model,
            sc.car_token,
            sc.waiver_received,
            sc.waiver_received_at,
            sc.waiver_received_by,
            p.name as owner_name
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ?
        ORDER BY sc.car_number ASC
    """, (show_id,)).fetchall()
    conn.close()
    return rows


# ----------------------------
# Placeholder cars (pre-print)
# ----------------------------
def create_placeholder_cars(show_id: int, start_number: int, count: int) -> int:
    """
    Creates placeholder cars with unique tokens for pre-printing.
    Returns how many were created (skips car_numbers that already exist).
    """
    conn = _conn()
    cur = conn.cursor()

    created = 0
    for n in range(start_number, start_number + count):
        exists = cur.execute(
            "SELECT 1 FROM show_cars WHERE show_id = ? AND car_number = ? LIMIT 1",
            (show_id, n),
        ).fetchone()
        if exists:
            continue

        # placeholder person (FK required; empty strings allowed)
        cur.execute(
            "INSERT INTO people (name, phone, email, opt_in_future) VALUES (?, ?, ?, ?)",
            ("", "", "", 0),
        )
        person_id = int(cur.lastrowid)

        token = _new_car_token()
        cur.execute("""
            INSERT INTO show_cars (show_id, person_id, car_number, car_token, year, make, model)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (show_id, person_id, n, token, "TBD", "TBD", "TBD"))
        created += 1

    conn.commit()
    conn.close()
    return created


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
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO votes (show_id, show_car_id, category, vote_qty, amount_cents, stripe_session_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (show_id, show_car_id, category, vote_qty, amount_cents, stripe_session_id))
        conn.commit()
    except sqlite3.IntegrityError:
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


# ----------------------------
# Sponsors
# ----------------------------
def upsert_sponsor(name: str, logo_path: str = "", website_url: str = "") -> int:
    conn = _conn()
    cur = conn.cursor()

    existing = cur.execute("SELECT id FROM sponsors WHERE name = ? LIMIT 1", (name,)).fetchone()
    if existing:
        cur.execute("""
            UPDATE sponsors SET logo_path = ?, website_url = ?
            WHERE id = ?
        """, (logo_path, website_url, int(existing["id"])))
        conn.commit()
        conn.close()
        return int(existing["id"])

    cur.execute("""
        INSERT INTO sponsors (name, logo_path, website_url)
        VALUES (?, ?, ?)
    """, (name, logo_path, website_url))
    conn.commit()
    sid = int(cur.lastrowid)
    conn.close()
    return sid


def attach_sponsor_to_show(show_id: int, sponsor_id: int, placement: str = "standard", sort_order: int = 100) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO show_sponsors (show_id, sponsor_id, placement, sort_order)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(show_id, sponsor_id) DO UPDATE SET
          placement=excluded.placement,
          sort_order=excluded.sort_order
    """, (show_id, sponsor_id, placement, sort_order))
    conn.commit()
    conn.close()


def remove_sponsor_from_show(show_id: int, sponsor_id: int) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM show_sponsors WHERE show_id = ? AND sponsor_id = ?", (show_id, sponsor_id))
    conn.commit()
    conn.close()


def set_title_sponsor(show_id: int, sponsor_id: int) -> None:
    """
    Ensures only one title sponsor per show by clearing any existing title placements.
    """
    conn = _conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE show_sponsors
        SET placement = 'standard'
        WHERE show_id = ? AND placement = 'title'
    """, (show_id,))

    attach_sponsor_to_show(show_id, sponsor_id, placement="title", sort_order=0)
    conn.commit()
    conn.close()


def get_show_sponsors(show_id: int):
    """
    Returns (title_sponsor, sponsors)
    title_sponsor: dict | None
    sponsors: list[dict]
    """
    conn = _conn()
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT
            s.id as sponsor_id,
            s.name,
            s.logo_path,
            s.website_url,
            ss.placement,
            ss.sort_order
        FROM show_sponsors ss
        JOIN sponsors s ON s.id = ss.sponsor_id
        WHERE ss.show_id = ?
        ORDER BY
            CASE WHEN ss.placement = 'title' THEN 0 ELSE 1 END,
            ss.sort_order ASC,
            s.id ASC
    """, (show_id,)).fetchall()

    conn.close()

    title = None
    sponsors = []
    for r in rows:
        item = {
            "id": int(r["sponsor_id"]),
            "name": r["name"],
            "logo_path": r["logo_path"],
            "website_url": r["website_url"],
            "placement": r["placement"],
            "sort_order": int(r["sort_order"] or 0),
        }
        if item["placement"] == "title" and title is None:
            title = item
        else:
            sponsors.append(item)

    return title, sponsors


# ----------------------------
# Attendees + Donations + Metrics
# ----------------------------
def create_attendee(
    show_id: int,
    first_name: str,
    last_name: str,
    phone: str,
    email: str,
    zip_code: str,
    sponsor_opt_in: bool,
    updates_opt_in: bool,
    consent_text: str,
    consent_version: str,
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO attendees
        (show_id, first_name, last_name, phone, email, zip, sponsor_opt_in, updates_opt_in, consent_text, consent_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        show_id,
        first_name,
        last_name,
        phone or None,
        email or None,
        zip_code or None,
        1 if sponsor_opt_in else 0,
        1 if updates_opt_in else 0,
        consent_text,
        consent_version,
    ))
    conn.commit()
    aid = int(cur.lastrowid)
    conn.close()
    return aid


def record_field_metric(show_id: int, field_name: str, was_provided: bool) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO field_metrics (show_id, field_name, was_provided)
        VALUES (?, ?, ?)
    """, (show_id, field_name, 1 if was_provided else 0))
    conn.commit()
    conn.close()


def create_donation_row(show_id: int, attendee_id: int, amount_cents: int, status: str) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO donations (show_id, attendee_id, amount_cents, status)
        VALUES (?, ?, ?, ?)
    """, (show_id, attendee_id, int(amount_cents), status))
    conn.commit()
    did = int(cur.lastrowid)
    conn.close()
    return did


def attach_stripe_session_to_donation(donation_id: int, stripe_session_id: str) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE donations
        SET stripe_session_id = ?
        WHERE id = ?
    """, (stripe_session_id, donation_id))
    conn.commit()
    conn.close()


def mark_donation_paid(stripe_session_id: str) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE donations
        SET status = 'paid'
        WHERE stripe_session_id = ?
    """, (stripe_session_id,))
    conn.commit()
    conn.close()


# ----------------------------
# Waiver tracking (paper-first)
# ----------------------------
def waiver_mark_received(show_id: int, show_car_id: int, received_by: str) -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE show_cars
        SET waiver_received = 1,
            waiver_received_at = datetime('now'),
            waiver_received_by = ?
        WHERE id = ? AND show_id = ?
    """, (received_by or "staff", show_car_id, show_id))
    conn.commit()
    conn.close()


# ----------------------------
# SNAPSHOT EXPORT (ZIP)
# ----------------------------
def export_show_row(show_id: int):
    conn = _conn()
    cur = conn.cursor()
    row = cur.execute(
        "SELECT * FROM shows WHERE id = ? LIMIT 1",
        (show_id,)
    ).fetchone()
    conn.close()
    return row


def export_people_rows_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT DISTINCT p.*
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ?
        ORDER BY p.created_at ASC
    """, (show_id,)).fetchall()
    conn.close()
    return rows


def export_show_cars_rows(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT
            sc.*,
            p.name as owner_name,
            p.phone as owner_phone,
            p.email as owner_email,
            p.opt_in_future
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ?
        ORDER BY sc.car_number ASC
    """, (show_id,)).fetchall()
    conn.close()
    return rows


def build_snapshot_zip_bytes(show_id: int):
    show = export_show_row(show_id)
    if not show:
        raise ValueError("Show not found")

    cars = export_show_cars_rows(show_id)
    people = export_people_rows_for_show(show_id)
    votes = export_votes_for_show(show_id)

    slug = show["slug"]
    ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    zip_name = f"{slug}-snapshot-{ts}Z.zip"

    mem = io.BytesIO()

    with zipfile.ZipFile(mem, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        # show.csv
        show_buf = io.StringIO()
        sw = csv.writer(show_buf)
        cols = list(show.keys())
        sw.writerow(cols)
        sw.writerow([show[c] for c in cols])
        z.writestr("show.csv", show_buf.getvalue().encode("utf-8"))

        # cars.csv
        cars_buf = io.StringIO()
        cw = csv.writer(cars_buf)
        if cars:
            ccols = list(cars[0].keys())
            cw.writerow(ccols)
            for r in cars:
                cw.writerow([r[c] for c in ccols])
        z.writestr("cars.csv", cars_buf.getvalue().encode("utf-8"))

        # people.csv
        people_buf = io.StringIO()
        pw = csv.writer(people_buf)
        if people:
            pcols = list(people[0].keys())
            pw.writerow(pcols)
            for r in people:
                pw.writerow([r[c] for c in pcols])
        z.writestr("people.csv", people_buf.getvalue().encode("utf-8"))

        # votes.csv
        votes_buf = io.StringIO()
        vw = csv.writer(votes_buf)
        if votes:
            vcols = list(votes[0].keys())
            vw.writerow(vcols)
            for r in votes:
                vw.writerow([r[c] for c in vcols])
        z.writestr("votes.csv", votes_buf.getvalue().encode("utf-8"))

    mem.seek(0)
    return mem.getvalue(), zip_name
