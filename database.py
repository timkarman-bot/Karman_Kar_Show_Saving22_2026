import os
import re
import sqlite3
import secrets
import io
import csv
import zipfile
import hashlib
import json

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from waiver_system import DEFAULT_WAIVER_TEMPLATE, builder_config_to_json, normalize_builder_config

DB_PATH = os.getenv("DB_PATH")
if not DB_PATH:
    DB_PATH = "/data/app.db" if os.path.isdir("/data") else "app.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _b(v: bool) -> int:
    return 1 if v else 0


def _new_token() -> str:
    return secrets.token_urlsafe(18)


def _new_car_token() -> str:
    return secrets.token_urlsafe(12)


def _sha256_text(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _slugify(value: str) -> str:
    value = (value or "").strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    value = value.strip("-")
    return value or "upcoming-show"


def init_db() -> None:
    conn = _conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT NOT NULL UNIQUE,
            flyer_image_path TEXT,
            title TEXT NOT NULL,
            date TEXT,
            time TEXT,
            location_name TEXT,
            cars_arrive_time TEXT,
            day_of_registration_time TEXT,
            show_start_time TEXT,
            show_end_time TEXT,
            map_url TEXT,
            address TEXT,
            benefiting TEXT,
            suggested_donation TEXT,
            description TEXT,
            voting_open INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS waiver_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            version TEXT NOT NULL,
            body_template TEXT NOT NULL,
            preset_key TEXT NOT NULL DEFAULT 'standard',
            builder_config TEXT,
            is_default INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )

    for sql in [
        "ALTER TABLE shows ADD COLUMN show_type TEXT NOT NULL DEFAULT 'full'",
        "ALTER TABLE shows ADD COLUMN allow_prereg_override INTEGER",
        "ALTER TABLE shows ADD COLUMN max_cars INTEGER",
        "ALTER TABLE shows ADD COLUMN use_single_processor INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE shows ADD COLUMN single_processor_target TEXT NOT NULL DEFAULT 'charity'",
        "ALTER TABLE shows ADD COLUMN voting_processor_target TEXT NOT NULL DEFAULT 'charity'",
        "ALTER TABLE shows ADD COLUMN registration_processor_target TEXT NOT NULL DEFAULT 'karman'",
        "ALTER TABLE shows ADD COLUMN donation_processor_target TEXT NOT NULL DEFAULT 'charity'",
        "ALTER TABLE shows ADD COLUMN karman_processor_label TEXT",
        "ALTER TABLE shows ADD COLUMN charity_processor_label TEXT",
        "ALTER TABLE shows ADD COLUMN karman_stripe_secret_key TEXT",
        "ALTER TABLE shows ADD COLUMN charity_stripe_secret_key TEXT",
        "ALTER TABLE shows ADD COLUMN public_vote_disclosure TEXT",
        "ALTER TABLE shows ADD COLUMN public_registration_disclosure TEXT",
        "ALTER TABLE shows ADD COLUMN public_donation_disclosure TEXT",
        "ALTER TABLE shows ADD COLUMN registration_fee_cents INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE shows ADD COLUMN attendee_fee_cents INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE shows ADD COLUMN vote_price_cents INTEGER NOT NULL DEFAULT 100",
        "ALTER TABLE shows ADD COLUMN charity_stripe_account_id TEXT",
        "ALTER TABLE shows ADD COLUMN charity_connect_status TEXT NOT NULL DEFAULT 'not_connected'",
        "ALTER TABLE shows ADD COLUMN charity_connected_at TEXT",
        "ALTER TABLE shows ADD COLUMN charity_connect_email TEXT",
        "ALTER TABLE shows ADD COLUMN waiver_text TEXT",
        "ALTER TABLE shows ADD COLUMN waiver_version TEXT",
        "ALTER TABLE shows ADD COLUMN waiver_template_id INTEGER",
        "ALTER TABLE shows ADD COLUMN organizer_name TEXT",
        "ALTER TABLE shows ADD COLUMN venue_name TEXT",
        "ALTER TABLE shows ADD COLUMN venue_address_line1 TEXT",
        "ALTER TABLE shows ADD COLUMN venue_address_line2 TEXT",
        "ALTER TABLE shows ADD COLUMN venue_city TEXT",
        "ALTER TABLE shows ADD COLUMN venue_state TEXT",
        "ALTER TABLE shows ADD COLUMN venue_zip TEXT",
        "ALTER TABLE shows ADD COLUMN charity_name TEXT",
        "ALTER TABLE shows ADD COLUMN charity_description TEXT",
        "ALTER TABLE shows ADD COLUMN status TEXT NOT NULL DEFAULT 'draft'",
        "ALTER TABLE shows ADD COLUMN short_details TEXT",
        "ALTER TABLE shows ADD COLUMN qr_message TEXT",
        "ALTER TABLE shows ADD COLUMN cta_label TEXT",
        "ALTER TABLE shows ADD COLUMN cta_url TEXT",
        "ALTER TABLE shows ADD COLUMN show_on_site INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE shows ADD COLUMN cars_arrive_time TEXT",
        "ALTER TABLE shows ADD COLUMN day_of_registration_time TEXT",
        "ALTER TABLE shows ADD COLUMN show_start_time TEXT",
        "ALTER TABLE shows ADD COLUMN show_end_time TEXT",
        "ALTER TABLE shows ADD COLUMN map_url TEXT",
        "ALTER TABLE shows ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 100",
        "ALTER TABLE shows ADD COLUMN hide_address INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE shows ADD COLUMN flyer_image_path TEXT",
        "ALTER TABLE shows ADD COLUMN voting_mode TEXT NOT NULL DEFAULT 'fundraiser_unlimited'",
        "ALTER TABLE shows ADD COLUMN payment_mode TEXT NOT NULL DEFAULT 'stripe'",
        "ALTER TABLE shows ADD COLUMN external_payment_url TEXT",
        "ALTER TABLE shows ADD COLUMN allow_custom_votes INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE shows ADD COLUMN preset_vote_options TEXT NOT NULL DEFAULT '1,5,10,20,25'",
        "ALTER TABLE shows ADD COLUMN max_votes_per_checkout INTEGER NOT NULL DEFAULT 50",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    try:
        cur.execute("ALTER TABLE waiver_templates ADD COLUMN preset_key TEXT NOT NULL DEFAULT 'standard'")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE waiver_templates ADD COLUMN builder_config TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT NOT NULL,
            opt_in_future INTEGER NOT NULL DEFAULT 0,
            sponsor_opt_in INTEGER NOT NULL DEFAULT 0,
            consent_text TEXT,
            consent_version TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )

    for sql in [
        "ALTER TABLE people ADD COLUMN sponsor_opt_in INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE people ADD COLUMN consent_text TEXT",
        "ALTER TABLE people ADD COLUMN consent_version TEXT",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
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
        """
    )

    for sql in [
        "ALTER TABLE show_cars ADD COLUMN waiver_received INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE show_cars ADD COLUMN waiver_received_at TEXT",
        "ALTER TABLE show_cars ADD COLUMN waiver_received_by TEXT",
        "ALTER TABLE show_cars ADD COLUMN registration_payment_status TEXT",
        "ALTER TABLE show_cars ADD COLUMN registration_amount_cents INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE show_cars ADD COLUMN registration_session_id TEXT",
        "ALTER TABLE show_cars ADD COLUMN waiver_signed_name TEXT",
        "ALTER TABLE show_cars ADD COLUMN waiver_signed_at TEXT",
        "ALTER TABLE show_cars ADD COLUMN waiver_version TEXT",
        "ALTER TABLE show_cars ADD COLUMN waiver_text TEXT",
        "ALTER TABLE show_cars ADD COLUMN waiver_text_sha256 TEXT",
        "ALTER TABLE show_cars ADD COLUMN waiver_template_id INTEGER",
        "ALTER TABLE show_cars ADD COLUMN is_placeholder INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE show_cars ADD COLUMN registration_state TEXT NOT NULL DEFAULT 'paid'",
        "ALTER TABLE show_cars ADD COLUMN checked_in_at TEXT",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS registration_intents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            intent_token TEXT NOT NULL UNIQUE,
            owner_name TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT NOT NULL,
            opt_in_future INTEGER NOT NULL DEFAULT 0,
            sponsor_opt_in INTEGER NOT NULL DEFAULT 0,
            car_number INTEGER NOT NULL,
            year TEXT NOT NULL,
            make TEXT NOT NULL,
            model TEXT NOT NULL,
            waiver_accepted INTEGER NOT NULL DEFAULT 0,
            waiver_signed_name TEXT NOT NULL,
            waiver_text TEXT,
            waiver_version TEXT,
            waiver_text_sha256 TEXT,
            amount_cents INTEGER NOT NULL DEFAULT 0,
            payment_status TEXT NOT NULL DEFAULT 'pending',
            stripe_session_id TEXT UNIQUE,
            stripe_payment_intent_id TEXT,
            finalized_show_car_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            paid_at TEXT,
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(finalized_show_car_id) REFERENCES show_cars(id)
        )
        """
    )

    for sql in [
        "ALTER TABLE registration_intents ADD COLUMN waiver_text_sha256 TEXT",
        "ALTER TABLE registration_intents ADD COLUMN waiver_template_id INTEGER",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS vote_intents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            show_car_id INTEGER NOT NULL,
            category TEXT NOT NULL,
            vote_qty INTEGER NOT NULL,
            amount_cents INTEGER NOT NULL,
            payment_status TEXT NOT NULL DEFAULT 'pending',
            stripe_session_id TEXT UNIQUE,
            stripe_payment_intent_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            paid_at TEXT,
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(show_car_id) REFERENCES show_cars(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sponsors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            logo_path TEXT,
            website_url TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS show_sponsors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            sponsor_id INTEGER NOT NULL,
            placement TEXT NOT NULL DEFAULT 'standard',
            sort_order INTEGER NOT NULL DEFAULT 100,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(show_id, sponsor_id),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(sponsor_id) REFERENCES sponsors(id)
        )
        """
    )

    cur.execute(
        """
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
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS donations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            attendee_id INTEGER,
            amount_cents INTEGER NOT NULL DEFAULT 0,
            currency TEXT NOT NULL DEFAULT 'USD',
            status TEXT NOT NULL,
            stripe_session_id TEXT UNIQUE,
            stripe_payment_intent_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            paid_at TEXT,
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(attendee_id) REFERENCES attendees(id)
        )
        """
    )

    for sql in [
        "ALTER TABLE donations ADD COLUMN stripe_payment_intent_id TEXT",
        "ALTER TABLE donations ADD COLUMN paid_at TEXT",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS field_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            was_provided INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_webhook_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stripe_event_id TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS waiver_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            registration_intent_id INTEGER,
            show_car_id INTEGER,
            car_number INTEGER,
            owner_name TEXT,
            phone TEXT,
            email TEXT,
            year TEXT,
            make TEXT,
            model TEXT,
            opt_in_future INTEGER NOT NULL DEFAULT 0,
            sponsor_opt_in INTEGER NOT NULL DEFAULT 0,
            waiver_version TEXT,
            waiver_text_sha256 TEXT,
            signed_name TEXT,
            waiver_accepted INTEGER NOT NULL DEFAULT 0,
            intent_token TEXT,
            html_path TEXT,
            request_path TEXT,
            ip_address TEXT,
            user_agent TEXT,
            created_at_utc TEXT NOT NULL,
            created_at_local TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(registration_intent_id) REFERENCES registration_intents(id),
            FOREIGN KEY(show_car_id) REFERENCES show_cars(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER,
            actor_type TEXT NOT NULL,
            action TEXT NOT NULL,
            details_json TEXT,
            ip_address TEXT,
            user_agent TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rate_limit_hits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bucket_key TEXT NOT NULL,
            window_started_at INTEGER NOT NULL,
            hit_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(bucket_key, window_started_at)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS event_interest_signups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER,
            first_name TEXT NOT NULL,
            last_name TEXT,
            email TEXT,
            phone TEXT,
            wants_email INTEGER NOT NULL DEFAULT 0,
            wants_text INTEGER NOT NULL DEFAULT 0,
            source TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )

    row = cur.execute("SELECT id FROM waiver_templates WHERE is_default = 1 LIMIT 1").fetchone()
    if not row:
        cur.execute(
            """
            INSERT INTO waiver_templates (title, version, body_template, preset_key, builder_config, is_default, is_active, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, 1, datetime('now'))
            """,
            (
                "Master Participant Waiver",
                "2026.03.25",
                DEFAULT_WAIVER_TEMPLATE,
                "standard",
                builder_config_to_json(normalize_builder_config({})),
            ),
        )

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_shows_active ON shows(is_active)",
        "CREATE INDEX IF NOT EXISTS idx_shows_status ON shows(status)",
        "CREATE INDEX IF NOT EXISTS idx_show_cars_show_id ON show_cars(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_show_cars_token ON show_cars(car_token)",
        "CREATE INDEX IF NOT EXISTS idx_show_cars_state ON show_cars(show_id, registration_state)",
        "CREATE INDEX IF NOT EXISTS idx_registration_intents_show_id ON registration_intents(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_vote_intents_show_id ON vote_intents(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_votes_show_id ON votes(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_donations_show_id ON donations(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_waiver_evidence_show_id ON waiver_evidence(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_audit_logs_show_id ON audit_logs(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_rate_limit_bucket ON rate_limit_hits(bucket_key, window_started_at)",
        "CREATE INDEX IF NOT EXISTS idx_event_interest_show_id ON event_interest_signups(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_event_interest_created_at ON event_interest_signups(created_at)",
    ]:
        cur.execute(sql)

    conn.commit()
    conn.close()


# SHOWS

def ensure_default_show(default_show: Dict[str, Any]) -> None:
    conn = _conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM shows WHERE slug = ?", (default_show["slug"],))
    row = cur.fetchone()

    if not row:
        cur.execute(
            """
            INSERT INTO shows (
                slug, title, date, time, location_name, address,
                benefiting, suggested_donation, description,
                status, short_details, qr_message, cta_label, cta_url,
                show_on_site, sort_order, hide_address, voting_open, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', '', '', '', '', 1, 100, 0, 0, 0)
            """,
            (
                default_show["slug"],
                default_show["title"],
                default_show.get("date"),
                default_show.get("time"),
                default_show.get("location_name"),
                default_show.get("address"),
                default_show.get("benefiting"),
                default_show.get("suggested_donation"),
                default_show.get("description"),
            ),
        )

    cur.execute("SELECT id FROM shows WHERE is_active = 1 LIMIT 1")
    active = cur.fetchone()
    if not active:
        cur.execute("UPDATE shows SET is_active = 0")
        cur.execute(
            "UPDATE shows SET is_active = 1, status = 'active' WHERE slug = ?",
            (default_show["slug"],),
        )

    conn.commit()
    conn.close()


def get_active_show() -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM shows WHERE is_active = 1 LIMIT 1").fetchone()
    conn.close()
    return row


def get_waiver_template_by_id(waiver_template_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM waiver_templates WHERE id = ? LIMIT 1", (int(waiver_template_id),)).fetchone()
    conn.close()
    return row


def list_waiver_templates() -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM waiver_templates WHERE is_active = 1 ORDER BY is_default DESC, id DESC"
    ).fetchall()
    conn.close()
    return rows


def get_effective_waiver_template_for_show(show_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    show = conn.execute("SELECT waiver_template_id FROM shows WHERE id = ? LIMIT 1", (int(show_id),)).fetchone()
    if show and show["waiver_template_id"]:
        row = conn.execute("SELECT * FROM waiver_templates WHERE id = ? LIMIT 1", (int(show["waiver_template_id"]),)).fetchone()
        conn.close()
        if row:
            return row
        return None
    row = conn.execute("SELECT * FROM waiver_templates WHERE is_default = 1 AND is_active = 1 LIMIT 1").fetchone()
    conn.close()
    return row


def create_waiver_template(
    *,
    title: str,
    version: str,
    body_template: str,
    is_default: bool = False,
    preset_key: str = "standard",
    builder_config: str = "",
) -> int:
    conn = _conn()
    cur = conn.cursor()
    if is_default:
        cur.execute("UPDATE waiver_templates SET is_default = 0")
    cur.execute(
        """
        INSERT INTO waiver_templates (title, version, body_template, preset_key, builder_config, is_default, is_active, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, 1, datetime('now'))
        """,
        (
            (title or "").strip(),
            (version or "").strip(),
            body_template or "",
            (preset_key or "standard").strip() or "standard",
            builder_config or None,
            1 if is_default else 0,
        ),
    )
    conn.commit()
    rid = int(cur.lastrowid)
    conn.close()
    return rid


def update_waiver_template(
    *,
    waiver_template_id: int,
    title: str,
    version: str,
    body_template: str,
    is_default: bool = False,
    preset_key: str = "standard",
    builder_config: str = "",
) -> None:
    conn = _conn()
    cur = conn.cursor()
    if is_default:
        cur.execute("UPDATE waiver_templates SET is_default = 0")
    cur.execute(
        """
        UPDATE waiver_templates
        SET title = ?, version = ?, body_template = ?, preset_key = ?, builder_config = ?, is_default = ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (
            (title or "").strip(),
            (version or "").strip(),
            body_template or "",
            (preset_key or "standard").strip() or "standard",
            builder_config or None,
            1 if is_default else 0,
            int(waiver_template_id),
        ),
    )
    conn.commit()
    conn.close()


def get_show_by_id(show_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM shows WHERE id = ? LIMIT 1", (show_id,)).fetchone()
    conn.close()
    return row


def get_show_by_slug(slug: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM shows WHERE slug = ? LIMIT 1", (slug,)).fetchone()
    conn.close()
    return row


def list_shows_admin() -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT *
        FROM shows
        ORDER BY
            CASE status
                WHEN 'active' THEN 0
                WHEN 'upcoming' THEN 1
                WHEN 'draft' THEN 2
                WHEN 'past' THEN 3
                ELSE 4
            END,
            sort_order ASC,
            date ASC,
            id DESC
        """
    ).fetchall()
    conn.close()
    return rows


def get_next_upcoming_show() -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        """
        SELECT *
        FROM shows
        WHERE status = 'upcoming' AND show_on_site = 1
        ORDER BY sort_order ASC, date ASC, id ASC
        LIMIT 1
        """
    ).fetchone()
    conn.close()
    return row


def create_show_admin(
    *,
    slug: str,
    flyer_image_path: str,
    title: str,
    date: str,
    time: str,
    cars_arrive_time: str = "",
    day_of_registration_time: str = "",
    show_start_time: str = "",
    show_end_time: str = "",
    location_name: str,
    address: str,
    benefiting: str,
    suggested_donation: str,
    description: str,
    status: str,
    short_details: str,
    qr_message: str,
    cta_label: str,
    cta_url: str,
    show_on_site: int,
    sort_order: int,
    hide_address: int = 0,
    waiver_template_id: Optional[int] = None,
    organizer_name: str = "",
    venue_name: str = "",
    venue_address_line1: str = "",
    venue_address_line2: str = "",
    venue_city: str = "",
    venue_state: str = "",
    venue_zip: str = "",
    charity_name: str = "",
    charity_description: str = "",
    voting_mode: str = "fundraiser_unlimited",
    payment_mode: str = "stripe",
    external_payment_url: str = "",
    allow_custom_votes: int = 1,
    preset_vote_options: str = "1,5,10,20,25",
    max_votes_per_checkout: int = 50,
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO shows (
            slug, flyer_image_path, title, date, time, cars_arrive_time, day_of_registration_time,
            show_start_time, show_end_time, location_name, address, benefiting,
            suggested_donation, description, status, short_details, qr_message,
            cta_label, cta_url, show_on_site, sort_order, hide_address, voting_open, is_active,
            waiver_template_id, organizer_name, venue_name, venue_address_line1, venue_address_line2,
            venue_city, venue_state, venue_zip, charity_name, charity_description,
            voting_mode, payment_mode, external_payment_url, allow_custom_votes, preset_vote_options, max_votes_per_checkout
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            slug.strip(),
            (flyer_image_path or "").strip(),
            title.strip(),
            (date or "").strip(),
            (time or "").strip(),
            (cars_arrive_time or "").strip(),
            (day_of_registration_time or "").strip(),
            (show_start_time or "").strip(),
            (show_end_time or "").strip(),
            (location_name or "").strip(),
            (address or "").strip(),
            (benefiting or "").strip(),
            (suggested_donation or "").strip(),
            (description or "").strip(),
            (status or "draft").strip(),
            (short_details or "").strip(),
            (qr_message or "").strip(),
            (cta_label or "").strip(),
            (cta_url or "").strip(),
            int(show_on_site),
            int(sort_order),
            int(hide_address),
            waiver_template_id,
            (organizer_name or "").strip(),
            (venue_name or "").strip(),
            (venue_address_line1 or "").strip(),
            (venue_address_line2 or "").strip(),
            (venue_city or "").strip(),
            (venue_state or "").strip(),
            (venue_zip or "").strip(),
            (charity_name or "").strip(),
            (charity_description or "").strip(),
            (voting_mode or "fundraiser_unlimited").strip(),
            (payment_mode or "stripe").strip(),
            (external_payment_url or "").strip(),
            int(allow_custom_votes),
            (preset_vote_options or "1,5,10,20,25").strip(),
            int(max_votes_per_checkout or 50),
        ),
    )
    conn.commit()
    rid = int(cur.lastrowid)
    conn.close()
    return rid


def update_show_admin_record(
    show_id: int,
    *,
    slug: str,
    title: str,
    flyer_image_path: str,
    date: str,
    time: str,
    cars_arrive_time: str = "",
    day_of_registration_time: str = "",
    show_start_time: str = "",
    show_end_time: str = "",
    location_name: str,
    address: str,
    benefiting: str,
    suggested_donation: str,
    description: str,
    status: str,
    short_details: str,
    qr_message: str,
    cta_label: str,
    cta_url: str,
    show_on_site: int,
    sort_order: int,
    hide_address: int = 0,
    waiver_template_id: Optional[int] = None,
    organizer_name: str = "",
    venue_name: str = "",
    venue_address_line1: str = "",
    venue_address_line2: str = "",
    venue_city: str = "",
    venue_state: str = "",
    venue_zip: str = "",
    charity_name: str = "",
    charity_description: str = "",
    voting_mode: str = "fundraiser_unlimited",
    payment_mode: str = "stripe",
    external_payment_url: str = "",
    allow_custom_votes: int = 1,
    preset_vote_options: str = "1,5,10,20,25",
    max_votes_per_checkout: int = 50,
) -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE shows
        SET slug = ?, title = ?, flyer_image_path = ?, date = ?, time = ?,
            cars_arrive_time = ?, day_of_registration_time = ?, show_start_time = ?, show_end_time = ?,
            location_name = ?, address = ?, benefiting = ?, suggested_donation = ?, description = ?, status = ?,
            short_details = ?, qr_message = ?, cta_label = ?, cta_url = ?,
            show_on_site = ?, sort_order = ?, hide_address = ?, waiver_template_id = ?,
            organizer_name = ?, venue_name = ?, venue_address_line1 = ?, venue_address_line2 = ?,
            venue_city = ?, venue_state = ?, venue_zip = ?, charity_name = ?, charity_description = ?,
            voting_mode = ?, payment_mode = ?, external_payment_url = ?, allow_custom_votes = ?,
            preset_vote_options = ?, max_votes_per_checkout = ?
        WHERE id = ?
        """,
        (
            slug.strip(),
            title.strip(),
            (flyer_image_path or "").strip(),
            (date or "").strip(),
            (time or "").strip(),
            (cars_arrive_time or "").strip(),
            (day_of_registration_time or "").strip(),
            (show_start_time or "").strip(),
            (show_end_time or "").strip(),
            (location_name or "").strip(),
            (address or "").strip(),
            (benefiting or "").strip(),
            (suggested_donation or "").strip(),
            (description or "").strip(),
            (status or "draft").strip(),
            (short_details or "").strip(),
            (qr_message or "").strip(),
            (cta_label or "").strip(),
            (cta_url or "").strip(),
            int(show_on_site),
            int(sort_order),
            int(hide_address),
            waiver_template_id,
            (organizer_name or "").strip(),
            (venue_name or "").strip(),
            (venue_address_line1 or "").strip(),
            (venue_address_line2 or "").strip(),
            (venue_city or "").strip(),
            (venue_state or "").strip(),
            (venue_zip or "").strip(),
            (charity_name or "").strip(),
            (charity_description or "").strip(),
            (voting_mode or "fundraiser_unlimited").strip(),
            (payment_mode or "stripe").strip(),
            (external_payment_url or "").strip(),
            int(allow_custom_votes),
            (preset_vote_options or "1,5,10,20,25").strip(),
            int(max_votes_per_checkout or 50),
            int(show_id),
        ),
    )
    conn.commit()
    conn.close()


def set_active_show(show_id: int) -> None:
    conn = _conn()
    conn.execute("UPDATE shows SET is_active = 0 WHERE is_active = 1")
    conn.execute("UPDATE shows SET is_active = 1, status = 'active' WHERE id = ?", (show_id,))
    conn.commit()
    conn.close()


def set_upcoming_show(show_id: int) -> None:
    conn = _conn()
    conn.execute("UPDATE shows SET status = 'upcoming', show_on_site = 1 WHERE id = ?", (show_id,))
    conn.commit()
    conn.close()


def set_past_show(show_id: int) -> None:
    conn = _conn()
    conn.execute("UPDATE shows SET status = 'past', is_active = 0 WHERE id = ?", (show_id,))
    conn.commit()
    conn.close()


def export_show_row(show_id: int):
    conn = _conn()
    row = conn.execute("SELECT * FROM shows WHERE id = ? LIMIT 1", (show_id,)).fetchone()
    conn.close()
    return row


def count_registered_cars(show_id: int) -> int:
    conn = _conn()
    row = conn.execute("SELECT COUNT(*) AS cnt FROM show_cars WHERE show_id = ?", (show_id,)).fetchone()
    conn.close()
    return int(row["cnt"] or 0)


def show_has_capacity(show_id: int) -> bool:
    show = export_show_row(show_id)
    if not show:
        return False
    max_cars = show["max_cars"] if "max_cars" in show.keys() else None
    if max_cars is None:
        return True
    try:
        max_cars = int(max_cars)
    except Exception:
        return True
    if max_cars <= 0:
        return True
    return count_registered_cars(show_id) < max_cars


def set_show_voting_open(show_id: int, voting_open: bool) -> None:
    conn = _conn()
    conn.execute("UPDATE shows SET voting_open = ? WHERE id = ?", (_b(voting_open), show_id))
    conn.commit()
    conn.close()


def toggle_show_voting(show_id: int) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE shows SET voting_open = CASE voting_open WHEN 1 THEN 0 ELSE 1 END WHERE id = ?",
        (show_id,),
    )
    conn.commit()
    conn.close()


def update_show_admin_settings(
    show_id: int,
    show_type: str,
    allow_prereg_override: Optional[int],
    max_cars: Optional[int],
    registration_fee_cents: int,
    attendee_fee_cents: int,
    vote_price_cents: int,
    public_vote_disclosure: str,
    public_registration_disclosure: str,
    public_donation_disclosure: str,
    voting_mode: str = "fundraiser_unlimited",
    payment_mode: str = "stripe",
    external_payment_url: str = "",
    allow_custom_votes: int = 1,
    preset_vote_options: str = "1,5,10,20,25",
    max_votes_per_checkout: int = 50,
    waiver_text: str = "",
    waiver_version: str = "",
) -> None:
    st = (show_type or "full").strip().lower()
    if st not in ("popup", "full"):
        st = "full"

    if allow_prereg_override is not None:
        try:
            allow_prereg_override = int(allow_prereg_override)
        except Exception:
            allow_prereg_override = None
        if allow_prereg_override not in (0, 1):
            allow_prereg_override = None

    if max_cars is not None:
        try:
            max_cars = int(max_cars)
            if max_cars <= 0:
                max_cars = None
        except Exception:
            max_cars = None

    try:
        registration_fee_cents = max(0, int(registration_fee_cents))
    except Exception:
        registration_fee_cents = 0

    try:
        attendee_fee_cents = max(0, int(attendee_fee_cents))
    except Exception:
        attendee_fee_cents = 0

    try:
        vote_price_cents = max(1, int(vote_price_cents))
    except Exception:
        vote_price_cents = 100

    voting_mode = (voting_mode or "fundraiser_unlimited").strip().lower()
    if voting_mode not in {"fundraiser_unlimited", "restricted_single"}:
        voting_mode = "fundraiser_unlimited"

    payment_mode = (payment_mode or "stripe").strip().lower()
    if payment_mode not in {"stripe", "external"}:
        payment_mode = "stripe"

    try:
        allow_custom_votes = 1 if int(allow_custom_votes) == 1 else 0
    except Exception:
        allow_custom_votes = 1

    preset_vote_options = (preset_vote_options or "1,5,10,20,25").strip()

    try:
        max_votes_per_checkout = max(1, int(max_votes_per_checkout))
    except Exception:
        max_votes_per_checkout = 50

    conn = _conn()
    conn.execute(
        """
        UPDATE shows
        SET show_type = ?,
            allow_prereg_override = ?,
            max_cars = ?,
            registration_fee_cents = ?,
            attendee_fee_cents = ?,
            vote_price_cents = ?,
            public_vote_disclosure = ?,
            public_registration_disclosure = ?,
            public_donation_disclosure = ?,
            voting_mode = ?,
            payment_mode = ?,
            external_payment_url = ?,
            allow_custom_votes = ?,
            preset_vote_options = ?,
            max_votes_per_checkout = ?,
            waiver_text = CASE WHEN TRIM(?) <> '' THEN ? ELSE waiver_text END,
            waiver_version = CASE WHEN TRIM(?) <> '' THEN ? ELSE waiver_version END
        WHERE id = ?
        """,
        (
            st,
            allow_prereg_override,
            max_cars,
            registration_fee_cents,
            attendee_fee_cents,
            vote_price_cents,
            (public_vote_disclosure or "").strip(),
            (public_registration_disclosure or "").strip(),
            (public_donation_disclosure or "").strip(),
            voting_mode,
            payment_mode,
            (external_payment_url or "").strip(),
            allow_custom_votes,
            preset_vote_options,
            max_votes_per_checkout,
            (waiver_text or "").strip(),
            (waiver_text or "").strip(),
            (waiver_version or "").strip(),
            (waiver_version or "").strip(),
            show_id,
        ),
    )
    conn.commit()
    conn.close()


def set_show_charity_connect(show_id: int, stripe_account_id: str, connect_status: str = "connected", connect_email: str = "") -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE shows
        SET charity_stripe_account_id = ?,
            charity_connect_status = ?,
            charity_connect_email = ?,
            charity_connected_at = datetime('now')
        WHERE id = ?
        """,
        ((stripe_account_id or "").strip(), (connect_status or "connected").strip(), (connect_email or "").strip(), show_id),
    )
    conn.commit()
    conn.close()


def clear_show_charity_connect(show_id: int) -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE shows
        SET charity_stripe_account_id = NULL,
            charity_connect_status = 'not_connected',
            charity_connect_email = NULL
        WHERE id = ?
        """,
        (show_id,),
    )
    conn.commit()
    conn.close()


# REGISTRATION / PEOPLE

def create_person(name: str, phone: str, email: str, opt_in_future: bool, sponsor_opt_in: bool, consent_text: str, consent_version: str) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO people (name, phone, email, opt_in_future, sponsor_opt_in, consent_text, consent_version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (name, phone, email, _b(opt_in_future), _b(sponsor_opt_in), consent_text, consent_version),
    )
    conn.commit()
    pid = int(cur.lastrowid)
    conn.close()
    return pid


def update_person(person_id: int, name: str, phone: str, email: str, opt_in_future: bool, sponsor_opt_in: bool, consent_text: str, consent_version: str) -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE people
        SET name = ?, phone = ?, email = ?, opt_in_future = ?, sponsor_opt_in = ?, consent_text = ?, consent_version = ?
        WHERE id = ?
        """,
        (name, phone, email, _b(opt_in_future), _b(sponsor_opt_in), consent_text, consent_version, person_id),
    )
    conn.commit()
    conn.close()


def create_show_car(show_id: int, person_id: int, car_number: int, year: str, make: str, model: str) -> Tuple[int, str]:
    conn = _conn()
    cur = conn.cursor()
    show = cur.execute("SELECT max_cars FROM shows WHERE id = ? LIMIT 1", (show_id,)).fetchone()
    if show and show["max_cars"] is not None:
        try:
            max_cars = int(show["max_cars"])
        except Exception:
            max_cars = None
        if max_cars and max_cars > 0:
            row = cur.execute("SELECT COUNT(*) AS cnt FROM show_cars WHERE show_id = ?", (show_id,)).fetchone()
            if int(row["cnt"] or 0) >= max_cars:
                conn.close()
                raise ValueError("This show has reached its maximum number of cars.")

    token = _new_car_token()
    try:
        cur.execute(
            """
            INSERT INTO show_cars (
                show_id, person_id, car_number, car_token, year, make, model,
                is_placeholder, registration_state
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'paid')
            """,
            (show_id, person_id, car_number, token, year, make, model),
        )
        conn.commit()
        scid = int(cur.lastrowid)
        conn.close()
        return scid, token
    except sqlite3.IntegrityError as e:
        conn.close()
        raise ValueError("That car number is already registered for this show.") from e


def update_show_car_details(show_car_id: int, year: str, make: str, model: str) -> None:
    conn = _conn()
    conn.execute("UPDATE show_cars SET year = ?, make = ?, model = ? WHERE id = ?", (year, make, model, show_car_id))
    conn.commit()
    conn.close()


def mark_show_car_checked_in(show_car_id: int) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE show_cars SET registration_state = 'checked_in', checked_in_at = COALESCE(checked_in_at, datetime('now')) WHERE id = ?",
        (show_car_id,),
    )
    conn.commit()
    conn.close()


def get_show_car_public_by_token(show_id: int, car_token: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        """
        SELECT sc.*, p.name as owner_name
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ? AND sc.car_token = ?
        LIMIT 1
        """,
        (show_id, car_token),
    ).fetchone()
    conn.close()
    return row


def get_show_car_private_by_token(show_id: int, car_token: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        """
        SELECT
            sc.*,
            p.name as owner_name,
            p.phone as owner_phone,
            p.email as owner_email,
            p.opt_in_future,
            p.sponsor_opt_in,
            p.consent_text,
            p.consent_version
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ? AND sc.car_token = ?
        LIMIT 1
        """,
        (show_id, car_token),
    ).fetchone()
    conn.close()
    return row


def get_show_car_by_number(show_id: int, car_number: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        """
        SELECT sc.*, p.name as owner_name
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ? AND sc.car_number = ?
        LIMIT 1
        """,
        (show_id, car_number),
    ).fetchone()
    conn.close()
    return row


def list_show_cars_public(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute(
        """
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
            sc.registration_payment_status,
            sc.registration_amount_cents,
            sc.waiver_signed_name,
            sc.waiver_signed_at,
            sc.is_placeholder,
            sc.registration_state,
            sc.checked_in_at,
            p.name as owner_name
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ?
        ORDER BY sc.car_number ASC
        """,
        (show_id,),
    ).fetchall()
    conn.close()
    return rows


# REGISTRATION INTENTS

def create_registration_intent(
    show_id: int,
    owner_name: str,
    phone: str,
    email: str,
    opt_in_future: bool,
    sponsor_opt_in: bool,
    car_number: int,
    year: str,
    make: str,
    model: str,
    waiver_accepted: bool,
    waiver_signed_name: str,
    waiver_text: str,
    waiver_version: str,
    amount_cents: int,
    waiver_template_id: Optional[int] = None,
) -> Tuple[int, str]:
    conn = _conn()
    cur = conn.cursor()

    if not show_has_capacity(show_id):
        conn.close()
        raise ValueError("This show has reached its maximum number of cars.")

    existing = cur.execute("SELECT id FROM show_cars WHERE show_id = ? AND car_number = ? LIMIT 1", (show_id, car_number)).fetchone()
    if existing:
        conn.close()
        raise ValueError("That car number is already registered for this show.")

    token = _new_token()
    cur.execute(
        """
        INSERT INTO registration_intents (
            show_id, intent_token, owner_name, phone, email, opt_in_future, sponsor_opt_in,
            car_number, year, make, model,
            waiver_accepted, waiver_signed_name, waiver_text, waiver_version, waiver_text_sha256,
            waiver_template_id, amount_cents, payment_status
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
        """,
        (
            show_id,
            token,
            owner_name,
            phone,
            email,
            _b(opt_in_future),
            _b(sponsor_opt_in),
            car_number,
            year,
            make,
            model,
            _b(waiver_accepted),
            waiver_signed_name,
            waiver_text,
            waiver_version,
            _sha256_text(waiver_text),
            waiver_template_id,
            int(amount_cents),
        ),
    )
    conn.commit()
    rid = int(cur.lastrowid)
    conn.close()
    return rid, token


def get_registration_intent_by_token(intent_token: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM registration_intents WHERE intent_token = ? LIMIT 1", (intent_token,)).fetchone()
    conn.close()
    return row


def get_registration_intent_by_session(stripe_session_id: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM registration_intents WHERE stripe_session_id = ? LIMIT 1", (stripe_session_id,)).fetchone()
    conn.close()
    return row


def attach_stripe_session_to_registration_intent(registration_intent_id: int, stripe_session_id: str, stripe_payment_intent_id: str = "") -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE registration_intents
        SET stripe_session_id = ?, stripe_payment_intent_id = ?
        WHERE id = ?
        """,
        (stripe_session_id, stripe_payment_intent_id or None, registration_intent_id),
    )
    conn.commit()
    conn.close()


def finalize_registration_intent_paid(stripe_session_id: str) -> Dict[str, Any]:
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        ri = cur.execute("SELECT * FROM registration_intents WHERE stripe_session_id = ? LIMIT 1", (stripe_session_id,)).fetchone()
        if not ri:
            raise ValueError("Registration intent not found.")

        if ri["finalized_show_car_id"]:
            sc = cur.execute("SELECT * FROM show_cars WHERE id = ? LIMIT 1", (int(ri["finalized_show_car_id"]),)).fetchone()
            cur.execute(
                "UPDATE registration_intents SET payment_status = 'paid', paid_at = COALESCE(paid_at, datetime('now')) WHERE id = ?",
                (int(ri["id"]),),
            )
            conn.commit()
            return {
                "registration_intent_id": int(ri["id"]),
                "show_car_id": int(ri["finalized_show_car_id"]),
                "car_token": sc["car_token"] if sc else None,
                "already_finalized": True,
            }

        show_id = int(ri["show_id"])
        if not show_has_capacity(show_id):
            raise ValueError("This show has reached its maximum number of cars.")

        existing_final = cur.execute(
            "SELECT id FROM show_cars WHERE show_id = ? AND car_number = ? LIMIT 1",
            (show_id, int(ri["car_number"])),
        ).fetchone()
        if existing_final:
            raise ValueError("That car number is already registered for this show.")

        person_consent_text = (
            "By submitting this form, you agree Karman Kar Shows & Events may contact you about this event and future "
            "events if selected and, if chosen, may share sponsor information. Msg/data rates may apply. Opt out anytime."
        )
        person_consent_version = "2026-registration-flow"

        cur.execute(
            """
            INSERT INTO people (name, phone, email, opt_in_future, sponsor_opt_in, consent_text, consent_version)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ri["owner_name"],
                ri["phone"],
                ri["email"],
                int(ri["opt_in_future"] or 0),
                int(ri["sponsor_opt_in"] or 0),
                person_consent_text,
                person_consent_version,
            ),
        )
        person_id = int(cur.lastrowid)

        car_token = _new_car_token()
        cur.execute(
            """
            INSERT INTO show_cars (
                show_id, person_id, car_number, car_token, year, make, model,
                registration_payment_status, registration_amount_cents, registration_session_id,
                waiver_signed_name, waiver_signed_at, waiver_version, waiver_text, waiver_text_sha256, waiver_template_id,
                waiver_received, waiver_received_at, waiver_received_by,
                is_placeholder, registration_state
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, 1, datetime('now'), 'electronic', 0, 'paid')
            """,
            (
                show_id,
                person_id,
                int(ri["car_number"]),
                car_token,
                ri["year"],
                ri["make"],
                ri["model"],
                "paid",
                int(ri["amount_cents"] or 0),
                stripe_session_id,
                ri["waiver_signed_name"],
                ri["waiver_version"],
                ri["waiver_text"],
                ri["waiver_text_sha256"],
                ri["waiver_template_id"] if "waiver_template_id" in ri.keys() else None,
            ),
        )
        show_car_id = int(cur.lastrowid)

        cur.execute(
            "UPDATE registration_intents SET payment_status = 'paid', paid_at = datetime('now'), finalized_show_car_id = ? WHERE id = ?",
            (show_car_id, int(ri["id"])),
        )

        conn.commit()
        return {
            "registration_intent_id": int(ri["id"]),
            "show_car_id": show_car_id,
            "car_token": car_token,
            "already_finalized": False,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# PLACEHOLDER CARS

def create_placeholder_cars(show_id: int, start_number: int, count: int) -> int:
    conn = _conn()
    cur = conn.cursor()
    show = cur.execute("SELECT max_cars FROM shows WHERE id = ? LIMIT 1", (show_id,)).fetchone()
    max_cars = None
    if show and show["max_cars"] is not None:
        try:
            max_cars = int(show["max_cars"])
        except Exception:
            max_cars = None

    current_count = int(cur.execute("SELECT COUNT(*) AS cnt FROM show_cars WHERE show_id = ?", (show_id,)).fetchone()["cnt"] or 0)
    created = 0
    for n in range(start_number, start_number + count):
        if max_cars and max_cars > 0 and (current_count + created) >= max_cars:
            break
        exists = cur.execute("SELECT 1 FROM show_cars WHERE show_id = ? AND car_number = ? LIMIT 1", (show_id, n)).fetchone()
        if exists:
            continue
        cur.execute(
            "INSERT INTO people (name, phone, email, opt_in_future, sponsor_opt_in, consent_text, consent_version) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("", "", "", 0, 0, None, None),
        )
        person_id = int(cur.lastrowid)
        token = _new_car_token()
        cur.execute(
            """
            INSERT INTO show_cars (
                show_id, person_id, car_number, car_token, year, make, model,
                is_placeholder, registration_state
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'placeholder')
            """,
            (show_id, person_id, n, token, "TBD", "TBD", "TBD"),
        )
        created += 1

    conn.commit()
    conn.close()
    return created


# VOTING

def create_vote_intent(show_id: int, show_car_id: int, category: str, vote_qty: int, amount_cents: int) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO vote_intents (show_id, show_car_id, category, vote_qty, amount_cents, payment_status) VALUES (?, ?, ?, ?, ?, 'pending')",
        (show_id, show_car_id, category, vote_qty, amount_cents),
    )
    conn.commit()
    vid = int(cur.lastrowid)
    conn.close()
    return vid


def get_vote_intent(vote_intent_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM vote_intents WHERE id = ? LIMIT 1", (vote_intent_id,)).fetchone()
    conn.close()
    return row


def get_vote_intent_by_session(stripe_session_id: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM vote_intents WHERE stripe_session_id = ? LIMIT 1", (stripe_session_id,)).fetchone()
    conn.close()
    return row


def attach_stripe_session_to_vote_intent(vote_intent_id: int, stripe_session_id: str, stripe_payment_intent_id: str = "") -> None:
    conn = _conn()
    conn.execute(
        "UPDATE vote_intents SET stripe_session_id = ?, stripe_payment_intent_id = ? WHERE id = ?",
        (stripe_session_id, stripe_payment_intent_id or None, vote_intent_id),
    )
    conn.commit()
    conn.close()


def finalize_vote_intent_paid(stripe_session_id: str) -> Dict[str, Any]:
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        vi = cur.execute("SELECT * FROM vote_intents WHERE stripe_session_id = ? LIMIT 1", (stripe_session_id,)).fetchone()
        if not vi:
            raise ValueError("Vote intent not found.")
        existing_vote = cur.execute("SELECT id FROM votes WHERE stripe_session_id = ? LIMIT 1", (stripe_session_id,)).fetchone()
        if not existing_vote:
            cur.execute(
                "INSERT INTO votes (show_id, show_car_id, category, vote_qty, amount_cents, stripe_session_id) VALUES (?, ?, ?, ?, ?, ?)",
                (int(vi["show_id"]), int(vi["show_car_id"]), vi["category"], int(vi["vote_qty"]), int(vi["amount_cents"]), stripe_session_id),
            )
        cur.execute(
            "UPDATE vote_intents SET payment_status = 'paid', paid_at = COALESCE(paid_at, datetime('now')) WHERE id = ?",
            (int(vi["id"]),),
        )
        conn.commit()
        return {"vote_intent_id": int(vi["id"]), "already_finalized": bool(existing_vote)}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reset_votes_for_show(show_id: int) -> None:
    conn = _conn()
    conn.execute("DELETE FROM votes WHERE show_id = ?", (show_id,))
    conn.execute("DELETE FROM vote_intents WHERE show_id = ?", (show_id,))
    conn.commit()
    conn.close()


def export_votes_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute(
        """
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
            p.opt_in_future,
            p.sponsor_opt_in,
            p.consent_version
        FROM votes v
        JOIN show_cars sc ON sc.id = v.show_car_id
        JOIN people p ON p.id = sc.person_id
        WHERE v.show_id = ?
        ORDER BY v.created_at ASC
        """,
        (show_id,),
    ).fetchall()
    conn.close()
    return rows


def leaderboard_by_category(show_id: int) -> Dict[str, List[Tuple[int, int]]]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT v.category, sc.car_number, SUM(v.vote_qty) as total_votes
        FROM votes v
        JOIN show_cars sc ON sc.id = v.show_car_id
        WHERE v.show_id = ?
        GROUP BY v.category, sc.car_number
        ORDER BY v.category ASC, total_votes DESC, sc.car_number ASC
        """,
        (show_id,),
    ).fetchall()
    conn.close()
    out: Dict[str, List[Tuple[int, int]]] = {}
    for r in rows:
        out.setdefault(r["category"], [])
        out[r["category"]].append((int(r["car_number"]), int(r["total_votes"] or 0)))
    return out


def leaderboard_overall(show_id: int) -> List[Tuple[int, int]]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT sc.car_number, SUM(v.vote_qty) as total_votes
        FROM votes v
        JOIN show_cars sc ON sc.id = v.show_car_id
        WHERE v.show_id = ?
        GROUP BY sc.car_number
        ORDER BY total_votes DESC, sc.car_number ASC
        """,
        (show_id,),
    ).fetchall()
    conn.close()
    return [(int(r["car_number"]), int(r["total_votes"] or 0)) for r in rows]


# SPONSORS

def upsert_sponsor(name: str, logo_path: str = "", website_url: str = "") -> int:
    conn = _conn()
    cur = conn.cursor()
    existing = cur.execute("SELECT id FROM sponsors WHERE name = ? LIMIT 1", (name,)).fetchone()
    if existing:
        cur.execute("UPDATE sponsors SET logo_path = ?, website_url = ? WHERE id = ?", (logo_path, website_url, int(existing["id"])))
        conn.commit()
        conn.close()
        return int(existing["id"])
    cur.execute("INSERT INTO sponsors (name, logo_path, website_url) VALUES (?, ?, ?)", (name, logo_path, website_url))
    conn.commit()
    sid = int(cur.lastrowid)
    conn.close()
    return sid


def attach_sponsor_to_show(show_id: int, sponsor_id: int, placement: str = "standard", sort_order: int = 100) -> None:
    conn = _conn()
    conn.execute(
        """
        INSERT INTO show_sponsors (show_id, sponsor_id, placement, sort_order)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(show_id, sponsor_id) DO UPDATE SET
          placement=excluded.placement,
          sort_order=excluded.sort_order
        """,
        (show_id, sponsor_id, placement, sort_order),
    )
    conn.commit()
    conn.close()


def remove_sponsor_from_show(show_id: int, sponsor_id: int) -> None:
    conn = _conn()
    conn.execute("DELETE FROM show_sponsors WHERE show_id = ? AND sponsor_id = ?", (show_id, sponsor_id))
    conn.commit()
    conn.close()


def set_title_sponsor(show_id: int, sponsor_id: int) -> None:
    conn = _conn()
    conn.execute("UPDATE show_sponsors SET placement = 'standard' WHERE show_id = ? AND placement = 'title'", (show_id,))
    conn.commit()
    conn.close()
    attach_sponsor_to_show(show_id, sponsor_id, placement="title", sort_order=0)


def get_show_sponsors(show_id: int):
    conn = _conn()
    rows = conn.execute(
        """
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
        ORDER BY CASE WHEN ss.placement = 'title' THEN 0 ELSE 1 END, ss.sort_order ASC, s.id ASC
        """,
        (show_id,),
    ).fetchall()
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


# ATTENDEES + DONATIONS + METRICS

def create_attendee(show_id: int, first_name: str, last_name: str, phone: str, email: str, zip_code: str, sponsor_opt_in: bool, updates_opt_in: bool, consent_text: str, consent_version: str) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO attendees
        (show_id, first_name, last_name, phone, email, zip, sponsor_opt_in, updates_opt_in, consent_text, consent_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (show_id, first_name, last_name, phone or None, email or None, zip_code or None, _b(sponsor_opt_in), _b(updates_opt_in), consent_text, consent_version),
    )
    conn.commit()
    aid = int(cur.lastrowid)
    conn.close()
    return aid


def record_field_metric(show_id: int, field_name: str, was_provided: bool) -> None:
    conn = _conn()
    conn.execute("INSERT INTO field_metrics (show_id, field_name, was_provided) VALUES (?, ?, ?)", (show_id, field_name, _b(was_provided)))
    conn.commit()
    conn.close()


def create_donation_row(show_id: int, attendee_id: int, amount_cents: int, status: str) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO donations (show_id, attendee_id, amount_cents, status) VALUES (?, ?, ?, ?)", (show_id, attendee_id, int(amount_cents), status))
    conn.commit()
    did = int(cur.lastrowid)
    conn.close()
    return did


def get_donation_by_id(donation_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM donations WHERE id = ? LIMIT 1", (donation_id,)).fetchone()
    conn.close()
    return row


def get_donation_by_session(stripe_session_id: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM donations WHERE stripe_session_id = ? LIMIT 1", (stripe_session_id,)).fetchone()
    conn.close()
    return row


def attach_stripe_session_to_donation(donation_id: int, stripe_session_id: str, stripe_payment_intent_id: str = "") -> None:
    conn = _conn()
    conn.execute(
        "UPDATE donations SET stripe_session_id = ?, stripe_payment_intent_id = ? WHERE id = ?",
        (stripe_session_id, stripe_payment_intent_id or None, donation_id),
    )
    conn.commit()
    conn.close()


def mark_donation_paid(stripe_session_id: str) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE donations SET status = 'paid', paid_at = COALESCE(paid_at, datetime('now')) WHERE stripe_session_id = ?",
        (stripe_session_id,),
    )
    conn.commit()
    conn.close()


# WAIVER TRACKING / AUDIT / RATE LIMITING

def waiver_mark_received(show_id: int, show_car_id: int, received_by: str) -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE show_cars
        SET waiver_received = 1,
            waiver_received_at = datetime('now'),
            waiver_received_by = ?
        WHERE id = ? AND show_id = ?
        """,
        (received_by or "staff", show_car_id, show_id),
    )
    conn.commit()
    conn.close()


def create_waiver_evidence_record(
    *,
    show_id: int,
    registration_intent_id: Optional[int],
    show_car_id: Optional[int],
    car_number: int,
    owner_name: str,
    phone: str,
    email: str,
    year: str,
    make: str,
    model: str,
    opt_in_future: bool,
    sponsor_opt_in: bool,
    waiver_version: str,
    waiver_text: str,
    signed_name: str,
    waiver_accepted: bool,
    intent_token: str,
    html_path: str,
    request_path: str,
    ip_address: str,
    user_agent: str,
    created_at_utc: str,
    created_at_local: str,
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO waiver_evidence (
            show_id, registration_intent_id, show_car_id, car_number,
            owner_name, phone, email, year, make, model,
            opt_in_future, sponsor_opt_in,
            waiver_version, waiver_text_sha256, signed_name, waiver_accepted,
            intent_token, html_path, request_path, ip_address, user_agent,
            created_at_utc, created_at_local
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            show_id,
            registration_intent_id,
            show_car_id,
            car_number,
            owner_name,
            phone,
            email,
            year,
            make,
            model,
            _b(opt_in_future),
            _b(sponsor_opt_in),
            waiver_version,
            _sha256_text(waiver_text),
            signed_name,
            _b(waiver_accepted),
            intent_token,
            html_path,
            request_path,
            ip_address,
            user_agent,
            created_at_utc,
            created_at_local,
        ),
    )
    conn.commit()
    rid = int(cur.lastrowid)
    conn.close()
    return rid


def log_audit_event(show_id: Optional[int], actor_type: str, action: str, details: Optional[Dict[str, Any]], ip_address: str, user_agent: str) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO audit_logs (show_id, actor_type, action, details_json, ip_address, user_agent) VALUES (?, ?, ?, ?, ?, ?)",
        (
            show_id,
            (actor_type or "system")[:50],
            (action or "unknown")[:100],
            json.dumps(details or {}, ensure_ascii=False),
            (ip_address or "")[:255],
            (user_agent or "")[:1000],
        ),
    )
    conn.commit()
    rid = int(cur.lastrowid)
    conn.close()
    return rid


def rate_limit_increment(bucket_key: str, window_seconds: int) -> int:
    now_epoch = int(datetime.utcnow().timestamp())
    window_started_at = now_epoch - (now_epoch % max(1, int(window_seconds)))
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO rate_limit_hits (bucket_key, window_started_at, hit_count)
        VALUES (?, ?, 1)
        ON CONFLICT(bucket_key, window_started_at)
        DO UPDATE SET hit_count = hit_count + 1, updated_at = datetime('now')
        """,
        (bucket_key, window_started_at),
    )
    row = cur.execute(
        "SELECT hit_count FROM rate_limit_hits WHERE bucket_key = ? AND window_started_at = ? LIMIT 1",
        (bucket_key, window_started_at),
    ).fetchone()
    conn.commit()
    conn.close()
    return int(row["hit_count"] or 0)


def has_processed_webhook_event(stripe_event_id: str) -> bool:
    conn = _conn()
    row = conn.execute("SELECT 1 FROM processed_webhook_events WHERE stripe_event_id = ? LIMIT 1", (stripe_event_id,)).fetchone()
    conn.close()
    return bool(row)


def mark_webhook_event_processed(stripe_event_id: str, event_type: str) -> None:
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO processed_webhook_events (stripe_event_id, event_type) VALUES (?, ?)",
            (stripe_event_id, event_type),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()


# UPCOMING EVENT / INTEREST SIGNUPS

def get_upcoming_event() -> Optional[Dict[str, Any]]:
    row = get_next_upcoming_show()
    if not row:
        return None

    return {
        "heading": "Upcoming show",
        "title": row["title"] or "",
        "display_date": row["date"] or "",
        "visible": 1 if int(row["show_on_site"] or 0) == 1 else 0,
        "intro": row["description"] or "Check the newsletter QR code for the latest details on our next show or pop-up event.",
        "details": row["short_details"] or "",
        "qr_message": row["qr_message"] or "",
    }


def save_upcoming_event(
    *,
    heading: str,
    title: str,
    display_date: str,
    visible: int,
    intro: str,
    details: str,
    qr_message: str,
) -> None:
    conn = _conn()
    cur = conn.cursor()

    row = cur.execute(
        """
        SELECT *
        FROM shows
        WHERE status = 'upcoming'
        ORDER BY sort_order ASC, id ASC
        LIMIT 1
        """
    ).fetchone()

    slug = _slugify(title or "upcoming-show")

    if row:
        cur.execute(
            """
            UPDATE shows
            SET title = ?,
                date = ?,
                description = ?,
                short_details = ?,
                qr_message = ?,
                show_on_site = ?,
                sort_order = 0,
                hide_address = 0,
                status = 'upcoming'
            WHERE id = ?
            """,
            (
                (title or "").strip(),
                (display_date or "").strip(),
                (intro or "").strip(),
                (details or "").strip(),
                (qr_message or "").strip(),
                int(visible),
                int(row["id"]),
            ),
        )
    else:
        candidate_slug = slug
        n = 2
        while cur.execute("SELECT 1 FROM shows WHERE slug = ? LIMIT 1", (candidate_slug,)).fetchone():
            candidate_slug = f"{slug}-{n}"
            n += 1

        cur.execute(
            """
            INSERT INTO shows (
                slug, title, date, description, short_details, qr_message,
                status, show_on_site, sort_order, hide_address, voting_open, is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, 'upcoming', ?, 0, 0, 0, 0)
            """,
            (
                candidate_slug,
                (title or "").strip(),
                (display_date or "").strip(),
                (intro or "").strip(),
                (details or "").strip(),
                (qr_message or "").strip(),
                int(visible),
            ),
        )

    conn.commit()
    conn.close()


def create_event_interest_signup(
    *,
    show_id: Optional[int] = None,
    first_name: str,
    last_name: str,
    email: str,
    phone: str,
    wants_email: bool,
    wants_text: bool,
    source: str = "",
) -> int:
    if show_id is None:
        row = get_next_upcoming_show()
        show_id = int(row["id"]) if row else None

    conn = _conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO event_interest_signups (
            show_id, first_name, last_name, email, phone,
            wants_email, wants_text, source
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            show_id,
            (first_name or "").strip(),
            (last_name or "").strip(),
            (email or "").strip(),
            (phone or "").strip(),
            _b(wants_email),
            _b(wants_text),
            (source or "").strip(),
        ),
    )

    conn.commit()
    rid = int(cur.lastrowid)
    conn.close()
    return rid


def list_event_interest_signups(show_id: Optional[int] = None) -> List[sqlite3.Row]:
    conn = _conn()
    if show_id is None:
        rows = conn.execute(
            """
            SELECT
                eis.*,
                s.title AS show_title,
                s.slug AS show_slug
            FROM event_interest_signups eis
            LEFT JOIN shows s ON s.id = eis.show_id
            ORDER BY eis.created_at DESC, eis.id DESC
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                eis.*,
                s.title AS show_title,
                s.slug AS show_slug
            FROM event_interest_signups eis
            LEFT JOIN shows s ON s.id = eis.show_id
            WHERE eis.show_id = ?
            ORDER BY eis.created_at DESC, eis.id DESC
            """,
            (show_id,),
        ).fetchall()
    conn.close()
    return rows


def export_event_interest_signups_csv(show_id: Optional[int] = None) -> bytes:
    rows = list_event_interest_signups(show_id)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "created_at",
        "show_id",
        "show_title",
        "show_slug",
        "first_name",
        "last_name",
        "email",
        "phone",
        "wants_email",
        "wants_text",
        "source",
    ])
    for r in rows:
        w.writerow([
            r["created_at"],
            r["show_id"],
            r["show_title"],
            r["show_slug"],
            r["first_name"],
            r["last_name"],
            r["email"],
            r["phone"],
            r["wants_email"],
            r["wants_text"],
            r["source"],
        ])
    return buf.getvalue().encode("utf-8")


# SNAPSHOT EXPORT

def export_people_rows_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT DISTINCT p.*
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ?
        ORDER BY p.created_at ASC
        """,
        (show_id,),
    ).fetchall()
    conn.close()
    return rows


def export_show_cars_rows(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT
            sc.*,
            p.name as owner_name,
            p.phone as owner_phone,
            p.email as owner_email,
            p.opt_in_future,
            p.sponsor_opt_in,
            p.consent_version,
            p.consent_text
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ?
        ORDER BY sc.car_number ASC
        """,
        (show_id,),
    ).fetchall()
    conn.close()
    return rows


def export_registration_intents_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute("SELECT * FROM registration_intents WHERE show_id = ? ORDER BY created_at ASC", (show_id,)).fetchall()
    conn.close()
    return rows


def export_vote_intents_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute("SELECT * FROM vote_intents WHERE show_id = ? ORDER BY created_at ASC", (show_id,)).fetchall()
    conn.close()
    return rows


def export_donations_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute("SELECT * FROM donations WHERE show_id = ? ORDER BY created_at ASC", (show_id,)).fetchall()
    conn.close()
    return rows


def export_waiver_evidence_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute("SELECT * FROM waiver_evidence WHERE show_id = ? ORDER BY created_at ASC", (show_id,)).fetchall()
    conn.close()
    return rows


def export_audit_logs_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute("SELECT * FROM audit_logs WHERE show_id = ? ORDER BY created_at ASC", (show_id,)).fetchall()
    conn.close()
    return rows


def _rows_to_csv_bytes(rows: List[sqlite3.Row]) -> bytes:
    buf = io.StringIO()
    if not rows:
        return b""
    headers = list(rows[0].keys())
    w = csv.writer(buf)
    w.writerow(headers)
    for r in rows:
        w.writerow([r[h] for h in headers])
    return buf.getvalue().encode("utf-8")


def build_snapshot_zip_bytes(show_id: int) -> Tuple[bytes, str]:
    show = export_show_row(show_id)
    if not show:
        raise ValueError("Show not found.")

    stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    slug = (show["slug"] or f"show-{show_id}").strip()
    filename = f"{slug}-snapshot-{stamp}.zip"

    mem = io.BytesIO()
    with zipfile.ZipFile(mem, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("show.csv", _rows_to_csv_bytes([show]))
        zf.writestr("people.csv", _rows_to_csv_bytes(export_people_rows_for_show(show_id)))
        zf.writestr("show_cars.csv", _rows_to_csv_bytes(export_show_cars_rows(show_id)))
        zf.writestr("registration_intents.csv", _rows_to_csv_bytes(export_registration_intents_for_show(show_id)))
        zf.writestr("votes.csv", _rows_to_csv_bytes(export_votes_for_show(show_id)))
        zf.writestr("vote_intents.csv", _rows_to_csv_bytes(export_vote_intents_for_show(show_id)))
        zf.writestr("donations.csv", _rows_to_csv_bytes(export_donations_for_show(show_id)))
        zf.writestr("waiver_evidence.csv", _rows_to_csv_bytes(export_waiver_evidence_for_show(show_id)))
        zf.writestr("audit_logs.csv", _rows_to_csv_bytes(export_audit_logs_for_show(show_id)))

    mem.seek(0)
    return mem.getvalue(), filename