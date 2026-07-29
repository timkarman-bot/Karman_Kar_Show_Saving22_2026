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
from datetime import datetime, timezone

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
        "ALTER TABLE shows ADD COLUMN allow_sponsorships INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE shows ADD COLUMN registration_slot_selection_mode TEXT NOT NULL DEFAULT 'single'",
        "ALTER TABLE shows ADD COLUMN card_headline TEXT",
        "ALTER TABLE shows ADD COLUMN card_subheadline TEXT",
        "ALTER TABLE shows ADD COLUMN card_layout_mode TEXT NOT NULL DEFAULT 'auto'",
        "ALTER TABLE shows ADD COLUMN participant_voting_enabled INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE shows ADD COLUMN participant_vote_change_allowed INTEGER NOT NULL DEFAULT 1",
        "ALTER TABLE shows ADD COLUMN participant_voting_completion_message TEXT",
        "ALTER TABLE shows ADD COLUMN voting_method TEXT NOT NULL DEFAULT 'qr_only'",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS show_judging_classes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            class_name TEXT,
            class_code TEXT,
            description TEXT,
            year_min INTEGER,
            year_max INTEGER,
            make_contains TEXT,
            model_contains TEXT,
            keyword_contains TEXT,
            award_places INTEGER NOT NULL DEFAULT 3,
            sort_order INTEGER NOT NULL DEFAULT 100,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )
    
    # Judging class table migrations
    # Safe for existing Railway databases where the table may already exist
    # but may be missing newer columns.
    for sql in [
        "ALTER TABLE show_judging_classes ADD COLUMN class_name TEXT",
        "ALTER TABLE show_judging_classes ADD COLUMN class_code TEXT",
        "ALTER TABLE show_judging_classes ADD COLUMN description TEXT",
        "ALTER TABLE show_judging_classes ADD COLUMN year_min INTEGER",
        "ALTER TABLE show_judging_classes ADD COLUMN year_max INTEGER",
        "ALTER TABLE show_judging_classes ADD COLUMN make_contains TEXT",
        "ALTER TABLE show_judging_classes ADD COLUMN model_contains TEXT",
        "ALTER TABLE show_judging_classes ADD COLUMN keyword_contains TEXT",
        "ALTER TABLE show_judging_classes ADD COLUMN award_places INTEGER NOT NULL DEFAULT 3",
        "ALTER TABLE show_judging_classes ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 100",
        "ALTER TABLE show_judging_classes ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1",
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
            charity_opt_in INTEGER NOT NULL DEFAULT 0,
            consent_text TEXT,
            consent_version TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )

    for sql in [
        "ALTER TABLE people ADD COLUMN sponsor_opt_in INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE people ADD COLUMN charity_opt_in INTEGER NOT NULL DEFAULT 0",
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
        "ALTER TABLE show_cars ADD COLUMN registration_slot_id INTEGER",
        "ALTER TABLE show_cars ADD COLUMN waiver_received INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE show_cars ADD COLUMN waiver_received_at TEXT",
        "ALTER TABLE show_cars ADD COLUMN waiver_received_by TEXT",
        "ALTER TABLE show_cars ADD COLUMN insurance_carrier TEXT",
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
        "ALTER TABLE show_cars ADD COLUMN judging_class_id INTEGER",
        "ALTER TABLE show_cars ADD COLUMN class_needs_review INTEGER NOT NULL DEFAULT 0",
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
            charity_opt_in INTEGER NOT NULL DEFAULT 0,
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
        "ALTER TABLE registration_intents ADD COLUMN registration_slot_id INTEGER",
        "ALTER TABLE registration_intents ADD COLUMN waiver_text_sha256 TEXT",
        "ALTER TABLE registration_intents ADD COLUMN waiver_template_id INTEGER",
        "ALTER TABLE registration_intents ADD COLUMN insurance_carrier TEXT",
        "ALTER TABLE registration_intents ADD COLUMN registration_slot_ids TEXT",
        "ALTER TABLE registration_intents ADD COLUMN charity_opt_in INTEGER NOT NULL DEFAULT 0",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS show_registration_slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            slot_label TEXT NOT NULL,
            slot_date TEXT,
            cars_arrive_time TEXT,
            start_time TEXT,
            end_time TEXT,
            participant_instructions TEXT,
            capacity INTEGER NOT NULL DEFAULT 0,
            sort_order INTEGER NOT NULL DEFAULT 100,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )

    for sql in [
        "ALTER TABLE show_registration_slots ADD COLUMN cars_arrive_time TEXT",
        "ALTER TABLE show_registration_slots ADD COLUMN participant_instructions TEXT",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS show_car_registration_slots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            show_car_id INTEGER NOT NULL,
            registration_slot_id INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(show_car_id, registration_slot_id),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(show_car_id) REFERENCES show_cars(id),
            FOREIGN KEY(registration_slot_id) REFERENCES show_registration_slots(id)
        )
        """
    )

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_show_registration_slots_show_id ON show_registration_slots(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_show_cars_registration_slot ON show_cars(show_id, registration_slot_id)",
        "CREATE INDEX IF NOT EXISTS idx_registration_intents_slot ON registration_intents(show_id, registration_slot_id)",
        "CREATE INDEX IF NOT EXISTS idx_show_car_registration_slots_show ON show_car_registration_slots(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_show_car_registration_slots_car ON show_car_registration_slots(show_car_id)",
        "CREATE INDEX IF NOT EXISTS idx_show_car_registration_slots_slot ON show_car_registration_slots(registration_slot_id)",
    ]:
        cur.execute(sql)

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS show_judging_classes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            class_code TEXT,
            class_name TEXT NOT NULL,
            description TEXT,
            sort_order INTEGER NOT NULL DEFAULT 100,
            is_active INTEGER NOT NULL DEFAULT 1,
            year_min INTEGER,
            year_max INTEGER,
            make_contains TEXT,
            model_contains TEXT,
            keyword_contains TEXT,
            award_places INTEGER NOT NULL DEFAULT 3,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_show_judging_classes_show_id ON show_judging_classes(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_show_cars_judging_class ON show_cars(show_id, judging_class_id)",
    ]:
        cur.execute(sql)

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
            entry_method TEXT NOT NULL DEFAULT 'car_qr',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(show_car_id) REFERENCES show_cars(id)
        )
        """
    )
    try:
        cur.execute("ALTER TABLE votes ADD COLUMN entry_method TEXT NOT NULL DEFAULT 'car_qr'")
    except sqlite3.OperationalError:
        pass

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
            entry_method TEXT NOT NULL DEFAULT 'car_qr',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            paid_at TEXT,
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(show_car_id) REFERENCES show_cars(id)
        )
        """
    )
    try:
        cur.execute("ALTER TABLE vote_intents ADD COLUMN entry_method TEXT NOT NULL DEFAULT 'car_qr'")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_ballots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            ballot_token TEXT NOT NULL UNIQUE,
            ballot_label TEXT,
            source TEXT NOT NULL DEFAULT 'manual',
            entered_by TEXT,
            status TEXT NOT NULL DEFAULT 'accepted',
            notes TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_ballot_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            paper_ballot_id INTEGER NOT NULL,
            show_id INTEGER NOT NULL,
            judging_class_id INTEGER NOT NULL,
            placement INTEGER NOT NULL,
            selected_show_car_id INTEGER NOT NULL,
            selected_car_number INTEGER NOT NULL,
            category TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(paper_ballot_id, judging_class_id, placement),
            FOREIGN KEY(paper_ballot_id) REFERENCES paper_ballots(id),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(judging_class_id) REFERENCES show_judging_classes(id),
            FOREIGN KEY(selected_show_car_id) REFERENCES show_cars(id)
        )
        """
    )

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_paper_ballots_show_id ON paper_ballots(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_paper_ballot_votes_show_class ON paper_ballot_votes(show_id, judging_class_id)",
    ]:
        cur.execute(sql)


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
            charity_opt_in INTEGER NOT NULL DEFAULT 0,
            consent_text TEXT,
            consent_version TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )
    try:
        cur.execute("ALTER TABLE attendees ADD COLUMN charity_opt_in INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass

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
            charity_opt_in INTEGER NOT NULL DEFAULT 0,
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
    try:
        cur.execute("ALTER TABLE waiver_evidence ADD COLUMN charity_opt_in INTEGER NOT NULL DEFAULT 0")
    except sqlite3.OperationalError:
        pass

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


    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            global_role TEXT NOT NULL DEFAULT 'show_owner',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_user_show_roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user_id INTEGER NOT NULL,
            show_id INTEGER NOT NULL,
            role TEXT NOT NULL DEFAULT 'show_owner',
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(admin_user_id, show_id, role),
            FOREIGN KEY(admin_user_id) REFERENCES admin_users(id),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_admin_users_email ON admin_users(email)",
        "CREATE INDEX IF NOT EXISTS idx_admin_users_active ON admin_users(is_active)",
        "CREATE INDEX IF NOT EXISTS idx_admin_user_show_roles_user ON admin_user_show_roles(admin_user_id)",
        "CREATE INDEX IF NOT EXISTS idx_admin_user_show_roles_show ON admin_user_show_roles(show_id)",
    ]:
        cur.execute(sql)

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS show_voters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            show_car_id INTEGER,
            voter_token TEXT NOT NULL UNIQUE,
            voter_type TEXT NOT NULL DEFAULT 'participant',
            display_name TEXT,
            email TEXT,
            phone TEXT,
            is_active INTEGER NOT NULL DEFAULT 1,
            activated_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT,
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(show_car_id) REFERENCES show_cars(id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS restricted_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            voter_id INTEGER NOT NULL,
            category_key TEXT NOT NULL,
            judging_class_id INTEGER,
            selected_show_car_id INTEGER NOT NULL,
            vote_weight INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT,
            UNIQUE(show_id, voter_id, category_key),
            FOREIGN KEY(show_id) REFERENCES shows(id),
            FOREIGN KEY(voter_id) REFERENCES show_voters(id),
            FOREIGN KEY(judging_class_id) REFERENCES show_judging_classes(id),
            FOREIGN KEY(selected_show_car_id) REFERENCES show_cars(id)
        )
        """
    )

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_show_voters_show ON show_voters(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_show_voters_car ON show_voters(show_id, show_car_id)",
        "CREATE INDEX IF NOT EXISTS idx_show_voters_token ON show_voters(voter_token)",
        "CREATE INDEX IF NOT EXISTS idx_restricted_votes_show ON restricted_votes(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_restricted_votes_voter ON restricted_votes(voter_id)",
        "CREATE INDEX IF NOT EXISTS idx_restricted_votes_selected_car ON restricted_votes(selected_show_car_id)",
    ]:
        cur.execute(sql)


    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS contact_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            subject TEXT NOT NULL,
            message TEXT NOT NULL,
            source_page TEXT,
            status TEXT NOT NULL DEFAULT 'new',
            email_sent INTEGER NOT NULL DEFAULT 0,
            email_error TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            read_at TEXT,
            archived_at TEXT
        )
        """
    )

    for sql in [
        "ALTER TABLE contact_messages ADD COLUMN phone TEXT",
        "ALTER TABLE contact_messages ADD COLUMN source_page TEXT",
        "ALTER TABLE contact_messages ADD COLUMN status TEXT NOT NULL DEFAULT 'new'",
        "ALTER TABLE contact_messages ADD COLUMN email_sent INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE contact_messages ADD COLUMN email_error TEXT",
        "ALTER TABLE contact_messages ADD COLUMN read_at TEXT",
        "ALTER TABLE contact_messages ADD COLUMN archived_at TEXT",
    ]:
        try:
            cur.execute(sql)
        except sqlite3.OperationalError:
            pass

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
        "CREATE INDEX IF NOT EXISTS idx_contact_messages_status_created ON contact_messages(status, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_contact_messages_created ON contact_messages(created_at)",
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


def list_public_registerable_shows() -> List[sqlite3.Row]:
    """Public show list for /register picker.

    Includes active and upcoming shows that are visible on the site. This allows
    the platform to support several simultaneous shows where some are in day-of
    mode and others are still accepting preregistration.
    """
    conn = _conn()
    rows = conn.execute(
        """
        SELECT *
        FROM shows
        WHERE show_on_site = 1
          AND COALESCE(status, 'draft') IN ('active', 'upcoming')
        ORDER BY
            CASE COALESCE(status, 'draft')
                WHEN 'active' THEN 0
                WHEN 'upcoming' THEN 1
                ELSE 2
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
    show_type: str = "full",
    max_cars: Optional[int] = None,
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
    public_vote_disclosure: str = "",
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
    voting_method: str = "both",
    participant_voting_enabled: int = 0,
    payment_mode: str = "stripe",
    charity_processor_label: str = "",
    external_payment_url: str = "",
    allow_custom_votes: int = 1,
    preset_vote_options: str = "1,5,10,20,25",
    max_votes_per_checkout: int = 50,
    allow_sponsorships: int = 1,
    registration_slot_selection_mode: str = "single",
    card_headline: str = "",
    card_subheadline: str = "",
    card_layout_mode: str = "auto",
) -> int:
    conn = _conn()
    cur = conn.cursor()
    show_type_clean = (show_type or "full").strip().lower().replace("-", "_")
    if show_type_clean in {"cruisein", "cruise"}:
        show_type_clean = "cruise_in"
    if show_type_clean not in {"full", "popup", "cruise_in"}:
        show_type_clean = "full"

    voting_mode_clean = (voting_mode or "fundraiser_unlimited").strip().lower()
    if voting_mode_clean not in {
        "fundraiser_unlimited",
        "restricted_single",
        "participant_restricted",
        "participant_only",
        "judge_only",
        "none",
    }:
        voting_mode_clean = "fundraiser_unlimited"

    voting_method_clean = normalize_voting_method(voting_method, default="both")

    payment_mode_clean = (payment_mode or "stripe").strip().lower()
    if payment_mode_clean not in {"stripe", "external", "none"}:
        payment_mode_clean = "stripe"

    card_layout_mode_clean = (card_layout_mode or "auto").strip().lower()
    if card_layout_mode_clean not in {"auto", "voting", "information", "sponsor"}:
        card_layout_mode_clean = "auto"

    cur.execute(
        """
        INSERT INTO shows (
            slug, flyer_image_path, title, show_type, max_cars, date, time, cars_arrive_time, day_of_registration_time,
            show_start_time, show_end_time, location_name, address, benefiting,
            suggested_donation, description, status, short_details, public_vote_disclosure, qr_message,
            cta_label, cta_url, show_on_site, sort_order, hide_address, voting_open, is_active,
            waiver_template_id, organizer_name, venue_name, venue_address_line1, venue_address_line2,
            venue_city, venue_state, venue_zip, charity_name, charity_description,
            voting_mode, voting_method, payment_mode, charity_processor_label, external_payment_url, allow_custom_votes, preset_vote_options, max_votes_per_checkout, allow_sponsorships, registration_slot_selection_mode,
            card_headline, card_subheadline, card_layout_mode
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            slug.strip(),
            (flyer_image_path or "").strip(),
            title.strip(),
            show_type_clean,
            int(max_cars) if max_cars else None,
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
            (public_vote_disclosure or "").strip(),
            (qr_message or "").strip(),
            (cta_label or "").strip(),
            (cta_url or "").strip(),
            int(show_on_site),
            int(sort_order),
            int(hide_address),
            0,
            0,
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
            voting_mode_clean,
            voting_method_clean,
            payment_mode_clean,
            (charity_processor_label or "").strip(),
            (external_payment_url or "").strip(),
            int(allow_custom_votes),
            (preset_vote_options or "1,5,10,20,25").strip(),
            int(max_votes_per_checkout or 50),
            1 if int(allow_sponsorships or 0) == 1 else 0,
            normalize_registration_slot_selection_mode(registration_slot_selection_mode),
            (card_headline or "").strip(),
            (card_subheadline or "").strip(),
            card_layout_mode_clean,
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
    show_type: str = "full",
    max_cars: Optional[int] = None,
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
    public_vote_disclosure: str = "",
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
    voting_method: str = "qr_only",
    participant_voting_enabled: int = 0,
    payment_mode: str = "stripe",
    charity_processor_label: str = "",
    external_payment_url: str = "",
    allow_custom_votes: int = 1,
    preset_vote_options: str = "1,5,10,20,25",
    max_votes_per_checkout: int = 50,
    allow_sponsorships: int = 1,
    registration_slot_selection_mode: str = "single",
    card_headline: str = "",
    card_subheadline: str = "",
    card_layout_mode: str = "auto",
) -> None:
    card_layout_mode_clean = (card_layout_mode or "auto").strip().lower()
    if card_layout_mode_clean not in {"auto", "voting", "information", "sponsor"}:
        card_layout_mode_clean = "auto"

    conn = _conn()
    conn.execute(
        """
        UPDATE shows
        SET slug = ?, title = ?, show_type = ?, max_cars = ?, flyer_image_path = ?, date = ?, time = ?,
            cars_arrive_time = ?, day_of_registration_time = ?, show_start_time = ?, show_end_time = ?,
            location_name = ?, address = ?, benefiting = ?, suggested_donation = ?, description = ?, status = ?,
            short_details = ?, public_vote_disclosure = ?, qr_message = ?, cta_label = ?, cta_url = ?,
            show_on_site = ?, sort_order = ?, hide_address = ?, waiver_template_id = ?,
            organizer_name = ?, venue_name = ?, venue_address_line1 = ?, venue_address_line2 = ?,
            venue_city = ?, venue_state = ?, venue_zip = ?, charity_name = ?, charity_description = ?,
            voting_mode = ?, voting_method = ?, participant_voting_enabled = ?, payment_mode = ?, charity_processor_label = ?, external_payment_url = ?, allow_custom_votes = ?,
            preset_vote_options = ?, max_votes_per_checkout = ?, allow_sponsorships = ?, registration_slot_selection_mode = ?,
            card_headline = ?, card_subheadline = ?, card_layout_mode = ?
        WHERE id = ?
        """,
        (
            slug.strip(),
            title.strip(),
            (show_type or "full").strip().lower().replace("-", "_"),
            int(max_cars) if max_cars else None,
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
            (public_vote_disclosure or "").strip(),
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
            normalize_voting_method(voting_method, default="qr_only"),
            1 if int(participant_voting_enabled or 0) == 1 or (voting_mode or "").strip().lower() in {"participant_restricted", "participant_only", "judge_only"} else 0,
            (payment_mode or "stripe").strip(),
            (charity_processor_label or "").strip(),
            (external_payment_url or "").strip(),
            int(allow_custom_votes),
            (preset_vote_options or "1,5,10,20,25").strip(),
            int(max_votes_per_checkout or 50),
            1 if int(allow_sponsorships or 0) == 1 else 0,
            normalize_registration_slot_selection_mode(registration_slot_selection_mode),
            (card_headline or "").strip(),
            (card_subheadline or "").strip(),
            card_layout_mode_clean,
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
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM show_cars
        WHERE show_id = ?
          AND COALESCE(is_placeholder, 0) = 0
          AND COALESCE(registration_state, '') != 'removed'
          AND COALESCE(registration_payment_status, '') NOT IN ('removed', 'canceled', 'refunded')
        """,
        (show_id,),
    ).fetchone()
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

# REGISTRATION SLOTS / MULTI-DAY CAPACITY

def _row_has_column(row: Any, column: str) -> bool:
    try:
        return column in row.keys()
    except Exception:
        return False


def normalize_registration_slot_selection_mode(value: str) -> str:
    value = (value or "single").strip().lower()
    return value if value in {"single", "multiple"} else "single"


def get_show_registration_slot_selection_mode(show_id: int) -> str:
    conn = _conn()
    row = conn.execute(
        "SELECT registration_slot_selection_mode FROM shows WHERE id = ? LIMIT 1",
        (int(show_id),),
    ).fetchone()
    conn.close()
    if not row or "registration_slot_selection_mode" not in row.keys():
        return "single"
    return normalize_registration_slot_selection_mode(row["registration_slot_selection_mode"])


def _normalize_slot_ids(slot_ids: Optional[List[int]]) -> List[int]:
    out: List[int] = []
    for raw in slot_ids or []:
        try:
            sid = int(raw)
        except Exception:
            continue
        if sid > 0 and sid not in out:
            out.append(sid)
    return out


def _slot_ids_json(slot_ids: Optional[List[int]]) -> str:
    return json.dumps(_normalize_slot_ids(slot_ids))


def _slot_ids_from_json(value: str) -> List[int]:
    try:
        data = json.loads(value or "[]")
    except Exception:
        data = []
    return _normalize_slot_ids(data if isinstance(data, list) else [])


def validate_registration_slot_ids(show_id: int, slot_ids: Optional[List[int]]) -> List[int]:
    slot_ids = _normalize_slot_ids(slot_ids)
    if not slot_ids:
        return []
    conn = _conn()
    rows = conn.execute(
        f"""
        SELECT id
        FROM show_registration_slots
        WHERE show_id = ?
          AND COALESCE(is_active, 1) = 1
          AND id IN ({','.join(['?'] * len(slot_ids))})
        ORDER BY sort_order ASC, id ASC
        """,
        [int(show_id)] + slot_ids,
    ).fetchall()
    conn.close()
    valid = [int(r["id"]) for r in rows]
    return [sid for sid in slot_ids if sid in valid]


def selected_registration_slots_have_capacity(show_id: int, slot_ids: Optional[List[int]]) -> bool:
    for slot_id in _normalize_slot_ids(slot_ids):
        if not show_slot_has_capacity(show_id, slot_id):
            return False
    return True


def save_show_car_registration_slots(show_id: int, show_car_id: int, slot_ids: Optional[List[int]]) -> None:
    slot_ids = validate_registration_slot_ids(show_id, slot_ids)
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM show_car_registration_slots WHERE show_car_id = ?", (int(show_car_id),))
        for slot_id in slot_ids:
            cur.execute(
                """
                INSERT OR IGNORE INTO show_car_registration_slots (show_id, show_car_id, registration_slot_id)
                VALUES (?, ?, ?)
                """,
                (int(show_id), int(show_car_id), int(slot_id)),
            )
        cur.execute(
            "UPDATE show_cars SET registration_slot_id = ? WHERE id = ?",
            (int(slot_ids[0]) if slot_ids else None, int(show_car_id)),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def registration_slot_labels_for_car(show_car_id: int) -> str:
    conn = _conn()
    row = conn.execute(
        """
        SELECT GROUP_CONCAT(slot_label, ', ') AS labels
        FROM (
            SELECT DISTINCT s.slot_label, s.sort_order, s.id
            FROM show_registration_slots s
            JOIN show_car_registration_slots x ON x.registration_slot_id = s.id
            WHERE x.show_car_id = ?
            ORDER BY s.sort_order ASC, s.id ASC
        )
        """,
        (int(show_car_id),),
    ).fetchone()
    conn.close()
    return (row["labels"] if row else "") or ""


def count_registered_cars_for_slot(show_id: int, registration_slot_id: Optional[int]) -> int:
    if not registration_slot_id:
        return 0
    conn = _conn()
    row = conn.execute(
        """
        SELECT COUNT(DISTINCT sc.id) AS cnt
        FROM show_cars sc
        LEFT JOIN show_car_registration_slots x
          ON x.show_car_id = sc.id AND x.registration_slot_id = ?
        WHERE sc.show_id = ?
          AND COALESCE(sc.is_placeholder, 0) = 0
          AND COALESCE(sc.registration_state, '') != 'removed'
          AND COALESCE(sc.registration_payment_status, '') NOT IN ('removed', 'canceled', 'refunded')
          AND (sc.registration_slot_id = ? OR x.registration_slot_id IS NOT NULL)
        """,
        (int(registration_slot_id), show_id, int(registration_slot_id)),
    ).fetchone()
    conn.close()
    return int(row["cnt"] or 0)


def show_has_registration_slots(show_id: int) -> bool:
    conn = _conn()
    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM show_registration_slots
        WHERE show_id = ? AND COALESCE(is_active, 1) = 1
        """,
        (show_id,),
    ).fetchone()
    conn.close()
    return int(row["cnt"] or 0) > 0


def get_registration_slot(show_id: int, registration_slot_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        """
        SELECT *
        FROM show_registration_slots
        WHERE show_id = ? AND id = ?
        LIMIT 1
        """,
        (show_id, int(registration_slot_id)),
    ).fetchone()
    conn.close()
    return row


def list_registration_slots(show_id: int, public_only: bool = False) -> List[sqlite3.Row]:
    conn = _conn()
    where = "WHERE s.show_id = ?"
    params: List[Any] = [show_id]
    if public_only:
        where += " AND COALESCE(s.is_active, 1) = 1"
    rows = conn.execute(
        f"""
        SELECT
            s.*,
            COALESCE(COUNT(DISTINCT sc.id), 0) AS registered_count,
            CASE
                WHEN COALESCE(s.capacity, 0) <= 0 THEN NULL
                ELSE MAX(COALESCE(s.capacity, 0) - COALESCE(COUNT(DISTINCT sc.id), 0), 0)
            END AS remaining_count,
            CASE
                WHEN COALESCE(s.capacity, 0) > 0 AND COALESCE(COUNT(DISTINCT sc.id), 0) >= COALESCE(s.capacity, 0) THEN 1
                ELSE 0
            END AS is_full
        FROM show_registration_slots s
        LEFT JOIN show_car_registration_slots x
            ON x.registration_slot_id = s.id
        LEFT JOIN show_cars sc
            ON sc.show_id = s.show_id
           AND COALESCE(sc.is_placeholder, 0) = 0
           AND COALESCE(sc.registration_state, '') != 'removed'
           AND COALESCE(sc.registration_payment_status, '') NOT IN ('removed', 'canceled', 'refunded')
           AND (sc.id = x.show_car_id OR sc.registration_slot_id = s.id)
        {where}
        GROUP BY s.id
        ORDER BY s.sort_order ASC, s.id ASC
        """,
        params,
    ).fetchall()
    conn.close()
    return rows


def show_slot_has_capacity(show_id: int, registration_slot_id: Optional[int]) -> bool:
    if not registration_slot_id:
        return not show_has_registration_slots(show_id) and show_has_capacity(show_id)
    slot = get_registration_slot(show_id, int(registration_slot_id))
    if not slot or int(slot["is_active"] or 0) != 1:
        return False
    try:
        capacity = int(slot["capacity"] or 0)
    except Exception:
        capacity = 0
    if capacity <= 0:
        return True
    return count_registered_cars_for_slot(show_id, int(registration_slot_id)) < capacity


def save_registration_slots_for_show(show_id: int, slot_payloads: List[Dict[str, Any]]) -> None:
    """Upsert registration day/session slots for a show.

    Admin page behavior:
    - A normal one-day show does not need any slot rows saved.
    - Blank rows are ignored.
    - Blank existing rows are made inactive so old empty/default rows do not keep showing.
    - Slots not submitted by the form are made inactive, not deleted, to preserve history.
    """
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        seen_ids = set()
        for idx, payload in enumerate(slot_payloads):
            raw_id = str(payload.get("id") or "").strip()
            slot_id = int(raw_id) if raw_id.isdigit() else None
            label = (payload.get("slot_label") or "").strip()
            slot_date = (payload.get("slot_date") or "").strip()
            cars_arrive_time = (payload.get("cars_arrive_time") or "").strip()
            start_time = (payload.get("start_time") or "").strip()
            end_time = (payload.get("end_time") or "").strip()
            participant_instructions = (payload.get("participant_instructions") or "").strip()
            raw_capacity = str(payload.get("capacity") or "").strip()

            has_any_content = any([
                label,
                slot_date,
                cars_arrive_time,
                start_time,
                end_time,
                participant_instructions,
                raw_capacity,
            ])

            # If an existing row was cleared on the form, keep the row for history
            # but make it inactive so it does not appear in public registration.
            if not has_any_content:
                if slot_id:
                    cur.execute(
                        """
                        UPDATE show_registration_slots
                        SET is_active = 0, updated_at = datetime('now')
                        WHERE show_id = ? AND id = ?
                        """,
                        (show_id, slot_id),
                    )
                    seen_ids.add(slot_id)
                continue

            # A row with details but no label gets a safe generic label.
            # This prevents an accidental blank label from creating a confusing public option.
            if not label:
                label = "Main show day" if idx == 0 else f"Day / Session {idx + 1}"

            try:
                capacity = int(raw_capacity or 0)
            except Exception:
                capacity = 0
            if capacity < 0:
                capacity = 0
            try:
                sort_order = int(payload.get("sort_order") or ((idx + 1) * 10))
            except Exception:
                sort_order = (idx + 1) * 10
            is_active = 1 if str(payload.get("is_active") or "").lower() in {"1", "true", "yes", "on"} else 0
            values = (
                label,
                slot_date,
                cars_arrive_time,
                start_time,
                end_time,
                participant_instructions,
                capacity,
                sort_order,
                is_active,
                show_id,
            )
            if slot_id:
                cur.execute(
                    """
                    UPDATE show_registration_slots
                    SET slot_label = ?, slot_date = ?, cars_arrive_time = ?, start_time = ?, end_time = ?, participant_instructions = ?, capacity = ?,
                        sort_order = ?, is_active = ?, updated_at = datetime('now')
                    WHERE show_id = ? AND id = ?
                    """,
                    values + (slot_id,),
                )
                seen_ids.add(slot_id)
            else:
                cur.execute(
                    """
                    INSERT INTO show_registration_slots
                        (slot_label, slot_date, cars_arrive_time, start_time, end_time, participant_instructions, capacity, sort_order, is_active, show_id, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    """,
                    values,
                )
                seen_ids.add(int(cur.lastrowid))

        # Rows hidden/removed from the admin form should no longer be active.
        # They are not deleted because old registrations may still reference them.
        if seen_ids:
            marks = ",".join("?" for _ in seen_ids)
            cur.execute(
                f"""
                UPDATE show_registration_slots
                SET is_active = 0, updated_at = datetime('now')
                WHERE show_id = ? AND id NOT IN ({marks})
                """,
                (show_id, *sorted(seen_ids)),
            )
        else:
            cur.execute(
                """
                UPDATE show_registration_slots
                SET is_active = 0, updated_at = datetime('now')
                WHERE show_id = ?
                """,
                (show_id,),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

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
    voting_method: str = "qr_only",
    participant_voting_enabled: int = 0,
    payment_mode: str = "stripe",
    charity_processor_label: str = "",
    external_payment_url: str = "",
    allow_custom_votes: int = 1,
    preset_vote_options: str = "1,5,10,20,25",
    max_votes_per_checkout: int = 50,
    waiver_text: str = "",
    waiver_version: str = "",
    registration_slot_selection_mode: str = "single",
) -> None:
    st = (show_type or "full").strip().lower()
    if st in {"cruise-in", "cruisein"}:
        st = "cruise_in"
    if st not in ("popup", "full", "cruise_in"):
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
    if voting_mode not in {"fundraiser_unlimited", "restricted_single", "none"}:
        voting_mode = "fundraiser_unlimited"
    voting_method = normalize_voting_method(voting_method, default="qr_only")

    payment_mode = (payment_mode or "stripe").strip().lower()
    if payment_mode not in {"stripe", "external", "none"}:
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
            voting_method = ?,
            payment_mode = ?,
            charity_processor_label = ?,
            external_payment_url = ?,
            allow_custom_votes = ?,
            preset_vote_options = ?,
            max_votes_per_checkout = ?,
            waiver_text = CASE WHEN TRIM(?) <> '' THEN ? ELSE waiver_text END,
            waiver_version = CASE WHEN TRIM(?) <> '' THEN ? ELSE waiver_version END,
            registration_slot_selection_mode = ?
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
            voting_method,
            payment_mode,
            (charity_processor_label or "").strip(),
            (external_payment_url or "").strip(),
            allow_custom_votes,
            preset_vote_options,
            max_votes_per_checkout,
            (waiver_text or "").strip(),
            (waiver_text or "").strip(),
            (waiver_version or "").strip(),
            (waiver_version or "").strip(),
            normalize_registration_slot_selection_mode(registration_slot_selection_mode),
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

def create_person(name: str, phone: str, email: str, opt_in_future: bool, sponsor_opt_in: bool, consent_text: str, consent_version: str, charity_opt_in: bool = False) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO people (name, phone, email, opt_in_future, sponsor_opt_in, charity_opt_in, consent_text, consent_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (name, phone, email, _b(opt_in_future), _b(sponsor_opt_in), _b(charity_opt_in), consent_text, consent_version),
    )
    conn.commit()
    pid = int(cur.lastrowid)
    conn.close()
    return pid


def update_person(person_id: int, name: str, phone: str, email: str, opt_in_future: bool, sponsor_opt_in: bool, consent_text: str, consent_version: str, charity_opt_in: bool = False) -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE people
        SET name = ?, phone = ?, email = ?, opt_in_future = ?, sponsor_opt_in = ?, charity_opt_in = ?, consent_text = ?, consent_version = ?
        WHERE id = ?
        """,
        (name, phone, email, _b(opt_in_future), _b(sponsor_opt_in), _b(charity_opt_in), consent_text, consent_version, person_id),
    )
    conn.commit()
    conn.close()


def create_show_car(show_id: int, person_id: int, car_number: int, year: str, make: str, model: str) -> Tuple[int, str]:
    conn = _conn()
    cur = conn.cursor()

    show = cur.execute(
        "SELECT max_cars FROM shows WHERE id = ? LIMIT 1",
        (show_id,),
    ).fetchone()

    if show and show["max_cars"] is not None:
        try:
            max_cars = int(show["max_cars"])
        except Exception:
            max_cars = None

        if max_cars and max_cars > 0:
            row = cur.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM show_cars
                WHERE show_id = ?
                  AND COALESCE(is_placeholder, 0) = 0
                """,
                (show_id,),
            ).fetchone()
            if int(row["cnt"] or 0) >= max_cars:
                conn.close()
                raise ValueError("This show has reached its maximum number of cars.")

    token = _new_car_token()
    try:
        cur.execute(
            """
            INSERT INTO show_cars (
                show_id, person_id, car_number, car_token, year, make, model,
                is_placeholder, registration_state, registration_payment_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 'claimed', 'paid')
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

def update_show_car_details(show_car_id: int, year: str, make: str, model: str, insurance_carrier: str = "") -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE show_cars
        SET year = ?,
            make = ?,
            model = ?,
            insurance_carrier = ?
        WHERE id = ?
        """,
        (year, make, model, (insurance_carrier or "").strip(), show_car_id),
    )
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
            p.consent_version,
            COALESCE((
                SELECT GROUP_CONCAT(label, ', ')
                FROM (
                    SELECT DISTINCT s2.slot_label AS label, s2.sort_order, s2.id
                    FROM show_registration_slots s2
                    JOIN show_car_registration_slots x2 ON x2.registration_slot_id = s2.id
                    WHERE x2.show_car_id = sc.id
                    ORDER BY s2.sort_order ASC, s2.id ASC
                )
            ), slot.slot_label, '') AS registration_slot_labels
        FROM show_cars sc
        LEFT JOIN show_registration_slots slot ON slot.id = sc.registration_slot_id
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


def normalize_voting_method(value: str, default: str = "qr_only") -> str:
    clean = (value or default or "qr_only").strip().lower().replace("-", "_")
    aliases = {
        "both_qr_and_car_number": "both",
        "both_qr_number": "both",
        "both": "both",
        "car_specific_qr": "qr_only",
        "car_qr": "qr_only",
        "qr": "qr_only",
        "qr_only": "qr_only",
        "car_number": "number_only",
        "number": "number_only",
        "number_only": "number_only",
        "manual": "number_only",
        "disabled": "disabled",
        "none": "disabled",
    }
    return aliases.get(clean, default if default in {"both", "qr_only", "number_only", "disabled"} else "qr_only")


def find_vote_car_by_number(show_id: int, car_number_raw: str) -> Dict[str, Any]:
    raw = (car_number_raw or "").strip()
    digits = re.sub(r"\D+", "", raw)
    if not digits:
        return {"status": "invalid", "message": "Enter a car number to continue.", "car": None, "suggestions": []}

    normalized = str(int(digits))
    conn = _conn()
    try:
        rows = conn.execute(
            """
            SELECT sc.*, p.name AS owner_name
            FROM show_cars sc
            JOIN people p ON p.id = sc.person_id
            WHERE sc.show_id = ?
              AND CAST(sc.car_number AS INTEGER) = ?
              AND COALESCE(sc.is_placeholder, 0) = 0
              AND COALESCE(sc.registration_payment_status, '') NOT IN ('removed', 'canceled', 'cancelled', 'refunded', 'inactive')
              AND COALESCE(sc.registration_state, '') NOT IN ('removed', 'canceled', 'cancelled', 'inactive')
              AND COALESCE(sc.checked_in_at, '') != ''
            ORDER BY sc.car_number ASC
            """,
            (int(show_id), int(normalized)),
        ).fetchall()
        if len(rows) == 1:
            return {"status": "ok", "message": "", "car": rows[0], "suggestions": []}
        if len(rows) > 1:
            return {
                "status": "ambiguous",
                "message": "That car number matches more than one active checked-in car. Please ask event staff to confirm the number.",
                "car": None,
                "suggestions": [],
            }

        nearby = conn.execute(
            """
            SELECT car_number
            FROM show_cars
            WHERE show_id = ?
              AND COALESCE(is_placeholder, 0) = 0
              AND COALESCE(registration_payment_status, '') NOT IN ('removed', 'canceled', 'cancelled', 'refunded', 'inactive')
              AND COALESCE(registration_state, '') NOT IN ('removed', 'canceled', 'cancelled', 'inactive')
              AND COALESCE(checked_in_at, '') != ''
            ORDER BY ABS(CAST(car_number AS INTEGER) - ?) ASC, car_number ASC
            LIMIT 3
            """,
            (int(show_id), int(normalized)),
        ).fetchall()
        return {
            "status": "not_found",
            "message": "We could not find an active checked-in car with that number. Please check the number and try again.",
            "car": None,
            "suggestions": [int(r["car_number"]) for r in nearby],
        }
    finally:
        conn.close()

def get_next_available_car_number(show_id: int) -> int:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT car_number
        FROM show_cars
        WHERE show_id = ?
        ORDER BY car_number ASC
        """,
        (show_id,),
    ).fetchall()
    conn.close()

    used = {int(r["car_number"]) for r in rows}
    n = 1
    while n in used:
        n += 1
    return n


def search_show_cars_admin(show_id: int, query: str) -> List[sqlite3.Row]:
    q = (query or "").strip()
    conn = _conn()

    base_select = """
        SELECT
            sc.id,
            sc.show_id,
            sc.person_id,
            sc.car_number,
            sc.car_token,
            sc.year,
            sc.make,
            sc.model,
            sc.registration_payment_status,
            sc.registration_amount_cents,
            sc.registration_session_id,
            sc.waiver_received,
            sc.waiver_received_at,
            sc.waiver_received_by,
            sc.waiver_signed_name,
            sc.waiver_signed_at,
            sc.is_placeholder,
            sc.registration_state,
            sc.checked_in_at,
            sc.registration_slot_id,
            sc.judging_class_id,
            sc.class_needs_review,
            jc.class_name AS judging_class_name,
            jc.class_code AS judging_class_code,
            slot.slot_label,
            slot.slot_date,
            slot.cars_arrive_time as slot_cars_arrive_time,
            slot.start_time as slot_start_time,
            slot.end_time as slot_end_time,
            slot.participant_instructions as slot_participant_instructions,
            COALESCE((
                SELECT GROUP_CONCAT(label, ', ')
                FROM (
                    SELECT DISTINCT s2.slot_label AS label, s2.sort_order, s2.id
                    FROM show_registration_slots s2
                    JOIN show_car_registration_slots x2 ON x2.registration_slot_id = s2.id
                    WHERE x2.show_car_id = sc.id
                    ORDER BY s2.sort_order ASC, s2.id ASC
                )
            ), slot.slot_label, '') AS registration_slot_labels,
            p.name AS owner_name,
            p.phone AS owner_phone,
            p.email AS owner_email,
            p.opt_in_future,
            p.sponsor_opt_in
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        LEFT JOIN show_registration_slots slot ON slot.id = sc.registration_slot_id
        LEFT JOIN show_judging_classes jc ON jc.id = sc.judging_class_id
    """

    if not q:
        rows = conn.execute(
            base_select + """
            WHERE sc.show_id = ?
            ORDER BY sc.car_number ASC
            LIMIT 250
            """,
            (show_id,),
        ).fetchall()
        conn.close()
        return rows

    like_q = f"%{q}%"
    number = None
    try:
        number = int(q)
    except Exception:
        number = None

    if number is not None:
        rows = conn.execute(
            base_select + """
            WHERE sc.show_id = ?
              AND (
                    sc.car_number = ?
                 OR p.name LIKE ?
                 OR p.phone LIKE ?
                 OR p.email LIKE ?
                 OR sc.year LIKE ?
                 OR sc.make LIKE ?
                 OR sc.model LIKE ?
              )
            ORDER BY sc.car_number ASC
            LIMIT 250
            """,
            (show_id, number, like_q, like_q, like_q, like_q, like_q, like_q),
        ).fetchall()
    else:
        rows = conn.execute(
            base_select + """
            WHERE sc.show_id = ?
              AND (
                    p.name LIKE ?
                 OR p.phone LIKE ?
                 OR p.email LIKE ?
                 OR sc.year LIKE ?
                 OR sc.make LIKE ?
                 OR sc.model LIKE ?
              )
            ORDER BY sc.car_number ASC
            LIMIT 250
            """,
            (show_id, like_q, like_q, like_q, like_q, like_q, like_q),
        ).fetchall()

    conn.close()
    return rows

def get_show_car_admin_by_id(show_id: int, show_car_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        """
        SELECT
            sc.*,
            p.name AS owner_name,
            p.phone AS owner_phone,
            p.email AS owner_email,
            p.opt_in_future,
            p.sponsor_opt_in,
            jc.class_name AS judging_class_name,
            jc.class_code AS judging_class_code,
            slot.slot_label,
            slot.slot_date,
            slot.cars_arrive_time AS slot_cars_arrive_time,
            slot.start_time AS slot_start_time,
            slot.end_time AS slot_end_time,
            slot.participant_instructions AS slot_participant_instructions,
            COALESCE((
                SELECT GROUP_CONCAT(label, ', ')
                FROM (
                    SELECT DISTINCT s2.slot_label AS label, s2.sort_order, s2.id
                    FROM show_registration_slots s2
                    JOIN show_car_registration_slots x2 ON x2.registration_slot_id = s2.id
                    WHERE x2.show_car_id = sc.id
                    ORDER BY s2.sort_order ASC, s2.id ASC
                )
            ), slot.slot_label, '') AS registration_slot_labels
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        LEFT JOIN show_registration_slots slot ON slot.id = sc.registration_slot_id
        LEFT JOIN show_judging_classes jc ON jc.id = sc.judging_class_id
        WHERE sc.show_id = ? AND sc.id = ?
        LIMIT 1
        """,
        (int(show_id), int(show_car_id)),
    ).fetchone()
    conn.close()
    return row

def update_show_car_admin_registration(
    *,
    show_id: int,
    show_car_id: int,
    owner_name: str,
    phone: str,
    email: str,
    year: str,
    make: str,
    model: str,
    insurance_carrier: str = "",
    registration_payment_status: str = "",
    registration_slot_ids: Optional[List[int]] = None,
) -> None:
    conn = _conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN IMMEDIATE")

        car = cur.execute(
            "SELECT * FROM show_cars WHERE show_id = ? AND id = ? LIMIT 1",
            (int(show_id), int(show_car_id)),
        ).fetchone()

        if not car:
            raise ValueError("Car registration not found.")

        person_id = int(car["person_id"])
        slot_ids = _normalize_slot_ids(registration_slot_ids or [])
        mode = get_show_registration_slot_selection_mode(int(show_id))
        if mode == "single" and len(slot_ids) > 1:
            raise ValueError("This show only allows one selected day/session for each registration.")
        slot_ids = validate_registration_slot_ids(int(show_id), slot_ids) if slot_ids else []
        primary_slot_id = int(slot_ids[0]) if slot_ids else None

        valid_statuses = {
            "paid",
            "paid_cash",
            "pending",
            "cash_pending",
            "manual_paid",
            "comped",
            "canceled",
            "refunded",
            "removed",
            "placeholder",
        }
        payment_status = (registration_payment_status or "").strip()
        if payment_status not in valid_statuses:
            payment_status = car["registration_payment_status"] or "pending"

        # Capacity release rule:
        # Canceled, refunded, or removed registrations do not hold a car spot.
        # This allows registration to reopen automatically when capacity becomes available.
        # A hard admin close still wins because prereg_allowed(show) is checked before capacity.
        releases_capacity = payment_status in {"canceled", "refunded", "removed"}
        if releases_capacity:
            slot_ids = []
            primary_slot_id = None

        cur.execute(
            """
            UPDATE people
            SET name = ?, phone = ?, email = ?
            WHERE id = ?
            """,
            (
                (owner_name or "").strip(),
                (phone or "").strip(),
                (email or "").strip().lower(),
                person_id,
            ),
        )

        cur.execute(
            """
            UPDATE show_cars
            SET year = ?,
                make = ?,
                model = ?,
                insurance_carrier = ?,
                registration_payment_status = ?,
                registration_slot_id = ?
            WHERE id = ? AND show_id = ?
            """,
            (
                (year or "").strip(),
                (make or "").strip(),
                (model or "").strip(),
                (insurance_carrier or "").strip(),
                payment_status,
                primary_slot_id,
                int(show_car_id),
                int(show_id),
            ),
        )

        cur.execute(
            "DELETE FROM show_car_registration_slots WHERE show_id = ? AND show_car_id = ?",
            (int(show_id), int(show_car_id)),
        )

        # If the registration was canceled/refunded/removed, leave it detached from all slots.
        # This frees both overall show capacity and per-slot capacity.
        if releases_capacity:
            conn.commit()
            return

        for slot_id in slot_ids:
            cur.execute(
                """
                INSERT OR IGNORE INTO show_car_registration_slots
                    (show_id, show_car_id, registration_slot_id)
                VALUES (?, ?, ?)
                """,
                (int(show_id), int(show_car_id), int(slot_id)),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()



def remove_show_car_registration(*, show_id: int, show_car_id: int, removed_by: str = "admin") -> None:
    """Soft-remove a registration so it no longer counts toward show or slot capacity.

    This keeps the car/person record for history and audit, but clears selected slots,
    check-in state, and marks the registration as removed.
    """
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        car = cur.execute(
            "SELECT * FROM show_cars WHERE show_id = ? AND id = ? LIMIT 1",
            (int(show_id), int(show_car_id)),
        ).fetchone()
        if not car:
            raise ValueError("Car registration not found.")

        cur.execute(
            "DELETE FROM show_car_registration_slots WHERE show_id = ? AND show_car_id = ?",
            (int(show_id), int(show_car_id)),
        )
        cur.execute(
            """
            UPDATE show_cars
            SET registration_state = 'removed',
                registration_payment_status = 'removed',
                registration_slot_id = NULL,
                checked_in_at = NULL
            WHERE show_id = ? AND id = ?
            """,
            (int(show_id), int(show_car_id)),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

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
            sc.registration_slot_id,
            sc.judging_class_id,
            sc.class_needs_review,
            jc.class_name AS judging_class_name,
            jc.class_code AS judging_class_code,
            slot.slot_label,
            slot.slot_date,
            slot.cars_arrive_time as slot_cars_arrive_time,
            slot.start_time as slot_start_time,
            slot.end_time as slot_end_time,
            slot.participant_instructions as slot_participant_instructions,
            COALESCE((
                SELECT GROUP_CONCAT(label, ', ')
                FROM (
                    SELECT DISTINCT s2.slot_label AS label, s2.sort_order, s2.id
                    FROM show_registration_slots s2
                    JOIN show_car_registration_slots x2 ON x2.registration_slot_id = s2.id
                    WHERE x2.show_car_id = sc.id
                    ORDER BY s2.sort_order ASC, s2.id ASC
                )
            ), slot.slot_label, '') AS registration_slot_labels,
            p.name as owner_name
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        LEFT JOIN show_registration_slots slot ON slot.id = sc.registration_slot_id
        LEFT JOIN show_judging_classes jc ON jc.id = sc.judging_class_id
        WHERE sc.show_id = ?
          AND COALESCE(sc.registration_state, '') != 'removed'
          AND COALESCE(sc.registration_payment_status, '') NOT IN ('removed', 'canceled', 'refunded')
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
    charity_opt_in: bool,
    year: str,
    make: str,
    model: str,
    waiver_signed_name: str,
    waiver_text: str,
    waiver_version: str,
    amount_cents: int,
    insurance_carrier: str = "",
    waiver_accepted: bool = False,
    waiver_template_id: Optional[int] = None,
    reserved_car_number: Optional[int] = None,
    registration_slot_id: Optional[int] = None,
    registration_slot_ids: Optional[List[int]] = None,
) -> Tuple[int, str, int]:
    conn = _conn()
    cur = conn.cursor()

    try:
        cur.execute("BEGIN IMMEDIATE")

        slot_ids = _normalize_slot_ids(registration_slot_ids or ([registration_slot_id] if registration_slot_id else []))
        mode = get_show_registration_slot_selection_mode(show_id)
        if mode == "single" and len(slot_ids) > 1:
            raise ValueError("Please select only one day/session for this show.")
        slot_ids = validate_registration_slot_ids(show_id, slot_ids)
        registration_slot_id = slot_ids[0] if slot_ids else None

        if show_has_registration_slots(show_id):
            if not slot_ids:
                raise ValueError("Please select which day/session you are registering for.")
            if not selected_registration_slots_have_capacity(show_id, slot_ids):
                raise ValueError("One of the selected days/sessions has reached its maximum number of cars.")
        elif not show_has_capacity(show_id):
            raise ValueError("This show has reached its maximum number of cars.")

        if reserved_car_number is not None:
            car_number = int(reserved_car_number)

            existing_paid = cur.execute(
                """
                SELECT id
                FROM show_cars
                WHERE show_id = ?
                  AND car_number = ?
                  AND COALESCE(is_placeholder, 0) = 0
                LIMIT 1
                """,
                (show_id, car_number),
            ).fetchone()
            if existing_paid:
                raise ValueError("That car number is already registered for this show.")

            existing_placeholder = cur.execute(
                """
                SELECT id
                FROM show_cars
                WHERE show_id = ?
                  AND car_number = ?
                  AND COALESCE(is_placeholder, 0) = 1
                  AND COALESCE(registration_state, '') = 'placeholder'
                LIMIT 1
                """,
                (show_id, car_number),
            ).fetchone()
            if not existing_placeholder:
                raise ValueError("That placeholder car number is no longer available.")
        else:
            next_placeholder = cur.execute(
                """
                SELECT car_number
                FROM show_cars
                WHERE show_id = ?
                  AND COALESCE(is_placeholder, 0) = 1
                  AND COALESCE(registration_state, '') = 'placeholder'
                ORDER BY car_number ASC
                LIMIT 1
                """,
                (show_id,),
            ).fetchone()
            if next_placeholder:
                car_number = int(next_placeholder["car_number"])
            else:
                car_number = get_next_available_car_number(show_id)

        token = _new_token()
        cur.execute(
            """
            INSERT INTO registration_intents (
                show_id, registration_slot_id, registration_slot_ids, intent_token, owner_name, phone, email, opt_in_future, sponsor_opt_in, charity_opt_in,
                car_number, year, make, model, insurance_carrier,
                waiver_accepted, waiver_signed_name, waiver_text, waiver_version, waiver_text_sha256,
                waiver_template_id, amount_cents, payment_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (
                show_id,
                int(registration_slot_id) if registration_slot_id else None,
                _slot_ids_json(slot_ids),
                token,
                owner_name,
                phone,
                email,
                _b(opt_in_future),
                _b(sponsor_opt_in),
                _b(charity_opt_in),
                car_number,
                year,
                make,
                model,
                (insurance_carrier or "").strip(),
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
        return rid, token, car_number
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

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
        ri = cur.execute(
            "SELECT * FROM registration_intents WHERE stripe_session_id = ? LIMIT 1",
            (stripe_session_id,),
        ).fetchone()
        if not ri:
            raise ValueError("Registration intent not found.")

        if ri["finalized_show_car_id"]:
            sc = cur.execute(
                "SELECT * FROM show_cars WHERE id = ? LIMIT 1",
                (int(ri["finalized_show_car_id"]),),
            ).fetchone()
            cur.execute(
                """
                UPDATE registration_intents
                SET payment_status = 'paid',
                    paid_at = COALESCE(paid_at, datetime('now'))
                WHERE id = ?
                """,
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
        slot_ids = _slot_ids_from_json(ri["registration_slot_ids"] if "registration_slot_ids" in ri.keys() and ri["registration_slot_ids"] else "[]")
        if not slot_ids and "registration_slot_id" in ri.keys() and ri["registration_slot_id"]:
            slot_ids = [int(ri["registration_slot_id"])]
        slot_ids = validate_registration_slot_ids(show_id, slot_ids)
        registration_slot_id = slot_ids[0] if slot_ids else None
        if show_has_registration_slots(show_id):
            if not slot_ids:
                raise ValueError("Please select which day/session you are registering for.")
            if not selected_registration_slots_have_capacity(show_id, slot_ids):
                raise ValueError("One of the selected days/sessions has reached its maximum number of cars.")
        elif not show_has_capacity(show_id):
            raise ValueError("This show has reached its maximum number of cars.")

        existing_final = cur.execute(
            """
            SELECT id
            FROM show_cars
            WHERE show_id = ?
              AND car_number = ?
              AND COALESCE(is_placeholder, 0) = 0
            LIMIT 1
            """,
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
            INSERT INTO people (name, phone, email, opt_in_future, sponsor_opt_in, charity_opt_in, consent_text, consent_version)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ri["owner_name"],
                ri["phone"],
                ri["email"],
                int(ri["opt_in_future"] or 0),
                int(ri["sponsor_opt_in"] or 0),
                int(ri["charity_opt_in"] or 0),
                person_consent_text,
                person_consent_version,
            ),
        )
        person_id = int(cur.lastrowid)

        placeholder = cur.execute(
            """
            SELECT id, car_token
            FROM show_cars
            WHERE show_id = ?
              AND car_number = ?
              AND COALESCE(is_placeholder, 0) = 1
              AND COALESCE(registration_state, '') = 'placeholder'
            LIMIT 1
            """,
            (show_id, int(ri["car_number"])),
        ).fetchone()

        if placeholder:
            show_car_id = int(placeholder["id"])
            car_token = placeholder["car_token"]
            cur.execute(
                """
                UPDATE show_cars
                SET person_id = ?,
                    year = ?,
                    make = ?,
                    model = ?,
                    insurance_carrier = ?,
                    registration_slot_id = ?,
                    registration_payment_status = 'paid',
                    registration_amount_cents = ?,
                    registration_session_id = ?,
                    waiver_signed_name = ?,
                    waiver_signed_at = datetime('now'),
                    waiver_version = ?,
                    waiver_text = ?,
                    waiver_text_sha256 = ?,
                    waiver_template_id = ?,
                    waiver_received = 1,
                    waiver_received_at = datetime('now'),
                    waiver_received_by = 'electronic',
                    is_placeholder = 0,
                    registration_state = 'claimed'
                WHERE id = ?
                """,
                (
                    person_id,
                    ri["year"],
                    ri["make"],
                    ri["model"],
                    ri["insurance_carrier"] if "insurance_carrier" in ri.keys() else "",
                    registration_slot_id,
                    int(ri["amount_cents"] or 0),
                    stripe_session_id,
                    ri["waiver_signed_name"],
                    ri["waiver_version"],
                    ri["waiver_text"],
                    ri["waiver_text_sha256"],
                    ri["waiver_template_id"] if "waiver_template_id" in ri.keys() else None,
                    show_car_id,
                ),
            )
        else:
            car_token = _new_car_token()
            cur.execute(
                """
                INSERT INTO show_cars (
                    show_id, person_id, car_number, car_token, year, make, model, insurance_carrier, registration_slot_id,
                    registration_payment_status, registration_amount_cents, registration_session_id,
                    waiver_signed_name, waiver_signed_at, waiver_version, waiver_text, waiver_text_sha256, waiver_template_id,
                    waiver_received, waiver_received_at, waiver_received_by,
                    is_placeholder, registration_state
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?, 1, datetime('now'), 'electronic', 0, 'claimed')
                """,
                (
                    show_id,
                    person_id,
                    int(ri["car_number"]),
                    car_token,
                    ri["year"],
                    ri["make"],
                    ri["model"],
                    ri["insurance_carrier"] if "insurance_carrier" in ri.keys() else "",
                    registration_slot_id,
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

        for slot_id in slot_ids:
            cur.execute(
                """
                INSERT OR IGNORE INTO show_car_registration_slots (show_id, show_car_id, registration_slot_id)
                VALUES (?, ?, ?)
                """,
                (show_id, show_car_id, int(slot_id)),
            )

        cur.execute(
            """
            UPDATE registration_intents
            SET payment_status = 'paid',
                paid_at = datetime('now'),
                finalized_show_car_id = ?
            WHERE id = ?
            """,
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


# JUDGING CLASSES / AUTO CLASS ASSIGNMENT

def list_judging_classes(show_id: int, active_only: bool = False) -> List[sqlite3.Row]:
    conn = _conn()
    where = "WHERE show_id = ?"
    params: list[Any] = [int(show_id)]
    if active_only:
        where += " AND is_active = 1"
    rows = conn.execute(
        f"""
        SELECT *
        FROM show_judging_classes
        {where}
        ORDER BY sort_order ASC, id ASC
        """,
        params,
    ).fetchall()
    conn.close()
    return rows


def _int_or_none(value: Any) -> Optional[int]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(str(value).strip())
    except Exception:
        return None


def save_judging_classes_for_show(show_id: int, payloads: List[Dict[str, Any]]) -> None:
    """Replace a show's judging/class list from the admin form.

    This is intentionally replace-all to keep the interface simple and predictable.
    Existing car assignments keep their class ids until the next claim/update, but inactive/deleted
    classes will no longer be offered for new assignments.
    """
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM show_judging_classes WHERE show_id = ?", (int(show_id),))
        for idx, item in enumerate(payloads, start=1):
            class_name = (item.get("class_name") or item.get("name") or "").strip()
            if not class_name:
                continue
            sort_order = _int_or_none(item.get("sort_order")) or idx * 10
            award_places = _int_or_none(item.get("award_places")) or 3
            cur.execute(
                """
                INSERT INTO show_judging_classes (
                    show_id, class_code, class_name, description, sort_order, is_active,
                    year_min, year_max, make_contains, model_contains, keyword_contains, award_places, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    int(show_id),
                    (item.get("class_code") or item.get("code") or "").strip(),
                    class_name,
                    (item.get("description") or "").strip(),
                    int(sort_order),
                    1 if str(item.get("is_active", "1")).lower() in {"1", "true", "yes", "on", "active"} else 0,
                    _int_or_none(item.get("year_min")),
                    _int_or_none(item.get("year_max")),
                    (item.get("make_contains") or "").strip(),
                    (item.get("model_contains") or "").strip(),
                    (item.get("keyword_contains") or "").strip(),
                    max(1, int(award_places)),
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _contains_any_term(haystack: str, terms_csv: str) -> bool:
    terms = [t.strip().lower() for t in str(terms_csv or "").replace(";", ",").split(",") if t.strip()]
    if not terms:
        return True
    h = (haystack or "").lower()
    return any(t in h for t in terms)


def find_matching_judging_class(show_id: int, year: str, make: str, model: str) -> Tuple[Optional[int], int]:
    """Return (class_id, needs_review).

    Rules are intentionally conservative:
    - A class matches if every populated rule field matches.
    - If exactly one active class matches, assign it.
    - If none or multiple match, leave class blank and flag for review.
    """
    try:
        y = int(str(year or "").strip())
    except Exception:
        y = None
    make_text = str(make or "").strip().lower()
    model_text = str(model or "").strip().lower()
    vehicle_text = " ".join([str(year or ""), make_text, model_text]).strip().lower()

    matches: list[int] = []
    for cls in list_judging_classes(show_id, active_only=True):
        has_rule = any([
            cls["year_min"] is not None,
            cls["year_max"] is not None,
            (cls["make_contains"] or "").strip(),
            (cls["model_contains"] or "").strip(),
            (cls["keyword_contains"] or "").strip(),
        ])
        if not has_rule:
            continue
        if cls["year_min"] is not None and (y is None or y < int(cls["year_min"])):
            continue
        if cls["year_max"] is not None and (y is None or y > int(cls["year_max"])):
            continue
        if not _contains_any_term(make_text, cls["make_contains"] or ""):
            continue
        if not _contains_any_term(model_text, cls["model_contains"] or ""):
            continue
        if not _contains_any_term(vehicle_text, cls["keyword_contains"] or ""):
            continue
        matches.append(int(cls["id"]))

    if len(matches) == 1:
        return matches[0], 0
    if len(matches) > 1:
        return None, 1
    active_classes = list_judging_classes(show_id, active_only=True)
    return None, 1 if active_classes else 0


def ensure_placeholder_cards_up_to_max(show_id: int) -> int:
    """Create available placeholder cards for all missing numbers up to shows.max_cars.

    This supports the show-day workflow where pre-registered cars get assigned numbers,
    and all remaining printed cards can be shuffled and handed out from multiple lines.
    """
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        show = cur.execute("SELECT max_cars FROM shows WHERE id = ? LIMIT 1", (int(show_id),)).fetchone()
        try:
            max_cars = int(show["max_cars"] or 0) if show else 0
        except Exception:
            max_cars = 0
        if max_cars <= 0:
            raise ValueError("Set Max Cars for this show before creating the open placeholder stack.")

        existing = {
            int(r["car_number"])
            for r in cur.execute("SELECT car_number FROM show_cars WHERE show_id = ?", (int(show_id),)).fetchall()
        }
        created = 0
        for n in range(1, max_cars + 1):
            if n in existing:
                continue
            cur.execute(
                """
                INSERT INTO people (name, phone, email, opt_in_future, sponsor_opt_in, consent_text, consent_version)
                VALUES (?, ?, ?, 0, 0, NULL, NULL)
                """,
                ("", "", ""),
            )
            person_id = int(cur.lastrowid)
            cur.execute(
                """
                INSERT INTO show_cars (
                    show_id, person_id, car_number, car_token, year, make, model,
                    is_placeholder, registration_state, registration_payment_status
                ) VALUES (?, ?, ?, ?, 'TBD', 'TBD', 'TBD', 1, 'placeholder', 'open')
                """,
                (int(show_id), person_id, n, _new_car_token()),
            )
            created += 1
        conn.commit()
        return created
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# PLACEHOLDER CARS

def create_placeholder_cars(show_id: int, start_number: int, count: int) -> int:
    conn = _conn()
    cur = conn.cursor()

    show = cur.execute(
        "SELECT max_cars FROM shows WHERE id = ? LIMIT 1",
        (show_id,),
    ).fetchone()

    max_cars = None
    if show and show["max_cars"] is not None:
        try:
            max_cars = int(show["max_cars"])
        except Exception:
            max_cars = None

    current_count = int(
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM show_cars WHERE show_id = ?",
            (show_id,),
        ).fetchone()["cnt"] or 0
    )

    created = 0
    for n in range(start_number, start_number + count):
        if max_cars and max_cars > 0 and (current_count + created) >= max_cars:
            break

        exists = cur.execute(
            "SELECT 1 FROM show_cars WHERE show_id = ? AND car_number = ? LIMIT 1",
            (show_id, n),
        ).fetchone()
        if exists:
            continue

        cur.execute(
            """
            INSERT INTO people (
                name, phone, email, opt_in_future, sponsor_opt_in, consent_text, consent_version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("", "", "", 0, 0, None, None),
        )
        person_id = int(cur.lastrowid)

        token = _new_car_token()
        cur.execute(
            """
            INSERT INTO show_cars (
                show_id, person_id, car_number, car_token, year, make, model,
                is_placeholder, registration_state, registration_payment_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, 'placeholder', 'open')
            """,
            (show_id, person_id, n, token, "TBD", "TBD", "TBD"),
        )
        created += 1

    conn.commit()
    conn.close()
    return created


# PARTICIPANT / RESTRICTED VOTING

def _restricted_voter_token() -> str:
    return secrets.token_urlsafe(18)


def get_or_create_participant_voter(show_id: int, show_car_id: int) -> sqlite3.Row:
    """Return the participant voter row tied to a registered show car."""
    conn = _conn()
    cur = conn.cursor()
    try:
        row = cur.execute(
            """
            SELECT * FROM show_voters
            WHERE show_id = ? AND show_car_id = ? AND voter_type = 'participant'
            LIMIT 1
            """,
            (int(show_id), int(show_car_id)),
        ).fetchone()
        if row:
            return row

        car = cur.execute(
            """
            SELECT sc.*, p.name AS owner_name, p.email AS owner_email, p.phone AS owner_phone
            FROM show_cars sc
            JOIN people p ON p.id = sc.person_id
            WHERE sc.show_id = ? AND sc.id = ?
            LIMIT 1
            """,
            (int(show_id), int(show_car_id)),
        ).fetchone()
        if not car:
            raise ValueError("Show car not found.")

        token = _restricted_voter_token()
        cur.execute(
            """
            INSERT INTO show_voters
                (show_id, show_car_id, voter_token, voter_type, display_name, email, phone, is_active, updated_at)
            VALUES (?, ?, ?, 'participant', ?, ?, ?, 1, datetime('now'))
            """,
            (
                int(show_id),
                int(show_car_id),
                token,
                car["owner_name"] or f"Car #{car['car_number']}",
                car["owner_email"] or "",
                car["owner_phone"] or "",
            ),
        )
        conn.commit()
        row = cur.execute("SELECT * FROM show_voters WHERE id = ?", (int(cur.lastrowid),)).fetchone()
        return row
    finally:
        conn.close()


def create_judge_voter(show_id: int, display_name: str = "Judge", email: str = "", phone: str = "") -> sqlite3.Row:
    conn = _conn()
    cur = conn.cursor()
    token = _restricted_voter_token()
    cur.execute(
        """
        INSERT INTO show_voters
            (show_id, show_car_id, voter_token, voter_type, display_name, email, phone, is_active, updated_at)
        VALUES (?, NULL, ?, 'judge', ?, ?, ?, 1, datetime('now'))
        """,
        (int(show_id), token, (display_name or "Judge").strip(), (email or "").strip(), (phone or "").strip()),
    )
    conn.commit()
    row = cur.execute("SELECT * FROM show_voters WHERE id = ?", (int(cur.lastrowid),)).fetchone()
    conn.close()
    return row


def get_show_voter_by_token(show_id: int, voter_token: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        """
        SELECT * FROM show_voters
        WHERE show_id = ? AND voter_token = ? AND is_active = 1
        LIMIT 1
        """,
        (int(show_id), (voter_token or "").strip()),
    ).fetchone()
    conn.close()
    return row


def activate_show_voter(show_id: int, voter_token: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT * FROM show_voters
        WHERE show_id = ? AND voter_token = ? AND is_active = 1
        LIMIT 1
        """,
        (int(show_id), (voter_token or "").strip()),
    ).fetchone()
    if row:
        cur.execute(
            """
            UPDATE show_voters
            SET activated_at = COALESCE(activated_at, datetime('now')), updated_at = datetime('now')
            WHERE id = ?
            """,
            (int(row["id"]),),
        )
        conn.commit()
        row = cur.execute("SELECT * FROM show_voters WHERE id = ?", (int(row["id"]),)).fetchone()
    conn.close()
    return row


def list_show_voters(show_id: int, voter_type: str = "") -> List[sqlite3.Row]:
    conn = _conn()
    params: list[Any] = [int(show_id)]
    where = "WHERE v.show_id = ?"
    if voter_type:
        where += " AND v.voter_type = ?"
        params.append(voter_type)
    rows = conn.execute(
        f"""
        SELECT v.*, sc.car_number
        FROM show_voters v
        LEFT JOIN show_cars sc ON sc.id = v.show_car_id
        {where}
        ORDER BY v.voter_type ASC, v.id DESC
        """,
        params,
    ).fetchall()
    conn.close()
    return rows


def get_restricted_vote(show_id: int, voter_id: int, category_key: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        """
        SELECT rv.*, sc.car_number, sc.year, sc.make, sc.model, p.name AS owner_name
        FROM restricted_votes rv
        JOIN show_cars sc ON sc.id = rv.selected_show_car_id
        JOIN people p ON p.id = sc.person_id
        WHERE rv.show_id = ? AND rv.voter_id = ? AND rv.category_key = ?
        LIMIT 1
        """,
        (int(show_id), int(voter_id), (category_key or "").strip()),
    ).fetchone()
    conn.close()
    return row


def upsert_restricted_vote(
    show_id: int,
    voter_id: int,
    category_key: str,
    selected_show_car_id: int,
    judging_class_id: Optional[int] = None,
    vote_weight: int = 1,
) -> sqlite3.Row:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO restricted_votes
            (show_id, voter_id, category_key, judging_class_id, selected_show_car_id, vote_weight, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(show_id, voter_id, category_key)
        DO UPDATE SET
            judging_class_id = excluded.judging_class_id,
            selected_show_car_id = excluded.selected_show_car_id,
            vote_weight = excluded.vote_weight,
            updated_at = datetime('now')
        """,
        (
            int(show_id),
            int(voter_id),
            (category_key or "").strip(),
            int(judging_class_id) if judging_class_id else None,
            int(selected_show_car_id),
            int(vote_weight or 1),
        ),
    )
    conn.commit()
    row = cur.execute(
        """
        SELECT * FROM restricted_votes
        WHERE show_id = ? AND voter_id = ? AND category_key = ?
        LIMIT 1
        """,
        (int(show_id), int(voter_id), (category_key or "").strip()),
    ).fetchone()
    conn.close()
    return row


def restricted_vote_progress(show_id: int, voter_id: int, category_keys: List[str]) -> Dict[str, Any]:
    keys = [str(k) for k in (category_keys or []) if str(k).strip()]
    conn = _conn()
    rows = conn.execute(
        """
        SELECT category_key FROM restricted_votes
        WHERE show_id = ? AND voter_id = ?
        """,
        (int(show_id), int(voter_id)),
    ).fetchall()
    conn.close()
    completed = {r["category_key"] for r in rows}
    remaining = [k for k in keys if k not in completed]
    return {
        "completed_keys": completed,
        "remaining_keys": remaining,
        "completed_count": len([k for k in keys if k in completed]),
        "total_count": len(keys),
        "is_complete": bool(keys) and not remaining,
    }


def restricted_leaderboard_by_category(show_id: int) -> Dict[str, List[Tuple[int, int]]]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT rv.category_key, sc.car_number, SUM(rv.vote_weight) AS total_votes
        FROM restricted_votes rv
        JOIN show_cars sc ON sc.id = rv.selected_show_car_id
        WHERE rv.show_id = ?
        GROUP BY rv.category_key, sc.car_number
        ORDER BY rv.category_key ASC, total_votes DESC, sc.car_number ASC
        """,
        (int(show_id),),
    ).fetchall()
    conn.close()
    out: Dict[str, List[Tuple[int, int]]] = {}
    for r in rows:
        out.setdefault(r["category_key"], [])
        out[r["category_key"]].append((int(r["car_number"]), int(r["total_votes"] or 0)))
    return out

# VOTING

def create_vote_intent(show_id: int, show_car_id: int, category: str, vote_qty: int, amount_cents: int, entry_method: str = "car_qr") -> int:
    conn = _conn()
    cur = conn.cursor()
    entry_method_clean = "car_number" if (entry_method or "").strip().lower() == "car_number" else "car_qr"
    cur.execute(
        "INSERT INTO vote_intents (show_id, show_car_id, category, vote_qty, amount_cents, payment_status, entry_method) VALUES (?, ?, ?, ?, ?, 'pending', ?)",
        (show_id, show_car_id, category, vote_qty, amount_cents, entry_method_clean),
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
                "INSERT INTO votes (show_id, show_car_id, category, vote_qty, amount_cents, stripe_session_id, entry_method) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (int(vi["show_id"]), int(vi["show_car_id"]), vi["category"], int(vi["vote_qty"]), int(vi["amount_cents"]), stripe_session_id, vi["entry_method"] if "entry_method" in vi.keys() else "car_qr"),
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
        
def finalize_external_vote_intent(vote_intent_id: int, approval_reference: str = "") -> Dict[str, Any]:
    conn = _conn()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")

        vi = cur.execute(
            "SELECT * FROM vote_intents WHERE id = ? LIMIT 1",
            (vote_intent_id,),
        ).fetchone()
        if not vi:
            raise ValueError("Vote intent not found.")

        synthetic_session_id = f"external_vote_{int(vi['id'])}"

        existing_vote = cur.execute(
            "SELECT id FROM votes WHERE stripe_session_id = ? LIMIT 1",
            (synthetic_session_id,),
        ).fetchone()

        if not existing_vote:
            cur.execute(
                """
                INSERT INTO votes (
                    show_id, show_car_id, category, vote_qty, amount_cents, stripe_session_id, entry_method
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(vi["show_id"]),
                    int(vi["show_car_id"]),
                    vi["category"],
                    int(vi["vote_qty"]),
                    int(vi["amount_cents"]),
                    synthetic_session_id,
                    vi["entry_method"] if "entry_method" in vi.keys() else "car_qr",
                ),
            )

        cur.execute(
            """
            UPDATE vote_intents
            SET payment_status = 'paid',
                paid_at = COALESCE(paid_at, datetime('now')),
                stripe_payment_intent_id = COALESCE(NULLIF(?, ''), stripe_payment_intent_id)
            WHERE id = ?
            """,
            (approval_reference, int(vi["id"])),
        )

        conn.commit()
        return {
            "vote_intent_id": int(vi["id"]),
            "already_finalized": bool(existing_vote),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def list_pending_vote_reviews(show_id: Optional[int] = None) -> List[sqlite3.Row]:
    conn = _conn()
    try:
        if show_id is None:
            rows = conn.execute(
                """
                SELECT
                    vi.*,
                    s.slug AS show_slug,
                    s.title AS show_title,
                    sc.car_number,
                    sc.year,
                    sc.make,
                    sc.model,
                    p.name AS owner_name
                FROM vote_intents vi
                JOIN shows s ON s.id = vi.show_id
                JOIN show_cars sc ON sc.id = vi.show_car_id
                JOIN people p ON p.id = sc.person_id
                WHERE vi.payment_status = 'pending_review'
                ORDER BY vi.created_at ASC, vi.id ASC
                """
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    vi.*,
                    s.slug AS show_slug,
                    s.title AS show_title,
                    sc.car_number,
                    sc.year,
                    sc.make,
                    sc.model,
                    p.name AS owner_name
                FROM vote_intents vi
                JOIN shows s ON s.id = vi.show_id
                JOIN show_cars sc ON sc.id = vi.show_car_id
                JOIN people p ON p.id = sc.person_id
                WHERE vi.payment_status = 'pending_review'
                  AND vi.show_id = ?
                ORDER BY vi.created_at ASC, vi.id ASC
                """,
                (int(show_id),),
            ).fetchall()
        return rows
    finally:
        conn.close()


def reject_external_vote_intent(vote_intent_id: int, rejection_reference: str = "") -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            UPDATE vote_intents
            SET payment_status = 'rejected',
                stripe_payment_intent_id = COALESCE(NULLIF(?, ''), stripe_payment_intent_id)
            WHERE id = ?
            """,
            (rejection_reference, int(vote_intent_id)),
        )
        conn.commit()
    finally:
        conn.close()
        
def reset_votes_for_show(show_id: int) -> None:
    conn = _conn()
    conn.execute("DELETE FROM votes WHERE show_id = ?", (show_id,))
    conn.execute("DELETE FROM vote_intents WHERE show_id = ?", (show_id,))
    conn.commit()
    conn.close()        
        
def _vote_date_where_clause(start_date: str = "", end_date: str = "") -> Tuple[str, List[str]]:
    """Builds an optional inclusive date filter for vote queries."""
    where_parts: List[str] = []
    params: List[str] = []

    start_date = (start_date or "").strip()
    end_date = (end_date or "").strip()

    if start_date:
        where_parts.append("date(v.created_at) >= date(?)")
        params.append(start_date)
    if end_date:
        where_parts.append("date(v.created_at) <= date(?)")
        params.append(end_date)

    if not where_parts:
        return "", []
    return " AND " + " AND ".join(where_parts), params


def export_votes_for_show(show_id: int, start_date: str = "", end_date: str = "") -> List[sqlite3.Row]:
    conn = _conn()
    date_where, date_params = _vote_date_where_clause(start_date, end_date)
    rows = conn.execute(
        f"""
        SELECT
            v.created_at,
            v.category,
            v.vote_qty,
            v.amount_cents,
            v.stripe_session_id,
            v.entry_method,
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
        WHERE v.show_id = ?{date_where}
        ORDER BY v.created_at ASC
        """,
        [show_id] + date_params,
    ).fetchall()
    conn.close()
    return rows


def leaderboard_by_category(show_id: int, start_date: str = "", end_date: str = "") -> Dict[str, List[Tuple[int, int]]]:
    conn = _conn()
    date_where, date_params = _vote_date_where_clause(start_date, end_date)
    rows = conn.execute(
        f"""
        SELECT v.category, sc.car_number, SUM(v.vote_qty) as total_votes
        FROM votes v
        JOIN show_cars sc ON sc.id = v.show_car_id
        WHERE v.show_id = ?{date_where}
        GROUP BY v.category, sc.car_number
        ORDER BY v.category ASC, total_votes DESC, sc.car_number ASC
        """,
        [show_id] + date_params,
    ).fetchall()
    conn.close()
    out: Dict[str, List[Tuple[int, int]]] = {}
    for r in rows:
        out.setdefault(r["category"], [])
        out[r["category"]].append((int(r["car_number"]), int(r["total_votes"] or 0)))
    return out


def leaderboard_overall(show_id: int, start_date: str = "", end_date: str = "") -> List[Tuple[int, int]]:
    conn = _conn()
    date_where, date_params = _vote_date_where_clause(start_date, end_date)
    rows = conn.execute(
        f"""
        SELECT sc.car_number, SUM(v.vote_qty) as total_votes
        FROM votes v
        JOIN show_cars sc ON sc.id = v.show_car_id
        WHERE v.show_id = ?{date_where}
        GROUP BY sc.car_number
        ORDER BY total_votes DESC, sc.car_number ASC
        """,
        [show_id] + date_params,
    ).fetchall()
    conn.close()
    return [(int(r["car_number"]), int(r["total_votes"] or 0)) for r in rows]



# PAPER BALLOTS / MANUAL VOTE ENTRY


def _paper_vote_category(class_row: sqlite3.Row, placement: int) -> str:
    code = (class_row["class_code"] or "").strip() if "class_code" in class_row.keys() else ""
    name = (class_row["class_name"] or "").strip() if "class_name" in class_row.keys() else ""
    base = code or name or f"Class {class_row['id']}"
    suffix = {1: "1st", 2: "2nd", 3: "3rd"}.get(int(placement), f"Place {placement}")
    return f"{base} - {suffix}"


def list_paper_ballot_classes(show_id: int) -> List[sqlite3.Row]:
    """Active judging/voting classes used to build paper ballots."""
    return list_judging_classes(int(show_id), active_only=True)


def _find_show_car_for_class(conn: sqlite3.Connection, show_id: int, judging_class_id: int, car_number: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT sc.*, p.name AS owner_name
        FROM show_cars sc
        LEFT JOIN people p ON p.id = sc.person_id
        WHERE sc.show_id = ?
          AND sc.car_number = ?
          AND COALESCE(sc.registration_state, '') != 'removed'
          AND COALESCE(sc.registration_payment_status, '') NOT IN ('removed', 'canceled', 'refunded')
          AND COALESCE(sc.is_placeholder, 0) = 0
          AND COALESCE(sc.judging_class_id, 0) = ?
        LIMIT 1
        """,
        (int(show_id), int(car_number), int(judging_class_id)),
    ).fetchone()


def create_paper_ballot_with_votes(
    show_id: int,
    selections: Dict[int, Dict[int, int]],
    *,
    ballot_label: str = "",
    source: str = "manual",
    entered_by: str = "",
    notes: str = "",
) -> Dict[str, Any]:
    """Create one paper ballot and count its 1st/2nd/3rd choices.

    selections shape:
        {judging_class_id: {1: car_number, 2: car_number, 3: car_number}}

    Business rules:
    - One paper ballot can cast only one 1st, one 2nd, and one 3rd choice per class.
    - The selected car number must exist in that same judging class for the show.
    - Paper votes are also inserted into the existing votes table so current leaderboards/export still work.
    """
    conn = _conn()
    cur = conn.cursor()
    errors: List[str] = []
    accepted: List[Dict[str, Any]] = []

    classes = {int(c["id"]): c for c in list_judging_classes(int(show_id), active_only=True)}
    cleaned: Dict[int, Dict[int, int]] = {}

    for class_id, places in (selections or {}).items():
        try:
            class_id = int(class_id)
        except Exception:
            continue
        if class_id not in classes:
            errors.append(f"Class ID {class_id} is not active for this show.")
            continue
        seen_numbers = set()
        cleaned[class_id] = {}
        for placement in (1, 2, 3):
            raw_number = (places or {}).get(placement)
            if raw_number in (None, ""):
                continue
            try:
                car_number = int(raw_number)
            except Exception:
                errors.append(f"{classes[class_id]['class_name']} {placement}: car number must be numeric.")
                continue
            if car_number in seen_numbers:
                errors.append(f"{classes[class_id]['class_name']}: car #{car_number} was entered more than once on the same ballot.")
                continue
            seen_numbers.add(car_number)
            car = _find_show_car_for_class(conn, int(show_id), class_id, car_number)
            if not car:
                errors.append(f"{classes[class_id]['class_name']} {placement}: car #{car_number} is not registered in this class.")
                continue
            cleaned[class_id][placement] = car_number
            accepted.append({
                "class_id": class_id,
                "class_name": classes[class_id]["class_name"],
                "placement": placement,
                "car_number": car_number,
                "show_car_id": int(car["id"]),
            })

    if errors:
        conn.close()
        return {"ok": False, "errors": errors, "accepted_count": 0}

    if not accepted:
        conn.close()
        return {"ok": False, "errors": ["No valid votes were entered."], "accepted_count": 0}

    token = secrets.token_urlsafe(12)
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT INTO paper_ballots (show_id, ballot_token, ballot_label, source, entered_by, status, notes)
            VALUES (?, ?, ?, ?, ?, 'accepted', ?)
            """,
            (int(show_id), token, ballot_label or None, source or "manual", entered_by or None, notes or None),
        )
        ballot_id = int(cur.lastrowid)

        for item in accepted:
            class_row = classes[int(item["class_id"])]
            category = _paper_vote_category(class_row, int(item["placement"]))
            synthetic_session_id = f"paper_{ballot_id}_{item['class_id']}_{item['placement']}"
            cur.execute(
                """
                INSERT INTO paper_ballot_votes (
                    paper_ballot_id, show_id, judging_class_id, placement,
                    selected_show_car_id, selected_car_number, category
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (ballot_id, int(show_id), int(item["class_id"]), int(item["placement"]), int(item["show_car_id"]), int(item["car_number"]), category),
            )
            cur.execute(
                """
                INSERT OR IGNORE INTO votes (show_id, show_car_id, category, vote_qty, amount_cents, stripe_session_id)
                VALUES (?, ?, ?, 1, 0, ?)
                """,
                (int(show_id), int(item["show_car_id"]), category, synthetic_session_id),
            )

        conn.commit()
        return {"ok": True, "ballot_id": ballot_id, "ballot_token": token, "accepted_count": len(accepted), "errors": []}
    except Exception as exc:
        conn.rollback()
        return {"ok": False, "errors": [str(exc)], "accepted_count": 0}
    finally:
        conn.close()


def list_recent_paper_ballots(show_id: int, limit: int = 25) -> List[sqlite3.Row]:
    conn = _conn()
    try:
        return conn.execute(
            """
            SELECT pb.*,
                   COUNT(pbv.id) AS vote_lines
            FROM paper_ballots pb
            LEFT JOIN paper_ballot_votes pbv ON pbv.paper_ballot_id = pb.id
            WHERE pb.show_id = ?
            GROUP BY pb.id
            ORDER BY pb.created_at DESC, pb.id DESC
            LIMIT ?
            """,
            (int(show_id), int(limit)),
        ).fetchall()
    finally:
        conn.close()


def build_paper_ballot_csv_template(show_id: int) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ballot_label", "class_code", "class_name", "first_car_number", "second_car_number", "third_car_number"])
    for c in list_paper_ballot_classes(int(show_id)):
        writer.writerow(["", c["class_code"] or "", c["class_name"] or "", "", "", ""])
    return output.getvalue()


def import_paper_ballot_csv(show_id: int, csv_text: str, *, entered_by: str = "") -> Dict[str, Any]:
    reader = csv.DictReader(io.StringIO(csv_text or ""))
    if not reader.fieldnames:
        return {"ok": False, "created_ballots": 0, "vote_lines": 0, "errors": ["CSV file is missing a header row."]}

    classes = list_paper_ballot_classes(int(show_id))
    by_code = {str(c["class_code"] or "").strip().lower(): c for c in classes if str(c["class_code"] or "").strip()}
    by_name = {str(c["class_name"] or "").strip().lower(): c for c in classes if str(c["class_name"] or "").strip()}

    grouped: Dict[str, Dict[int, Dict[int, int]]] = {}
    errors: List[str] = []

    for idx, row in enumerate(reader, start=2):
        row = {str(k or "").strip().lower(): (v or "").strip() for k, v in row.items()}
        label = row.get("ballot_label") or row.get("ballot") or f"import-row-{idx}"
        class_key = (row.get("class_code") or "").strip().lower()
        class_name = (row.get("class_name") or row.get("class") or "").strip().lower()
        class_row = by_code.get(class_key) if class_key else None
        if not class_row and class_name:
            class_row = by_name.get(class_name)
        if not class_row:
            errors.append(f"Row {idx}: class not found ({row.get('class_code') or row.get('class_name') or row.get('class')}).")
            continue
        class_id = int(class_row["id"])
        grouped.setdefault(label, {})[class_id] = {}
        for placement, col in [(1, "first_car_number"), (2, "second_car_number"), (3, "third_car_number")]:
            raw = row.get(col) or row.get({1:"first",2:"second",3:"third"}[placement]) or ""
            if raw:
                grouped[label][class_id][placement] = raw

    if errors:
        return {"ok": False, "created_ballots": 0, "vote_lines": 0, "errors": errors}

    created = 0
    vote_lines = 0
    for label, selections in grouped.items():
        result = create_paper_ballot_with_votes(
            int(show_id),
            selections,
            ballot_label=label,
            source="csv_import",
            entered_by=entered_by,
        )
        if not result.get("ok"):
            errors.extend([f"{label}: {e}" for e in result.get("errors", [])])
        else:
            created += 1
            vote_lines += int(result.get("accepted_count") or 0)

    return {"ok": not errors, "created_ballots": created, "vote_lines": vote_lines, "errors": errors}


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

def create_attendee(show_id: int, first_name: str, last_name: str, phone: str, email: str, zip_code: str, sponsor_opt_in: bool, updates_opt_in: bool, charity_opt_in: bool, consent_text: str, consent_version: str) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO attendees
        (show_id, first_name, last_name, phone, email, zip, sponsor_opt_in, updates_opt_in, charity_opt_in, consent_text, consent_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (show_id, first_name, last_name, phone or None, email or None, zip_code or None, _b(sponsor_opt_in), _b(updates_opt_in), _b(charity_opt_in), consent_text, consent_version),
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
    charity_opt_in: bool,
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
            opt_in_future, sponsor_opt_in, charity_opt_in,
            waiver_version, waiver_text_sha256, signed_name, waiver_accepted,
            intent_token, html_path, request_path, ip_address, user_agent,
            created_at_utc, created_at_local
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            _b(charity_opt_in),
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
    now_epoch = int(datetime.now(timezone.utc).timestamp())
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


def list_marketing_contacts(show_id: Optional[int] = None) -> List[sqlite3.Row]:
    """Unified, consent-only contact center for participants and attendees."""
    conn = _conn()
    participant_filter = "AND sc.show_id = ?" if show_id is not None else ""
    attendee_filter = "AND a.show_id = ?" if show_id is not None else ""
    interest_filter = "AND eis.show_id = ?" if show_id is not None else ""
    params: list[Any] = []
    if show_id is not None:
        params = [int(show_id), int(show_id), int(show_id)]
    rows = conn.execute(
        f"""
        SELECT * FROM (
            SELECT p.created_at, sc.show_id, s.title AS show_title, s.slug AS show_slug,
                   p.name AS full_name, p.email, p.phone,
                   p.opt_in_future AS event_opt_in, p.sponsor_opt_in, p.charity_opt_in,
                   p.consent_version, 'participant' AS source_type,
                   'vehicle registration' AS source
            FROM people p
            JOIN show_cars sc ON sc.person_id = p.id
            JOIN shows s ON s.id = sc.show_id
            WHERE (p.opt_in_future = 1 OR p.sponsor_opt_in = 1 OR p.charity_opt_in = 1)
              {participant_filter}

            UNION ALL

            SELECT a.created_at, a.show_id, s.title AS show_title, s.slug AS show_slug,
                   trim(a.first_name || ' ' || a.last_name) AS full_name, a.email, a.phone,
                   a.updates_opt_in AS event_opt_in, a.sponsor_opt_in, a.charity_opt_in,
                   a.consent_version, 'attendee' AS source_type,
                   'attendee check-in' AS source
            FROM attendees a
            JOIN shows s ON s.id = a.show_id
            WHERE (a.updates_opt_in = 1 OR a.sponsor_opt_in = 1 OR a.charity_opt_in = 1)
              {attendee_filter}

            UNION ALL

            SELECT eis.created_at, eis.show_id, s.title AS show_title, s.slug AS show_slug,
                   trim(eis.first_name || ' ' || COALESCE(eis.last_name, '')) AS full_name,
                   eis.email, eis.phone,
                   CASE WHEN eis.wants_email = 1 OR eis.wants_text = 1 THEN 1 ELSE 0 END AS event_opt_in,
                   0 AS sponsor_opt_in, 0 AS charity_opt_in, NULL AS consent_version,
                   'interest' AS source_type, COALESCE(eis.source, 'event updates') AS source
            FROM event_interest_signups eis
            LEFT JOIN shows s ON s.id = eis.show_id
            WHERE (eis.wants_email = 1 OR eis.wants_text = 1)
              {interest_filter}
        )
        ORDER BY created_at DESC
        """,
        params,
    ).fetchall()
    conn.close()
    return rows


def export_marketing_contacts_csv(show_id: Optional[int] = None) -> bytes:
    rows = list_marketing_contacts(show_id)
    buf = io.StringIO()
    writer = csv.writer(buf)
    headers = [
        "created_at", "show_id", "show_title", "show_slug", "full_name",
        "email", "phone", "event_opt_in", "sponsor_opt_in", "charity_opt_in",
        "consent_version", "source_type", "source",
    ]
    writer.writerow(headers)
    for row in rows:
        writer.writerow([row[h] for h in headers])
    return buf.getvalue().encode("utf-8-sig")


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
            p.consent_text,
            slot.slot_label,
            slot.slot_date,
            slot.start_time AS slot_start_time,
            slot.end_time AS slot_end_time
        FROM show_cars sc
        JOIN people p ON p.id = sc.person_id
        LEFT JOIN show_registration_slots slot ON slot.id = sc.registration_slot_id
        LEFT JOIN show_judging_classes jc ON jc.id = sc.judging_class_id
        WHERE sc.show_id = ?
        ORDER BY sc.car_number ASC
        """,
        (show_id,),
    ).fetchall()
    conn.close()
    return rows


def export_registration_intents_for_show(show_id: int) -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT ri.*, slot.slot_label, slot.slot_date, slot.start_time AS slot_start_time, slot.end_time AS slot_end_time
        FROM registration_intents ri
        LEFT JOIN show_registration_slots slot ON slot.id = ri.registration_slot_id
        WHERE ri.show_id = ?
        ORDER BY ri.created_at ASC
        """,
        (show_id,),
    ).fetchall()
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



def export_table_rows_for_show(table_name: str, show_id: int) -> List[sqlite3.Row]:
    """Generic safe export helper for optional show-scoped tables."""
    conn = _conn()
    try:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        if not exists:
            return []
        cols = [r[1] for r in conn.execute(f"PRAGMA table_info({table_name})").fetchall()]
        if "show_id" not in cols:
            return []
        rows = conn.execute(f"SELECT * FROM {table_name} WHERE show_id = ? ORDER BY id ASC", (show_id,)).fetchall()
        return rows
    finally:
        conn.close()

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
        zf.writestr("registration_slots.csv", _rows_to_csv_bytes(list_registration_slots(show_id, public_only=False)))
        zf.writestr("registration_intents.csv", _rows_to_csv_bytes(export_registration_intents_for_show(show_id)))
        zf.writestr("votes.csv", _rows_to_csv_bytes(export_votes_for_show(show_id)))
        zf.writestr("vote_intents.csv", _rows_to_csv_bytes(export_vote_intents_for_show(show_id)))
        zf.writestr("donations.csv", _rows_to_csv_bytes(export_donations_for_show(show_id)))
        zf.writestr("waiver_evidence.csv", _rows_to_csv_bytes(export_waiver_evidence_for_show(show_id)))
        zf.writestr("audit_logs.csv", _rows_to_csv_bytes(export_audit_logs_for_show(show_id)))
        zf.writestr("attendees.csv", _rows_to_csv_bytes(export_table_rows_for_show("attendees", show_id)))
        zf.writestr("field_metrics.csv", _rows_to_csv_bytes(export_table_rows_for_show("field_metrics", show_id)))
        zf.writestr("sponsorship_catalog.csv", _rows_to_csv_bytes(export_table_rows_for_show("sponsorship_catalog", show_id)))
        zf.writestr("sponsorship_sales.csv", _rows_to_csv_bytes(export_table_rows_for_show("sponsorship_sales", show_id)))
        zf.writestr("show_sponsors.csv", _rows_to_csv_bytes(export_table_rows_for_show("show_sponsors", show_id)))

    mem.seek(0)
    return mem.getvalue(), filename
# ADMIN IMPORT / ARCHIVE HELPERS

def _csv_key_map(row: Dict[str, Any]) -> Dict[str, str]:
    def norm(k: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(k or "").strip().lower())
    return {norm(k): k for k in row.keys()}


def _csv_value(row: Dict[str, Any], *names: str) -> str:
    key_map = _csv_key_map(row)
    for name in names:
        n = re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())
        if n in key_map:
            return str(row.get(key_map[n]) or "").strip()
    return ""


def _csv_int(value: str, default: Optional[int] = None) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return default
    try:
        return int(float(raw))
    except Exception:
        return default


def _csv_bool(value: str, default: int = 1) -> int:
    raw = str(value or "").strip().lower()
    if not raw:
        return int(default)
    return 1 if raw in {"1", "true", "yes", "y", "active", "on", "checked"} else 0


def archive_show(show_id: int) -> None:
    """Archive a show without deleting historical data."""
    conn = _conn()
    conn.execute(
        """
        UPDATE shows
        SET status = 'archived', is_active = 0, voting_open = 0
        WHERE id = ?
        """,
        (int(show_id),),
    )
    conn.commit()
    conn.close()


def import_judging_classes_for_show(show_id: int, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """Append/update judging classes from CSV rows.

    Strict preferred columns: class_code, class_name, description, year_min, year_max,
    make_contains, model_contains, keyword_contains, award_places, sort_order, is_active.
    Common aliases such as code, class, class name, judging class, make, and model are accepted.
    Existing rows with the same class_code or class_name are updated.
    """
    conn = _conn()
    cur = conn.cursor()
    created = 0
    updated = 0
    skipped = 0
    try:
        cur.execute("BEGIN IMMEDIATE")
        for i, row in enumerate(rows, start=1):
            class_code = _csv_value(row, "class_code", "code", "class", "class number")
            class_name = _csv_value(row, "class_name", "name", "class name", "judging class")
            if not class_name and class_code:
                class_name = class_code
            if not class_name:
                skipped += 1
                continue
            description = _csv_value(row, "description", "desc")
            year_min = _csv_int(_csv_value(row, "year_min", "year min", "min year"))
            year_max = _csv_int(_csv_value(row, "year_max", "year max", "max year"))
            make_contains = _csv_value(row, "make_contains", "make contains", "make")
            model_contains = _csv_value(row, "model_contains", "model contains", "model")
            keyword_contains = _csv_value(row, "keyword_contains", "keyword", "keywords")
            award_places = _csv_int(_csv_value(row, "award_places", "awards", "places"), 3) or 3
            sort_order = _csv_int(_csv_value(row, "sort_order", "sort", "order"), i * 10) or (i * 10)
            is_active = _csv_bool(_csv_value(row, "is_active", "active"), 1)

            existing = None
            if class_code:
                existing = cur.execute(
                    "SELECT id FROM show_judging_classes WHERE show_id = ? AND lower(class_code) = lower(?) LIMIT 1",
                    (int(show_id), class_code),
                ).fetchone()
            if not existing:
                existing = cur.execute(
                    "SELECT id FROM show_judging_classes WHERE show_id = ? AND lower(class_name) = lower(?) LIMIT 1",
                    (int(show_id), class_name),
                ).fetchone()

            if existing:
                cur.execute(
                    """
                    UPDATE show_judging_classes
                    SET class_code = ?, class_name = ?, description = ?, sort_order = ?, is_active = ?,
                        year_min = ?, year_max = ?, make_contains = ?, model_contains = ?, keyword_contains = ?,
                        award_places = ?, updated_at = datetime('now')
                    WHERE id = ?
                    """,
                    (class_code, class_name, description, int(sort_order), int(is_active), year_min, year_max,
                     make_contains, model_contains, keyword_contains, int(award_places), int(existing["id"])),
                )
                updated += 1
            else:
                cur.execute(
                    """
                    INSERT INTO show_judging_classes (
                        show_id, class_code, class_name, description, sort_order, is_active,
                        year_min, year_max, make_contains, model_contains, keyword_contains, award_places
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (int(show_id), class_code, class_name, description, int(sort_order), int(is_active),
                     year_min, year_max, make_contains, model_contains, keyword_contains, int(award_places)),
                )
                created += 1
        conn.commit()
        return {"created": created, "updated": updated, "skipped": skipped}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _find_class_id_by_code_or_name(cur: sqlite3.Cursor, show_id: int, value: str) -> Optional[int]:
    raw = str(value or "").strip()
    if not raw:
        return None
    row = cur.execute(
        """
        SELECT id FROM show_judging_classes
        WHERE show_id = ? AND (lower(class_code) = lower(?) OR lower(class_name) = lower(?))
        LIMIT 1
        """,
        (int(show_id), raw, raw),
    ).fetchone()
    return int(row["id"]) if row else None


def _ensure_import_class_for_show(
    cur: sqlite3.Cursor,
    show_id: int,
    *,
    class_code: str = "",
    class_name: str = "",
    sort_order: Optional[int] = None,
) -> Optional[int]:
    """Find or create a judging class during registration import.

    Strict imports are still preferred, but accepted-registration spreadsheets may be
    the first time a class appears. If a class_code or class_name is supplied and
    does not exist for the show, create it so the imported car can be assigned.
    """
    code = str(class_code or "").strip()
    name = str(class_name or "").strip()
    if not code and not name:
        return None
    if not name:
        name = code

    existing = None
    if code:
        existing = cur.execute(
            "SELECT id FROM show_judging_classes WHERE show_id = ? AND lower(class_code) = lower(?) LIMIT 1",
            (int(show_id), code),
        ).fetchone()
    if not existing and name:
        existing = cur.execute(
            "SELECT id FROM show_judging_classes WHERE show_id = ? AND lower(class_name) = lower(?) LIMIT 1",
            (int(show_id), name),
        ).fetchone()
    if existing:
        return int(existing["id"])

    cur.execute(
        """
        INSERT INTO show_judging_classes (
            show_id, class_code, class_name, description, sort_order, is_active, award_places
        ) VALUES (?, ?, ?, 'Created from accepted registration import', ?, 1, 3)
        """,
        (int(show_id), code, name, int(sort_order or 1000)),
    )
    return int(cur.lastrowid)


def import_registered_cars_for_show(show_id: int, rows: List[Dict[str, Any]], *, assume_paid: bool = True) -> Dict[str, int]:
    """Import already accepted outside registrations.

    Strict preferred columns include: car_number, owner_name, phone, email, year, make, model,
    class_code, class_name, payment_status, waiver_received. Common aliases such as name,
    full name, vehicle year, vehicle make, vehicle model, class, division, and judging class are accepted.
    Missing classes from class_code/class_name are created automatically for the show.
    If car_number is blank, the next open placeholder card is claimed; if none exists, the next
    available number is used.
    """
    conn = _conn()
    cur = conn.cursor()
    created = 0
    updated_placeholders = 0
    classes_created = 0
    skipped = 0
    try:
        cur.execute("BEGIN IMMEDIATE")
        for row in rows:
            owner_name = _csv_value(row, "owner_name", "owner", "name", "full name", "participant name")
            phone = _csv_value(row, "phone", "mobile", "cell")
            email = _csv_value(row, "email", "email address")
            year = _csv_value(row, "year", "vehicle year", "car year") or "TBD"
            make = _csv_value(row, "make", "vehicle make", "car make") or "TBD"
            model = _csv_value(row, "model", "vehicle model", "car model") or "TBD"
            if not owner_name and make == "TBD" and model == "TBD":
                skipped += 1
                continue
            if not owner_name:
                owner_name = "Imported Owner"
            car_number = _csv_int(_csv_value(row, "car_number", "car #", "number", "entry number"))

            if car_number is None:
                ph = cur.execute(
                    """
                    SELECT car_number FROM show_cars
                    WHERE show_id = ? AND COALESCE(is_placeholder, 0) = 1 AND COALESCE(registration_state, '') = 'placeholder'
                    ORDER BY car_number ASC LIMIT 1
                    """,
                    (int(show_id),),
                ).fetchone()
                car_number = int(ph["car_number"]) if ph else get_next_available_car_number(int(show_id))

            existing_registered = cur.execute(
                """
                SELECT id FROM show_cars
                WHERE show_id = ? AND car_number = ? AND COALESCE(is_placeholder, 0) = 0
                LIMIT 1
                """,
                (int(show_id), int(car_number)),
            ).fetchone()
            if existing_registered:
                skipped += 1
                continue

            class_code = _csv_value(row, "class_code", "class code", "code")
            class_name = _csv_value(row, "class_name", "class name", "judging_class", "judging class", "class", "car class", "division")
            class_value = class_code or class_name
            judging_class_id = _find_class_id_by_code_or_name(cur, int(show_id), class_value)
            class_created_from_import = 0
            if not judging_class_id and (class_code or class_name):
                judging_class_id = _ensure_import_class_for_show(
                    cur,
                    int(show_id),
                    class_code=class_code,
                    class_name=class_name,
                    sort_order=1000 + created,
                )
                class_created_from_import = 1 if judging_class_id else 0
                classes_created += class_created_from_import
            class_needs_review = 0
            if not judging_class_id:
                # Same logic as automatic day-of class assignment, but done inline to avoid nested write locks.
                matches = []
                try:
                    y = int(float(str(year).strip()))
                except Exception:
                    y = None
                hay_make = str(make or "").lower()
                hay_model = str(model or "").lower()
                hay_all = f"{hay_make} {hay_model}".strip()
                for cls in cur.execute("SELECT * FROM show_judging_classes WHERE show_id = ? AND is_active = 1", (int(show_id),)).fetchall():
                    ok = True
                    if cls["year_min"] is not None and (y is None or y < int(cls["year_min"])):
                        ok = False
                    if cls["year_max"] is not None and (y is None or y > int(cls["year_max"])):
                        ok = False
                    mk = (cls["make_contains"] or "").strip().lower()
                    if mk:
                        terms = [t.strip() for t in re.split(r"[,;|]", mk) if t.strip()]
                        if terms and not any(t in hay_make for t in terms):
                            ok = False
                    md = (cls["model_contains"] or "").strip().lower()
                    if md:
                        terms = [t.strip() for t in re.split(r"[,;|]", md) if t.strip()]
                        if terms and not any(t in hay_model for t in terms):
                            ok = False
                    kw = (cls["keyword_contains"] or "").strip().lower()
                    if kw:
                        terms = [t.strip() for t in re.split(r"[,;|]", kw) if t.strip()]
                        if terms and not any(t in hay_all for t in terms):
                            ok = False
                    if ok:
                        matches.append(int(cls["id"]))
                if len(matches) == 1:
                    judging_class_id = matches[0]
                elif len(matches) != 1:
                    class_needs_review = 1

            cur.execute(
                """
                INSERT INTO people (name, phone, email, opt_in_future, sponsor_opt_in, consent_text, consent_version)
                VALUES (?, ?, ?, 0, 0, ?, ?)
                """,
                (owner_name, phone, email, "Imported outside registration", "imported-outside-system"),
            )
            person_id = int(cur.lastrowid)
            waiver_received = _csv_bool(_csv_value(row, "waiver_received", "waiver", "signed waiver"), 0)
            payment_status = _csv_value(row, "payment_status", "paid status", "status") or ("paid_imported" if assume_paid else "imported")

            placeholder = cur.execute(
                """
                SELECT id, car_token FROM show_cars
                WHERE show_id = ? AND car_number = ? AND COALESCE(is_placeholder, 0) = 1 AND COALESCE(registration_state, '') = 'placeholder'
                LIMIT 1
                """,
                (int(show_id), int(car_number)),
            ).fetchone()
            if placeholder:
                cur.execute(
                    """
                    UPDATE show_cars
                    SET person_id = ?, year = ?, make = ?, model = ?, registration_payment_status = ?,
                        registration_state = 'claimed', is_placeholder = 0, waiver_received = ?,
                        waiver_received_at = CASE WHEN ? = 1 THEN COALESCE(waiver_received_at, datetime('now')) ELSE waiver_received_at END,
                        waiver_received_by = CASE WHEN ? = 1 THEN 'import' ELSE waiver_received_by END,
                        judging_class_id = ?, class_needs_review = ?
                    WHERE id = ?
                    """,
                    (person_id, year, make, model, payment_status, waiver_received, waiver_received, waiver_received,
                     judging_class_id, int(class_needs_review), int(placeholder["id"])),
                )
                updated_placeholders += 1
            else:
                cur.execute(
                    """
                    INSERT INTO show_cars (
                        show_id, person_id, car_number, car_token, year, make, model,
                        registration_payment_status, is_placeholder, registration_state,
                        waiver_received, waiver_received_at, waiver_received_by,
                        judging_class_id, class_needs_review
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'claimed', ?, CASE WHEN ? = 1 THEN datetime('now') ELSE NULL END, CASE WHEN ? = 1 THEN 'import' ELSE NULL END, ?, ?)
                    """,
                    (int(show_id), person_id, int(car_number), _new_car_token(), year, make, model, payment_status,
                     waiver_received, waiver_received, waiver_received, judging_class_id, int(class_needs_review)),
                )
            created += 1
        conn.commit()
        return {"created": created, "updated_placeholders": updated_placeholders, "classes_created": classes_created, "skipped": skipped}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ADMIN USERS / SHOW ACCESS

def get_admin_user_by_email(email: str) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        "SELECT * FROM admin_users WHERE lower(email) = lower(?) LIMIT 1",
        ((email or "").strip(),),
    ).fetchone()
    conn.close()
    return row


def get_admin_user_by_id(admin_user_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute(
        "SELECT * FROM admin_users WHERE id = ? LIMIT 1",
        (int(admin_user_id),),
    ).fetchone()
    conn.close()
    return row


def create_admin_user(
    *,
    name: str,
    email: str,
    password_hash: str,
    global_role: str = "show_owner",
    is_active: int = 1,
) -> int:
    conn = _conn()
    cur = conn.cursor()
    role = (global_role or "show_owner").strip().lower()
    if role not in {"super_admin", "show_owner", "registrar", "judge", "volunteer"}:
        role = "show_owner"
    cur.execute(
        """
        INSERT INTO admin_users (name, email, password_hash, global_role, is_active, updated_at)
        VALUES (?, lower(?), ?, ?, ?, datetime('now'))
        """,
        ((name or "").strip(), (email or "").strip(), password_hash or "", role, 1 if int(is_active or 0) else 0),
    )
    conn.commit()
    rid = int(cur.lastrowid)
    conn.close()
    return rid


def list_admin_users() -> List[sqlite3.Row]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT *
        FROM admin_users
        ORDER BY is_active DESC, global_role ASC, name ASC, email ASC
        """
    ).fetchall()
    conn.close()
    return rows


def set_admin_user_active(admin_user_id: int, is_active: int) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE admin_users SET is_active = ?, updated_at = datetime('now') WHERE id = ?",
        (1 if int(is_active or 0) else 0, int(admin_user_id)),
    )
    conn.commit()
    conn.close()


def assign_admin_user_show_role(admin_user_id: int, show_id: int, role: str) -> None:
    role = (role or "show_owner").strip().lower()
    if role not in {"show_owner", "registrar", "judge", "volunteer"}:
        role = "show_owner"
    conn = _conn()
    conn.execute(
        """
        INSERT INTO admin_user_show_roles (admin_user_id, show_id, role, is_active, updated_at)
        VALUES (?, ?, ?, 1, datetime('now'))
        ON CONFLICT(admin_user_id, show_id, role)
        DO UPDATE SET is_active = 1, updated_at = datetime('now')
        """,
        (int(admin_user_id), int(show_id), role),
    )
    conn.commit()
    conn.close()


def remove_admin_user_show_role(admin_user_id: int, show_id: int, role: str) -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE admin_user_show_roles
        SET is_active = 0, updated_at = datetime('now')
        WHERE admin_user_id = ? AND show_id = ? AND role = ?
        """,
        (int(admin_user_id), int(show_id), (role or "").strip().lower()),
    )
    conn.commit()
    conn.close()


def list_admin_user_show_roles(admin_user_id: Optional[int] = None) -> List[sqlite3.Row]:
    conn = _conn()
    if admin_user_id:
        rows = conn.execute(
            """
            SELECT r.*, s.title AS show_title, s.slug AS show_slug, u.name AS user_name, u.email AS user_email
            FROM admin_user_show_roles r
            JOIN shows s ON s.id = r.show_id
            JOIN admin_users u ON u.id = r.admin_user_id
            WHERE r.admin_user_id = ? AND r.is_active = 1
            ORDER BY s.title ASC, r.role ASC
            """,
            (int(admin_user_id),),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT r.*, s.title AS show_title, s.slug AS show_slug, u.name AS user_name, u.email AS user_email
            FROM admin_user_show_roles r
            JOIN shows s ON s.id = r.show_id
            JOIN admin_users u ON u.id = r.admin_user_id
            WHERE r.is_active = 1
            ORDER BY u.name ASC, s.title ASC, r.role ASC
            """
        ).fetchall()
    conn.close()
    return rows


def admin_user_can_access_show(admin_user_id: int, show_id: int) -> bool:
    user = get_admin_user_by_id(int(admin_user_id))
    if not user or int(user["is_active"] or 0) != 1:
        return False
    if (user["global_role"] or "").strip().lower() == "super_admin":
        return True
    conn = _conn()
    row = conn.execute(
        """
        SELECT id
        FROM admin_user_show_roles
        WHERE admin_user_id = ? AND show_id = ? AND is_active = 1
        LIMIT 1
        """,
        (int(admin_user_id), int(show_id)),
    ).fetchone()
    conn.close()
    return bool(row)


def list_show_ids_for_admin_user(admin_user_id: int) -> List[int]:
    conn = _conn()
    rows = conn.execute(
        """
        SELECT DISTINCT show_id
        FROM admin_user_show_roles
        WHERE admin_user_id = ? AND is_active = 1
        ORDER BY show_id ASC
        """,
        (int(admin_user_id),),
    ).fetchall()
    conn.close()
    return [int(r["show_id"]) for r in rows]


def list_shows_admin_for_user(admin_user_id: Optional[int], is_super_admin: bool = False) -> List[sqlite3.Row]:
    if is_super_admin or not admin_user_id:
        return list_shows_admin()
    show_ids = list_show_ids_for_admin_user(int(admin_user_id))
    if not show_ids:
        return []
    placeholders = ",".join(["?"] * len(show_ids))
    conn = _conn()
    rows = conn.execute(
        f"""
        SELECT *
        FROM shows
        WHERE id IN ({placeholders})
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
        """,
        tuple(show_ids),
    ).fetchall()
    conn.close()
    return rows


# CONTACT MESSAGES

def create_contact_message(
    *,
    name: str,
    email: str,
    phone: str = "",
    subject: str,
    message: str,
    source_page: str = "contact",
) -> int:
    conn = _conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO contact_messages (
            name, email, phone, subject, message, source_page, status, email_sent, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 'new', 0, datetime('now'))
        """,
        (
            (name or "").strip(),
            (email or "").strip(),
            (phone or "").strip(),
            (subject or "").strip(),
            (message or "").strip(),
            (source_page or "contact").strip(),
        ),
    )
    message_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return message_id


def mark_contact_message_email_result(message_id: int, *, sent: bool, error: str = "") -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE contact_messages
        SET email_sent = ?, email_error = ?
        WHERE id = ?
        """,
        (1 if sent else 0, (error or "")[:1000], int(message_id)),
    )
    conn.commit()
    conn.close()


def list_contact_messages(status: str = "open", query: str = "", limit: int = 200) -> List[sqlite3.Row]:
    status = (status or "open").strip().lower()
    query = (query or "").strip()
    params: List[Any] = []
    where = []
    if status == "archived":
        where.append("archived_at IS NOT NULL")
    elif status == "new":
        where.append("archived_at IS NULL AND status = 'new'")
    elif status == "read":
        where.append("archived_at IS NULL AND status = 'read'")
    else:
        where.append("archived_at IS NULL")
    if query:
        like = f"%{query}%"
        where.append("(name LIKE ? OR email LIKE ? OR phone LIKE ? OR subject LIKE ? OR message LIKE ?)")
        params.extend([like, like, like, like, like])
    params.append(int(limit))
    conn = _conn()
    rows = conn.execute(
        f"""
        SELECT *
        FROM contact_messages
        WHERE {' AND '.join(where)}
        ORDER BY datetime(created_at) DESC, id DESC
        LIMIT ?
        """,
        tuple(params),
    ).fetchall()
    conn.close()
    return rows


def get_contact_message(message_id: int) -> Optional[sqlite3.Row]:
    conn = _conn()
    row = conn.execute("SELECT * FROM contact_messages WHERE id = ? LIMIT 1", (int(message_id),)).fetchone()
    conn.close()
    return row


def mark_contact_message_read(message_id: int) -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE contact_messages
        SET status = 'read', read_at = COALESCE(read_at, datetime('now'))
        WHERE id = ?
        """,
        (int(message_id),),
    )
    conn.commit()
    conn.close()


def archive_contact_message(message_id: int) -> None:
    conn = _conn()
    conn.execute(
        """
        UPDATE contact_messages
        SET status = 'archived', archived_at = COALESCE(archived_at, datetime('now'))
        WHERE id = ?
        """,
        (int(message_id),),
    )
    conn.commit()
    conn.close()


def count_new_contact_messages() -> int:
    conn = _conn()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM contact_messages
        WHERE archived_at IS NULL AND status = 'new'
        """
    ).fetchone()
    conn.close()
    return int(row["c"] if row else 0)
