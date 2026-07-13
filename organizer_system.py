import re
import secrets
import sqlite3
from typing import Any, Dict, List, Optional

from werkzeug.security import check_password_hash, generate_password_hash


def init_organizer_tables(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS organizers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organization_name TEXT NOT NULL,
            contact_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            phone TEXT,
            password_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    for sql in [
        "ALTER TABLE shows ADD COLUMN organizer_id INTEGER",
        "ALTER TABLE shows ADD COLUMN payment_collection_mode TEXT NOT NULL DEFAULT 'platform'",
        "ALTER TABLE shows ADD COLUMN platform_fee_percent REAL NOT NULL DEFAULT 10.0",
        "ALTER TABLE shows ADD COLUMN processing_fee_percent REAL NOT NULL DEFAULT 2.9",
        "ALTER TABLE shows ADD COLUMN processing_fee_fixed_cents INTEGER NOT NULL DEFAULT 30",
        "ALTER TABLE shows ADD COLUMN payment_reference TEXT",
        "ALTER TABLE shows ADD COLUMN organizer_notes TEXT",
    ]:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass
    rows = conn.execute("SELECT id FROM shows WHERE payment_reference IS NULL OR payment_reference = ''").fetchall()
    for row in rows:
        conn.execute(
            "UPDATE shows SET payment_reference = ? WHERE id = ?",
            (show_reference(int(row["id"])), int(row["id"])),
        )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_shows_organizer_id ON shows(organizer_id)")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_shows_payment_reference ON shows(payment_reference)")
    conn.commit()
    conn.close()


def show_reference(show_id: int) -> str:
    return f"KKS-SHOW-{int(show_id):06d}"


def _slugify(value: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")
    return value or f"show-{secrets.token_hex(3)}"


def create_organizer(
    db_path: str,
    *,
    organization_name: str,
    contact_name: str,
    email: str,
    phone: str,
    password: str,
) -> int:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute(
            """
            INSERT INTO organizers (organization_name, contact_name, email, phone, password_hash)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                organization_name.strip(),
                contact_name.strip(),
                email.strip().lower(),
                phone.strip(),
                generate_password_hash(password),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)
    finally:
        conn.close()


def authenticate_organizer(db_path: str, email: str, password: str) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM organizers WHERE lower(email) = lower(?) AND status = 'active' LIMIT 1",
            (email.strip(),),
        ).fetchone()
        if not row or not check_password_hash(row["password_hash"], password or ""):
            return None
        return dict(row)
    finally:
        conn.close()


def get_organizer(db_path: str, organizer_id: int) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM organizers WHERE id = ?", (int(organizer_id),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def list_organizer_shows(db_path: str, organizer_id: int) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        return [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM shows WHERE organizer_id = ? ORDER BY created_at DESC, id DESC",
                (int(organizer_id),),
            ).fetchall()
        ]
    finally:
        conn.close()


def list_platform_organizers(db_path: str) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT o.*, COUNT(s.id) AS show_count
            FROM organizers o
            LEFT JOIN shows s ON s.organizer_id = o.id
            GROUP BY o.id
            ORDER BY o.created_at DESC, o.id DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def list_platform_organizer_shows(db_path: str) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT s.*, o.organization_name, o.contact_name, o.email AS organizer_email
            FROM shows s
            JOIN organizers o ON o.id = s.organizer_id
            ORDER BY s.created_at DESC, s.id DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def create_organizer_show(db_path: str, organizer_id: int, payload: Dict[str, Any]) -> int:
    title = (payload.get("title") or "").strip()
    base_slug = _slugify(title)
    slug = base_slug
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        suffix = 2
        while conn.execute("SELECT 1 FROM shows WHERE slug = ?", (slug,)).fetchone():
            slug = f"{base_slug}-{suffix}"
            suffix += 1
        cur = conn.execute(
            """
            INSERT INTO shows (
                slug, title, date, time, location_name, address, benefiting, description,
                status, show_on_site, voting_open, is_active, organizer_id, organizer_name,
                charity_name, payment_mode, payment_collection_mode, platform_fee_percent,
                processing_fee_percent, processing_fee_fixed_cents,
                registration_fee_cents, attendee_fee_cents, vote_price_cents,
                voting_mode, allow_sponsorships, payment_reference
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', 0, 0, 0, ?, ?, ?, 'stripe', ?, ?, ?, ?, ?, ?, ?, ?, ?, '')
            """,
            (
                slug,
                title,
                (payload.get("date") or "").strip(),
                (payload.get("time") or "").strip(),
                (payload.get("location_name") or "").strip(),
                (payload.get("address") or "").strip(),
                (payload.get("charity_name") or "").strip(),
                (payload.get("description") or "").strip(),
                int(organizer_id),
                (payload.get("organizer_name") or "").strip(),
                (payload.get("charity_name") or "").strip(),
                (payload.get("payment_collection_mode") or "platform").strip(),
                float(payload.get("platform_fee_percent") or 10),
                float(payload.get("processing_fee_percent") or 2.9),
                int(payload.get("processing_fee_fixed_cents") or 30),
                int(payload.get("registration_fee_cents") or 0),
                int(payload.get("attendee_fee_cents") or 0),
                int(payload.get("vote_price_cents") or 100),
                (payload.get("voting_mode") or "fundraiser_unlimited").strip(),
                1 if payload.get("allow_sponsorships") else 0,
            ),
        )
        show_id = int(cur.lastrowid)
        conn.execute("UPDATE shows SET payment_reference = ? WHERE id = ?", (show_reference(show_id), show_id))
        conn.commit()
        return show_id
    finally:
        conn.close()


def get_organizer_show(db_path: str, organizer_id: int, show_id: int) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM shows WHERE id = ? AND organizer_id = ?",
            (int(show_id), int(organizer_id)),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def show_transaction_report(db_path: str, show_id: int) -> Dict[str, Any]:
    from platform_operations import actual_processing_fee_map

    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        show = conn.execute("SELECT * FROM shows WHERE id = ?", (int(show_id),)).fetchone()
        if not show:
            return {"rows": [], "totals": {}}
        rows: List[Dict[str, Any]] = []
        sources = [
            ("Registration", "registration_intents", "amount_cents", "stripe_session_id", "paid_at", "payment_status = 'paid'"),
            ("Voting", "vote_intents", "amount_cents", "stripe_session_id", "paid_at", "payment_status = 'paid'"),
            ("Attendance", "donations", "amount_cents", "stripe_session_id", "paid_at", "status = 'paid'"),
        ]
        for label, table, amount_col, session_col, date_col, paid_where in sources:
            query = f"SELECT id, {amount_col} amount_cents, {session_col} stripe_session_id, {date_col} paid_at FROM {table} WHERE show_id = ? AND {paid_where}"
            for row in conn.execute(query, (int(show_id),)).fetchall():
                rows.append(
                    {
                        "type": label,
                        "source_id": int(row["id"]),
                        "amount_cents": int(row["amount_cents"] or 0),
                        "stripe_session_id": row["stripe_session_id"] or "",
                        "paid_at": row["paid_at"] or "",
                    }
                )
        try:
            sponsor_rows = conn.execute(
                """
                SELECT id, final_price_cents amount_cents, stripe_checkout_session_id stripe_session_id,
                       updated_at paid_at
                FROM sponsorship_sales
                WHERE show_id = ? AND payment_status IN ('paid', 'manual_paid')
                """,
                (int(show_id),),
            ).fetchall()
            for row in sponsor_rows:
                rows.append({"type": "Sponsorship", "source_id": int(row["id"]), "amount_cents": int(row["amount_cents"] or 0), "stripe_session_id": row["stripe_session_id"] or "", "paid_at": row["paid_at"] or ""})
        except sqlite3.OperationalError:
            pass
        totals: Dict[str, int] = {}
        gross = 0
        for row in rows:
            totals[row["type"]] = totals.get(row["type"], 0) + row["amount_cents"]
            gross += row["amount_cents"]
        fee_percent = float(show["platform_fee_percent"] or 0)
        fee_cents = int(round(gross * fee_percent / 100)) if show["payment_collection_mode"] == "platform" else 0
        transaction_count = sum(1 for row in rows if row["amount_cents"] > 0)
        fee_cents += int(show["platform_event_fee_cents"] or 0) if show["payment_collection_mode"] == "platform" else 0
        fee_cents += transaction_count * int(show["platform_per_transaction_fee_cents"] or 0) if show["payment_collection_mode"] == "platform" else 0
        processing_fee_cents = 0
        actual_fee_count = 0
        if show["payment_collection_mode"] == "platform":
            processing_percent = float(show["processing_fee_percent"] or 0)
            processing_fixed = int(show["processing_fee_fixed_cents"] or 0)
            actual_map = actual_processing_fee_map(db_path, show_id)
            for row in rows:
                if row["amount_cents"] <= 0:
                    continue
                session_id = row["stripe_session_id"]
                if session_id in actual_map:
                    row["processing_fee_cents"] = actual_map[session_id]
                    row["processing_fee_source"] = "Actual"
                    actual_fee_count += 1
                else:
                    row["processing_fee_cents"] = min(row["amount_cents"], int(round(row["amount_cents"] * processing_percent / 100)) + processing_fixed)
                    row["processing_fee_source"] = "Estimated"
                processing_fee_cents += row["processing_fee_cents"]
        return {
            "show": dict(show),
            "rows": sorted(rows, key=lambda row: row["paid_at"], reverse=True),
            "totals": totals,
            "gross_cents": gross,
            "platform_fee_cents": fee_cents,
            "processing_fee_cents": processing_fee_cents,
            "actual_fee_count": actual_fee_count,
            "transaction_count": transaction_count,
            "organizer_net_cents": gross - fee_cents - processing_fee_cents,
        }
    finally:
        conn.close()
