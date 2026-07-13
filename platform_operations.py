import csv
import io
import re
import sqlite3
from typing import Any, Dict, List, Optional


DEFAULT_CATEGORIES = [
    ("army", "Army"),
    ("navy", "Navy"),
    ("marines", "Marines"),
    ("air-force", "Air Force"),
    ("peoples-choice", "People's Choice"),
]


def init_platform_operations(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    for sql in [
        "ALTER TABLE shows ADD COLUMN platform_event_fee_cents INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE shows ADD COLUMN platform_per_transaction_fee_cents INTEGER NOT NULL DEFAULT 0",
    ]:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS show_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            slug TEXT NOT NULL,
            name TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 100,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(show_id, slug),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS platform_payment_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            show_id INTEGER NOT NULL,
            organizer_id INTEGER,
            item_type TEXT NOT NULL,
            checkout_session_id TEXT NOT NULL UNIQUE,
            payment_intent_id TEXT,
            balance_transaction_id TEXT,
            gross_amount_cents INTEGER NOT NULL DEFAULT 0,
            processing_fee_cents INTEGER NOT NULL DEFAULT 0,
            net_amount_cents INTEGER NOT NULL DEFAULT 0,
            fee_source TEXT NOT NULL DEFAULT 'actual',
            synced_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY(show_id) REFERENCES shows(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_platform_payments_show ON platform_payment_records(show_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_show_categories_show ON show_categories(show_id, sort_order)")
    show_rows = conn.execute("SELECT id FROM shows").fetchall()
    for show in show_rows:
        count = conn.execute("SELECT COUNT(*) FROM show_categories WHERE show_id = ?", (int(show["id"]),)).fetchone()[0]
        if count == 0:
            for idx, (slug, name) in enumerate(DEFAULT_CATEGORIES, start=1):
                conn.execute(
                    "INSERT OR IGNORE INTO show_categories (show_id, slug, name, sort_order) VALUES (?, ?, ?, ?)",
                    (int(show["id"]), slug, name, idx * 10),
                )
    conn.commit()
    conn.close()


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", (value or "").strip().lower()).strip("-")


def list_show_categories(db_path: str, show_id: int, active_only: bool = True) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        where = " AND is_active = 1" if active_only else ""
        rows = conn.execute(
            f"SELECT * FROM show_categories WHERE show_id = ?{where} ORDER BY sort_order, id",
            (int(show_id),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def save_show_categories(db_path: str, show_id: int, rows: List[Dict[str, Any]]) -> int:
    cleaned = []
    seen = set()
    for idx, row in enumerate(rows, start=1):
        name = (row.get("name") or "").strip()
        slug = _slugify(row.get("slug") or name)
        if not name or not slug or slug in seen:
            continue
        seen.add(slug)
        cleaned.append((slug, name, int(row.get("sort_order") or idx * 10)))
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute("DELETE FROM show_categories WHERE show_id = ?", (int(show_id),))
        conn.executemany(
            "INSERT INTO show_categories (show_id, slug, name, sort_order) VALUES (?, ?, ?, ?)",
            [(int(show_id), slug, name, order) for slug, name, order in cleaned],
        )
        conn.commit()
        return len(cleaned)
    finally:
        conn.close()


def parse_category_csv(content: bytes) -> List[Dict[str, Any]]:
    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for idx, row in enumerate(reader, start=1):
        normalized = {str(k or "").strip().lower(): v for k, v in row.items()}
        name = normalized.get("name") or normalized.get("category") or normalized.get("category name") or ""
        slug = normalized.get("slug") or normalized.get("category slug") or ""
        order = normalized.get("sort_order") or normalized.get("sort order") or idx * 10
        rows.append({"name": str(name).strip(), "slug": str(slug).strip(), "sort_order": order})
    return rows


def upsert_platform_payment_record(db_path: str, record: Dict[str, Any]) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute(
            """
            INSERT INTO platform_payment_records (
                show_id, organizer_id, item_type, checkout_session_id, payment_intent_id,
                balance_transaction_id, gross_amount_cents, processing_fee_cents,
                net_amount_cents, fee_source, synced_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'actual', datetime('now'))
            ON CONFLICT(checkout_session_id) DO UPDATE SET
                payment_intent_id = excluded.payment_intent_id,
                balance_transaction_id = excluded.balance_transaction_id,
                gross_amount_cents = excluded.gross_amount_cents,
                processing_fee_cents = excluded.processing_fee_cents,
                net_amount_cents = excluded.net_amount_cents,
                fee_source = 'actual', synced_at = datetime('now')
            """,
            (
                int(record["show_id"]),
                record.get("organizer_id"),
                record.get("item_type") or "payment",
                record["checkout_session_id"],
                record.get("payment_intent_id") or "",
                record.get("balance_transaction_id") or "",
                int(record.get("gross_amount_cents") or 0),
                int(record.get("processing_fee_cents") or 0),
                int(record.get("net_amount_cents") or 0),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def actual_processing_fees(db_path: str, show_id: int) -> Dict[str, Any]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) record_count,
                   COALESCE(SUM(processing_fee_cents), 0) processing_fee_cents,
                   COALESCE(SUM(gross_amount_cents), 0) gross_amount_cents
            FROM platform_payment_records WHERE show_id = ?
            """,
            (int(show_id),),
        ).fetchone()
        return dict(row)
    finally:
        conn.close()


def actual_processing_fee_map(db_path: str, show_id: int) -> Dict[str, int]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT checkout_session_id, processing_fee_cents FROM platform_payment_records WHERE show_id = ?",
            (int(show_id),),
        ).fetchall()
        return {str(row["checkout_session_id"]): int(row["processing_fee_cents"] or 0) for row in rows}
    finally:
        conn.close()


def update_show_platform_pricing(
    db_path: str,
    show_id: int,
    *,
    platform_fee_percent: float,
    platform_event_fee_cents: int,
    platform_per_transaction_fee_cents: int,
) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute(
            """
            UPDATE shows SET platform_fee_percent = ?, platform_event_fee_cents = ?,
                platform_per_transaction_fee_cents = ? WHERE id = ?
            """,
            (
                max(0, min(float(platform_fee_percent), 100)),
                max(0, int(platform_event_fee_cents)),
                max(0, int(platform_per_transaction_fee_cents)),
                int(show_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()
