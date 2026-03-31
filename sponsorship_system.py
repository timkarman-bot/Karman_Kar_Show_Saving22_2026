import os, sqlite3
from typing import Any, Dict, List, Optional

DB_PATH = os.getenv("DB_PATH")
if not DB_PATH:
    DB_PATH = "/data/app.db" if os.path.isdir("/data") else "app.db"

def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn

def init_sponsorship_tables() -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS sponsorship_packages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id INTEGER NOT NULL,
        package_name TEXT NOT NULL,
        description TEXT,
        price_cents INTEGER NOT NULL DEFAULT 0,
        quantity_total INTEGER NOT NULL DEFAULT 1,
        quantity_sold INTEGER NOT NULL DEFAULT 0,
        public_status TEXT NOT NULL DEFAULT 'available',
        credit_person_name TEXT,
        organizer_name TEXT,
        agreed_percent REAL NOT NULL DEFAULT 0,
        internal_notes TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    conn.commit(); conn.close()

def list_sponsorship_packages(show_id: int) -> List[Dict[str, Any]]:
    init_sponsorship_tables()
    conn = _conn()
    rows = conn.execute("SELECT * FROM sponsorship_packages WHERE show_id=? ORDER BY id ASC", (int(show_id),)).fetchall()
    conn.close()
    out = []
    for row in rows:
        price_cents = int(row["price_cents"] or 0)
        qty_total = int(row["quantity_total"] or 0)
        qty_sold = int(row["quantity_sold"] or 0)
        out.append({
            "id": int(row["id"]),
            "package_name": row["package_name"],
            "description": row["description"] or "",
            "price_cents": price_cents,
            "quantity_total": qty_total,
            "quantity_sold": qty_sold,
            "effective_public_status": "sold_out" if qty_total > 0 and qty_sold >= qty_total else "available",
            "credit_person_name": row["credit_person_name"] or "",
            "organizer_name": row["organizer_name"] or "",
            "agreed_percent": float(row["agreed_percent"] or 0),
            "payout_display": f"{float(row['agreed_percent'] or 0):.2f}% (${(price_cents * float(row['agreed_percent'] or 0)/100)/100:.2f})",
            "internal_notes": row["internal_notes"] or ""
        })
    return out

def save_sponsorship_package(*, show_id:int, package_id:Optional[int], package_name:str, description:str, price_cents:int, quantity_total:int, quantity_sold:int, credit_person_name:str, organizer_name:str, agreed_percent:float, internal_notes:str, **kwargs) -> int:
    init_sponsorship_tables()
    conn = _conn(); cur = conn.cursor()
    if package_id:
        cur.execute("""UPDATE sponsorship_packages SET package_name=?, description=?, price_cents=?, quantity_total=?, quantity_sold=?, credit_person_name=?, organizer_name=?, agreed_percent=?, internal_notes=?, updated_at=datetime('now') WHERE id=? AND show_id=?""",
                    (package_name, description, price_cents, quantity_total, quantity_sold, credit_person_name, organizer_name, agreed_percent, internal_notes, int(package_id), int(show_id)))
        rid = int(package_id)
    else:
        cur.execute("""INSERT INTO sponsorship_packages (show_id, package_name, description, price_cents, quantity_total, quantity_sold, credit_person_name, organizer_name, agreed_percent, internal_notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (int(show_id), package_name, description, price_cents, quantity_total, quantity_sold, credit_person_name, organizer_name, agreed_percent, internal_notes))
        rid = int(cur.lastrowid)
    conn.commit(); conn.close(); return rid
