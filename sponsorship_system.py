import os, sqlite3
from typing import Any, Dict, List, Optional

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

def init_sponsorship_tables() -> None:
    conn = _conn()
    cur = conn.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS sponsorship_salespeople (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        default_commission_percent REAL NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        notes TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sponsorship_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id INTEGER NOT NULL,
        package_name TEXT NOT NULL,
        description TEXT,
        price_cents INTEGER NOT NULL DEFAULT 0,
        total_available INTEGER NOT NULL DEFAULT 1,
        sort_order INTEGER NOT NULL DEFAULT 100,
        is_active INTEGER NOT NULL DEFAULT 1,
        is_public INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS sponsorship_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id INTEGER NOT NULL,
        catalog_id INTEGER,
        sponsor_business_name TEXT NOT NULL,
        contact_name TEXT,
        phone TEXT,
        email TEXT,
        website_url TEXT,
        salesperson_id INTEGER,
        salesperson_name_snapshot TEXT,
        commission_percent REAL NOT NULL DEFAULT 0,
        gross_amount_cents INTEGER NOT NULL DEFAULT 0,
        commission_amount_cents INTEGER NOT NULL DEFAULT 0,
        net_amount_cents INTEGER NOT NULL DEFAULT 0,
        logo_path TEXT,
        logo_pending INTEGER NOT NULL DEFAULT 0,
        placement TEXT NOT NULL DEFAULT 'standard',
        payment_status TEXT NOT NULL DEFAULT 'pending',
        status TEXT NOT NULL DEFAULT 'open',
        notes TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""")
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
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_sponsorship_catalog_show_id ON sponsorship_catalog(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_sponsorship_sales_show_id ON sponsorship_sales(show_id)",
        "CREATE INDEX IF NOT EXISTS idx_sponsorship_sales_catalog_id ON sponsorship_sales(catalog_id)",
        "CREATE INDEX IF NOT EXISTS idx_sponsorship_sales_salesperson_id ON sponsorship_sales(salesperson_id)",
    ]:
        cur.execute(sql)
    conn.commit(); conn.close()

def _d(row): return {k: row[k] for k in row.keys()}

def list_salespeople(active_only: bool = False) -> List[Dict[str, Any]]:
    init_sponsorship_tables()
    conn = _conn()
    q = "SELECT * FROM sponsorship_salespeople"
    if active_only: q += " WHERE is_active = 1"
    q += " ORDER BY is_active DESC, name ASC"
    rows = conn.execute(q).fetchall()
    conn.close()
    return [_d(r) for r in rows]

def get_salesperson(salesperson_id: int) -> Optional[Dict[str, Any]]:
    init_sponsorship_tables()
    conn = _conn()
    row = conn.execute("SELECT * FROM sponsorship_salespeople WHERE id = ? LIMIT 1", (int(salesperson_id),)).fetchone()
    conn.close()
    return _d(row) if row else None

def save_salesperson(*, salesperson_id: Optional[int], name: str, default_commission_percent: float, is_active: int = 1, notes: str = "") -> int:
    init_sponsorship_tables()
    conn = _conn(); cur = conn.cursor()
    if salesperson_id:
        cur.execute("UPDATE sponsorship_salespeople SET name=?, default_commission_percent=?, is_active=?, notes=?, updated_at=datetime('now') WHERE id=?",
                    ((name or "").strip(), float(default_commission_percent or 0), int(is_active), (notes or "").strip(), int(salesperson_id)))
        rid = int(salesperson_id)
    else:
        cur.execute("INSERT INTO sponsorship_salespeople (name, default_commission_percent, is_active, notes) VALUES (?, ?, ?, ?)",
                    ((name or "").strip(), float(default_commission_percent or 0), int(is_active), (notes or "").strip()))
        rid = int(cur.lastrowid)
    conn.commit(); conn.close(); return rid

def list_sponsorship_catalog(show_id: int) -> List[Dict[str, Any]]:
    init_sponsorship_tables()
    conn = _conn()
    rows = conn.execute("""SELECT c.*, COALESCE((SELECT COUNT(*) FROM sponsorship_sales s WHERE s.show_id=c.show_id AND s.catalog_id=c.id AND s.status='sold'),0) AS sold_count
                           FROM sponsorship_catalog c WHERE c.show_id=? AND c.is_active=1 ORDER BY c.sort_order ASC, c.id ASC""", (int(show_id),)).fetchall()
    conn.close()
    out = []
    for row in rows:
        d = _d(row)
        sold = int(d.get("sold_count") or 0)
        total = int(d.get("total_available") or 0)
        d["left_count"] = max(0, total - sold)
        d["effective_public_status"] = "sold_out" if total > 0 and sold >= total else "available"
        out.append(d)
    return out

def save_catalog_item(*, catalog_id: Optional[int], show_id: int, package_name: str, description: str, price_cents: int, total_available: int, sort_order: int = 100, is_active: int = 1, is_public: int = 1) -> int:
    init_sponsorship_tables()
    conn = _conn(); cur = conn.cursor()
    if catalog_id:
        cur.execute("""UPDATE sponsorship_catalog SET package_name=?, description=?, price_cents=?, total_available=?, sort_order=?, is_active=?, is_public=?, updated_at=datetime('now')
                       WHERE id=? AND show_id=?""",
                    ((package_name or "").strip(), (description or "").strip(), max(0, int(price_cents or 0)), max(0, int(total_available or 0)), int(sort_order or 100), int(is_active), int(is_public), int(catalog_id), int(show_id)))
        rid = int(catalog_id)
    else:
        cur.execute("""INSERT INTO sponsorship_catalog (show_id, package_name, description, price_cents, total_available, sort_order, is_active, is_public)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (int(show_id), (package_name or "").strip(), (description or "").strip(), max(0, int(price_cents or 0)), max(0, int(total_available or 0)), int(sort_order or 100), int(is_active), int(is_public)))
        rid = int(cur.lastrowid)
    conn.commit(); conn.close(); return rid

def list_sponsorship_sales(show_id: int) -> List[Dict[str, Any]]:
    init_sponsorship_tables()
    conn = _conn()
    rows = conn.execute("""SELECT s.*, c.package_name AS package_name
                           FROM sponsorship_sales s LEFT JOIN sponsorship_catalog c ON c.id=s.catalog_id
                           WHERE s.show_id=? ORDER BY s.created_at DESC, s.id DESC""", (int(show_id),)).fetchall()
    conn.close()
    return [_d(r) for r in rows]

def save_sponsorship_sale(*, sale_id: Optional[int], show_id: int, catalog_id: Optional[int], sponsor_business_name: str, contact_name: str, phone: str, email: str, website_url: str, salesperson_id: Optional[int], commission_percent: float, logo_path: str, logo_pending: int, placement: str, payment_status: str, status: str, notes: str) -> int:
    init_sponsorship_tables()
    conn = _conn(); cur = conn.cursor()
    salesperson_name_snapshot = ""
    if salesperson_id:
        row = cur.execute("SELECT name FROM sponsorship_salespeople WHERE id=? LIMIT 1", (int(salesperson_id),)).fetchone()
        if row: salesperson_name_snapshot = row["name"]
    gross_amount_cents = 0
    if catalog_id:
        cat = cur.execute("SELECT price_cents FROM sponsorship_catalog WHERE id=? LIMIT 1", (int(catalog_id),)).fetchone()
        if cat: gross_amount_cents = int(cat["price_cents"] or 0)
    commission_amount_cents = int(round(gross_amount_cents * (float(commission_percent or 0) / 100.0)))
    net_amount_cents = gross_amount_cents - commission_amount_cents
    payload = (
        int(show_id), int(catalog_id) if catalog_id else None, (sponsor_business_name or "").strip(), (contact_name or "").strip(),
        (phone or "").strip(), (email or "").strip(), (website_url or "").strip(),
        int(salesperson_id) if salesperson_id else None, salesperson_name_snapshot, float(commission_percent or 0),
        gross_amount_cents, commission_amount_cents, net_amount_cents,
        (logo_path or "").strip(), int(logo_pending), (placement or "standard").strip(),
        (payment_status or "pending").strip(), (status or "open").strip(), (notes or "").strip()
    )
    if sale_id:
        cur.execute("""UPDATE sponsorship_sales SET show_id=?, catalog_id=?, sponsor_business_name=?, contact_name=?, phone=?, email=?, website_url=?, salesperson_id=?, salesperson_name_snapshot=?, commission_percent=?, gross_amount_cents=?, commission_amount_cents=?, net_amount_cents=?, logo_path=?, logo_pending=?, placement=?, payment_status=?, status=?, notes=?, updated_at=datetime('now') WHERE id=?""", payload + (int(sale_id),))
        rid = int(sale_id)
    else:
        cur.execute("""INSERT INTO sponsorship_sales (show_id, catalog_id, sponsor_business_name, contact_name, phone, email, website_url, salesperson_id, salesperson_name_snapshot, commission_percent, gross_amount_cents, commission_amount_cents, net_amount_cents, logo_path, logo_pending, placement, payment_status, status, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", payload)
        rid = int(cur.lastrowid)
    conn.commit(); conn.close(); return rid

# Backward-compat wrappers
def list_sponsorship_packages(show_id: int) -> List[Dict[str, Any]]:
    rows = list_sponsorship_catalog(show_id)
    return [{"id": int(r["id"]), "package_name": r["package_name"], "description": r.get("description") or "", "price_cents": int(r.get("price_cents") or 0), "quantity_total": int(r.get("total_available") or 0), "quantity_sold": int(r.get("sold_count") or 0), "effective_public_status": r.get("effective_public_status", "available"), "credit_person_name": "", "organizer_name": "", "agreed_percent": 0.0, "payout_display": "0.00% ($0.00)", "internal_notes": ""} for r in rows]

def save_sponsorship_package(*, show_id: int, package_id: Optional[int], package_name: str, description: str, price_cents: int, quantity_total: int, **kwargs) -> int:
    return save_catalog_item(catalog_id=package_id, show_id=show_id, package_name=package_name, description=description, price_cents=price_cents, total_available=quantity_total, sort_order=int(kwargs.get("sort_order", 100) or 100), is_active=1, is_public=1)
