import csv
import io
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo


DEFAULT_VENDOR_SETTINGS = {
    "vendors_enabled": 0,
    "vendor_public_status": "draft",
    "vendor_headline": "Vendor Registration",
    "vendor_instructions": "",
    "vendor_agreement": "",
    "vendor_policy_version": "vendor-policy-2026-07",
    "vendor_open_at": "",
    "vendor_deadline": "",
    "vendor_overall_max": None,
    "vendor_reserved_sponsor_spaces": 0,
    "food_vendors_enabled": 1,
}


def _utcnow() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


DEFAULT_VENDOR_TIMEZONE = "America/Chicago"


def _vendor_tz(timezone_name: str = DEFAULT_VENDOR_TIMEZONE):
    try:
        return ZoneInfo(timezone_name or DEFAULT_VENDOR_TIMEZONE)
    except Exception:
        return timezone(timedelta(hours=-6), DEFAULT_VENDOR_TIMEZONE)


def _parse_dt(value: str, timezone_name: str = DEFAULT_VENDOR_TIMEZONE) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    local_tz = _vendor_tz(timezone_name)
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=local_tz)
        except ValueError:
            pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(local_tz) if parsed.tzinfo else parsed.replace(tzinfo=local_tz)
    except ValueError:
        return None


def _format_local_dt(value: str, timezone_name: str = DEFAULT_VENDOR_TIMEZONE) -> str:
    parsed = _parse_dt(value, timezone_name)
    if not parsed:
        return (value or "").strip()
    day = parsed.day
    hour = parsed.strftime("%I").lstrip("0") or "0"
    return f"{parsed.strftime('%B')} {day}, {parsed.year} at {hour}:{parsed.strftime('%M %p')}"


def init_vendor_tables(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS show_vendor_settings (
                show_id INTEGER PRIMARY KEY,
                vendors_enabled INTEGER NOT NULL DEFAULT 0,
                vendor_public_status TEXT NOT NULL DEFAULT 'draft',
                vendor_headline TEXT NOT NULL DEFAULT 'Vendor Registration',
                vendor_instructions TEXT NOT NULL DEFAULT '',
                vendor_agreement TEXT NOT NULL DEFAULT '',
                vendor_policy_version TEXT NOT NULL DEFAULT 'vendor-policy-2026-07',
                vendor_open_at TEXT NOT NULL DEFAULT '',
                vendor_deadline TEXT NOT NULL DEFAULT '',
                vendor_overall_max INTEGER,
                vendor_reserved_sponsor_spaces INTEGER NOT NULL DEFAULT 0,
                food_vendors_enabled INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(show_id) REFERENCES shows(id)
            )
            """
        )
        for sql in [
            "ALTER TABLE show_vendor_settings ADD COLUMN vendor_policy_version TEXT NOT NULL DEFAULT 'vendor-policy-2026-07'",
            "ALTER TABLE show_vendor_settings ADD COLUMN vendor_open_at TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE show_vendor_settings ADD COLUMN vendor_deadline TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE show_vendor_settings ADD COLUMN vendor_overall_max INTEGER",
            "ALTER TABLE show_vendor_settings ADD COLUMN vendor_reserved_sponsor_spaces INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE show_vendor_settings ADD COLUMN food_vendors_enabled INTEGER NOT NULL DEFAULT 1",
        ]:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                pass
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS show_vendor_packages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                show_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                price_cents INTEGER NOT NULL DEFAULT 0,
                capacity INTEGER,
                reserved_sponsor_spaces INTEGER NOT NULL DEFAULT 0,
                is_food INTEGER NOT NULL DEFAULT 0,
                is_closed INTEGER NOT NULL DEFAULT 0,
                sort_order INTEGER NOT NULL DEFAULT 100,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(show_id) REFERENCES shows(id)
            )
            """
        )
        for sql in [
            "ALTER TABLE show_vendor_packages ADD COLUMN reserved_sponsor_spaces INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE show_vendor_packages ADD COLUMN is_food INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE show_vendor_packages ADD COLUMN is_closed INTEGER NOT NULL DEFAULT 0",
        ]:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                pass
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vendor_registrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                show_id INTEGER NOT NULL,
                package_id INTEGER NOT NULL,
                hold_token TEXT NOT NULL UNIQUE,
                confirmation_number TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'hold',
                business_name TEXT NOT NULL,
                contact_name TEXT NOT NULL,
                email TEXT NOT NULL,
                phone TEXT NOT NULL DEFAULT '',
                website_url TEXT NOT NULL DEFAULT '',
                products TEXT NOT NULL DEFAULT '',
                electricity_request INTEGER NOT NULL DEFAULT 0,
                special_space_request TEXT NOT NULL DEFAULT '',
                is_food_vendor INTEGER NOT NULL DEFAULT 0,
                food_details TEXT NOT NULL DEFAULT '',
                insurance_ack INTEGER NOT NULL DEFAULT 0,
                rules_accepted INTEGER NOT NULL DEFAULT 0,
                refund_accepted INTEGER NOT NULL DEFAULT 0,
                accepted_policy_version TEXT NOT NULL DEFAULT '',
                accepted_policy_text TEXT NOT NULL DEFAULT '',
                accepted_at TEXT NOT NULL DEFAULT '',
                checkout_session_id TEXT NOT NULL DEFAULT '',
                stripe_payment_intent_id TEXT NOT NULL DEFAULT '',
                payment_status TEXT NOT NULL DEFAULT 'unpaid',
                amount_cents INTEGER NOT NULL DEFAULT 0,
                hold_expires_at TEXT NOT NULL DEFAULT '',
                capacity_exception INTEGER NOT NULL DEFAULT 0,
                email_status TEXT NOT NULL DEFAULT 'not_sent',
                email_error TEXT NOT NULL DEFAULT '',
                admin_notes TEXT NOT NULL DEFAULT '',
                booth_assignment TEXT NOT NULL DEFAULT '',
                checked_in_at TEXT NOT NULL DEFAULT '',
                canceled_at TEXT NOT NULL DEFAULT '',
                cancellation_releases_slot INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY(show_id) REFERENCES shows(id),
                FOREIGN KEY(package_id) REFERENCES show_vendor_packages(id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vendor_packages_show ON show_vendor_packages(show_id, sort_order)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vendor_regs_show_status ON vendor_registrations(show_id, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vendor_regs_package_status ON vendor_registrations(package_id, status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vendor_regs_checkout ON vendor_registrations(checkout_session_id)")
        conn.commit()
    finally:
        conn.close()


def get_vendor_settings(db_path: str, show_id: int) -> Dict[str, Any]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM show_vendor_settings WHERE show_id = ?", (int(show_id),)).fetchone()
        if row:
            return dict(row)
        settings = dict(DEFAULT_VENDOR_SETTINGS)
        settings["show_id"] = int(show_id)
        return settings
    finally:
        conn.close()


def save_vendor_settings(db_path: str, show_id: int, data: Dict[str, Any]) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute(
            """
            INSERT INTO show_vendor_settings (
                show_id, vendors_enabled, vendor_public_status, vendor_headline,
                vendor_instructions, vendor_agreement, vendor_policy_version,
                vendor_open_at, vendor_deadline, vendor_overall_max, vendor_reserved_sponsor_spaces,
                food_vendors_enabled, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(show_id) DO UPDATE SET
                vendors_enabled = excluded.vendors_enabled,
                vendor_public_status = excluded.vendor_public_status,
                vendor_headline = excluded.vendor_headline,
                vendor_instructions = excluded.vendor_instructions,
                vendor_agreement = excluded.vendor_agreement,
                vendor_policy_version = excluded.vendor_policy_version,
                vendor_open_at = excluded.vendor_open_at,
                vendor_deadline = excluded.vendor_deadline,
                vendor_overall_max = excluded.vendor_overall_max,
                vendor_reserved_sponsor_spaces = excluded.vendor_reserved_sponsor_spaces,
                food_vendors_enabled = excluded.food_vendors_enabled,
                updated_at = datetime('now')
            """,
            (
                int(show_id),
                1 if data.get("vendors_enabled") else 0,
                (data.get("vendor_public_status") or "draft").strip(),
                (data.get("vendor_headline") or "Vendor Registration").strip(),
                (data.get("vendor_instructions") or "").strip(),
                (data.get("vendor_agreement") or "").strip(),
                (data.get("vendor_policy_version") or "vendor-policy-2026-07").strip(),
                (data.get("vendor_open_at") or "").strip(),
                (data.get("vendor_deadline") or "").strip(),
                data.get("vendor_overall_max"),
                max(0, int(data.get("vendor_reserved_sponsor_spaces") or 0)),
                1 if data.get("food_vendors_enabled") else 0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_vendor_packages(db_path: str, show_id: int, active_only: bool = False) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        where = " AND is_active = 1" if active_only else ""
        rows = conn.execute(
            f"SELECT * FROM show_vendor_packages WHERE show_id = ?{where} ORDER BY sort_order, id",
            (int(show_id),),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def save_vendor_packages(db_path: str, show_id: int, rows: List[Dict[str, Any]]) -> int:
    cleaned = []
    for idx, row in enumerate(rows, start=1):
        name = (row.get("name") or "").strip()
        if not name:
            continue
        cleaned.append(
            (
                int(row.get("id") or 0),
                int(show_id),
                name,
                (row.get("description") or "").strip(),
                max(0, int(row.get("price_cents") or 0)),
                row.get("capacity"),
                max(0, int(row.get("reserved_sponsor_spaces") or 0)),
                1 if row.get("is_food") else 0,
                1 if row.get("is_closed") else 0,
                int(row.get("sort_order") or idx * 10),
                1 if row.get("is_active") else 0,
            )
        )
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        kept_ids = []
        for row in cleaned:
            package_id = int(row[0])
            values = row[1:]
            if package_id:
                conn.execute(
                    """
                    UPDATE show_vendor_packages
                    SET name = ?, description = ?, price_cents = ?, capacity = ?,
                        reserved_sponsor_spaces = ?, is_food = ?, is_closed = ?,
                        sort_order = ?, is_active = ?
                    WHERE id = ? AND show_id = ?
                    """,
                    (values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9], package_id, int(show_id)),
                )
                kept_ids.append(package_id)
            else:
                cur = conn.execute(
                    """
                    INSERT INTO show_vendor_packages (
                        show_id, name, description, price_cents, capacity, reserved_sponsor_spaces,
                        is_food, is_closed, sort_order, is_active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    values,
                )
                kept_ids.append(int(cur.lastrowid))
        if kept_ids:
            placeholders = ",".join("?" for _ in kept_ids)
            conn.execute(
                f"UPDATE show_vendor_packages SET is_active = 0, is_closed = 1 WHERE show_id = ? AND id NOT IN ({placeholders})",
                [int(show_id), *kept_ids],
            )
        conn.commit()
        return len(cleaned)
    finally:
        conn.close()


def vendor_counts(db_path: str, show_id: int) -> Dict[str, Any]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        regs = conn.execute(
            """
            SELECT package_id, status, COUNT(*) count
            FROM vendor_registrations
            WHERE show_id = ? AND status IN ('hold', 'confirmed')
            GROUP BY package_id, status
            """,
            (int(show_id),),
        ).fetchall()
        by_package: Dict[int, Dict[str, int]] = {}
        for row in regs:
            by_package.setdefault(int(row["package_id"]), {"hold": 0, "confirmed": 0})
            by_package[int(row["package_id"])][str(row["status"])] = int(row["count"] or 0)
        total_confirmed = sum(v.get("confirmed", 0) for v in by_package.values())
        total_holds = sum(v.get("hold", 0) for v in by_package.values())
        return {"by_package": by_package, "total_confirmed": total_confirmed, "total_holds": total_holds}
    finally:
        conn.close()


def package_availability(db_path: str, show_id: int) -> List[Dict[str, Any]]:
    settings = get_vendor_settings(db_path, show_id)
    counts = vendor_counts(db_path, show_id)
    rows = []
    for package in list_vendor_packages(db_path, show_id, active_only=False):
        package_counts = counts["by_package"].get(int(package["id"]), {"hold": 0, "confirmed": 0})
        raw_capacity = package.get("capacity")
        has_valid_capacity = raw_capacity is not None and int(raw_capacity or 0) > 0
        public_capacity = None
        if raw_capacity is not None:
            public_capacity = max(0, int(raw_capacity or 0) - int(package.get("reserved_sponsor_spaces") or 0))
        used = int(package_counts.get("confirmed", 0)) + int(package_counts.get("hold", 0))
        remaining = None if public_capacity is None else max(0, public_capacity - used)
        selectable = (
            int(package.get("is_active") or 0) == 1
            and int(package.get("is_closed") or 0) == 0
            and has_valid_capacity
            and public_capacity is not None
            and public_capacity > 0
            and remaining > 0
            and (int(settings.get("food_vendors_enabled") or 0) == 1 or int(package.get("is_food") or 0) == 0)
        )
        item = dict(package)
        item.update({
            "confirmed_count": int(package_counts.get("confirmed", 0)),
            "held_count": int(package_counts.get("hold", 0)),
            "public_capacity": public_capacity,
            "has_valid_capacity": has_valid_capacity,
            "remaining_count": remaining,
            "is_full": remaining == 0 if remaining is not None else False,
            "selectable": selectable,
        })
        rows.append(item)
    return rows


def vendor_registration_open(db_path: str, show_id: int, timezone_name: str = DEFAULT_VENDOR_TIMEZONE) -> Dict[str, Any]:
    settings = get_vendor_settings(db_path, show_id)
    counts = vendor_counts(db_path, show_id)
    deadline = _parse_dt(settings.get("vendor_deadline", ""), timezone_name)
    open_at = _parse_dt(settings.get("vendor_open_at", ""), timezone_name)
    now = datetime.now(_vendor_tz(timezone_name))
    deadline_passed = bool(deadline and now > deadline)
    before_open = bool(open_at and now < open_at)
    overall_max = settings.get("vendor_overall_max")
    overall_remaining = None
    valid_overall_max = overall_max is not None and int(overall_max or 0) > 0
    if valid_overall_max:
        overall_remaining = max(0, int(overall_max) - int(settings.get("vendor_reserved_sponsor_spaces") or 0) - counts["total_confirmed"] - counts["total_holds"])
    packages = package_availability(db_path, show_id)
    active_packages = [p for p in packages if int(p.get("is_active") or 0) == 1 and int(p.get("is_closed") or 0) == 0]
    active_with_capacity = [p for p in active_packages if p.get("has_valid_capacity") and (p.get("public_capacity") or 0) > 0]
    any_package = any(p["selectable"] for p in packages)
    has_policy = bool((settings.get("vendor_agreement") or "").strip())
    has_no_refund_policy = "refund" in (settings.get("vendor_agreement") or "").lower()
    setup_warnings: List[str] = []
    if not active_packages:
        setup_warnings.append("Add at least one active vendor category.")
    elif not active_with_capacity:
        setup_warnings.append("Set a maximum greater than zero for at least one active vendor category.")
    if not valid_overall_max:
        setup_warnings.append("Set an overall vendor maximum greater than zero.")
    if not has_policy or not has_no_refund_policy:
        setup_warnings.append("Add the vendor rules and no-refund policy.")
    if active_with_capacity and not any_package:
        setup_warnings.append("All vendor categories are currently full.")
    setup_ready = not setup_warnings
    open_now = (
        int(settings.get("vendors_enabled") or 0) == 1
        and settings.get("vendor_public_status") == "open"
        and not before_open
        and not deadline_passed
        and setup_ready
        and any_package
        and overall_remaining is not None
        and overall_remaining > 0
    )
    reason = ""
    public_reason = ""
    status_label = "Not configured"
    if not int(settings.get("vendors_enabled") or 0):
        reason = "Vendor registration is not enabled for this show."
        public_reason = "Vendor registration is not currently available for this event."
        status_label = "Disabled"
    elif before_open:
        formatted_open = _format_local_dt(settings.get("vendor_open_at", ""), timezone_name)
        reason = f"Vendor registration opens {formatted_open}."
        public_reason = reason
        status_label = f"Opens {formatted_open}"
    elif settings.get("vendor_public_status") != "open":
        if settings.get("vendor_public_status") == "closed":
            reason = "Vendor registration has been manually closed."
            status_label = "Closed manually"
        else:
            reason = "Vendor registration is not configured for public signup yet."
            status_label = "Not configured"
        public_reason = "Vendor registration is not currently available for this event."
    elif deadline_passed:
        reason = "The vendor registration deadline has passed."
        public_reason = "Vendor registration is not currently available for this event."
        status_label = "Closed by deadline"
    elif not setup_ready:
        reason = "Vendor registration setup is incomplete."
        public_reason = "Vendor registration is not currently available for this event."
        status_label = "Not configured" if setup_warnings else "Closed"
    elif overall_remaining == 0:
        reason = "Vendor registration is full for this show."
        public_reason = reason
        status_label = "Full"
    elif not any_package:
        reason = "All vendor categories are full or closed."
        public_reason = "Vendor registration is not currently available for this event."
        status_label = "Full"
    elif open_now:
        status_label = "Open"
    if not public_reason:
        public_reason = reason
    return {
        "open": open_now,
        "reason": reason,
        "public_reason": public_reason,
        "status_label": status_label,
        "setup_ready": setup_ready,
        "setup_warnings": setup_warnings,
        "settings": settings,
        "packages": packages,
        "overall_remaining": overall_remaining,
        "total_confirmed": counts["total_confirmed"],
        "total_holds": counts["total_holds"],
    }


def cleanup_expired_vendor_holds(db_path: str) -> int:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute(
            """
            UPDATE vendor_registrations
            SET status = 'expired', updated_at = datetime('now')
            WHERE status = 'hold' AND hold_expires_at < ?
            """,
            (_utcnow(),),
        )
        conn.commit()
        return int(cur.rowcount or 0)
    finally:
        conn.close()


def get_vendor_registration(db_path: str, registration_id: int) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT vr.*, svp.name package_name, svp.description package_description
            FROM vendor_registrations vr
            JOIN show_vendor_packages svp ON svp.id = vr.package_id
            WHERE vr.id = ?
            """,
            (int(registration_id),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_vendor_registration_by_token(db_path: str, hold_token: str) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT vr.*, svp.name package_name, svp.description package_description
            FROM vendor_registrations vr
            JOIN show_vendor_packages svp ON svp.id = vr.package_id
            WHERE vr.hold_token = ?
            """,
            ((hold_token or "").strip(),),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_vendor_registration_by_checkout(db_path: str, checkout_session_id: str) -> Optional[Dict[str, Any]]:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM vendor_registrations WHERE checkout_session_id = ?", ((checkout_session_id or "").strip(),)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def create_vendor_hold(db_path: str, show_id: int, package_id: int, form: Dict[str, Any], policy_text: str, policy_version: str) -> Dict[str, Any]:
    cleanup_expired_vendor_holds(db_path)
    availability = vendor_registration_open(db_path, show_id)
    packages = {int(p["id"]): p for p in availability["packages"]}
    package = packages.get(int(package_id))
    if not availability["open"] or not package or not package["selectable"]:
        raise ValueError(availability["reason"] or "This vendor category is no longer available.")
    hold_token = secrets.token_urlsafe(24)
    confirmation_number = f"V-{int(show_id):04d}-{secrets.token_hex(3).upper()}"
    hold_expires_at = datetime.now(timezone.utc) + timedelta(minutes=20)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        cur = conn.execute(
            """
            INSERT INTO vendor_registrations (
                show_id, package_id, hold_token, confirmation_number, status,
                business_name, contact_name, email, phone, website_url, products,
                electricity_request, special_space_request, is_food_vendor, food_details,
                insurance_ack, rules_accepted, refund_accepted, accepted_policy_version,
                accepted_policy_text, accepted_at, amount_cents, hold_expires_at
            ) VALUES (?, ?, ?, ?, 'hold', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(show_id),
                int(package_id),
                hold_token,
                confirmation_number,
                (form.get("business_name") or "").strip(),
                (form.get("contact_name") or "").strip(),
                (form.get("email") or "").strip(),
                (form.get("phone") or "").strip(),
                (form.get("website_url") or "").strip(),
                (form.get("products") or "").strip(),
                1 if form.get("electricity_request") else 0,
                (form.get("special_space_request") or "").strip(),
                1 if form.get("is_food_vendor") else 0,
                (form.get("food_details") or "").strip(),
                1 if form.get("insurance_ack") else 0,
                1 if form.get("rules_accepted") else 0,
                1 if form.get("refund_accepted") else 0,
                policy_version,
                policy_text,
                _utcnow(),
                int(package["price_cents"] or 0),
                hold_expires_at.replace(microsecond=0).isoformat(),
            ),
        )
        conn.commit()
        return get_vendor_registration(db_path, int(cur.lastrowid)) or {}
    finally:
        conn.close()


def attach_vendor_checkout(db_path: str, registration_id: int, checkout_session_id: str, payment_intent_id: str = "") -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute(
            """
            UPDATE vendor_registrations
            SET checkout_session_id = ?, stripe_payment_intent_id = ?, payment_status = 'checkout_started',
                updated_at = datetime('now')
            WHERE id = ?
            """,
            ((checkout_session_id or "").strip(), (payment_intent_id or "").strip(), int(registration_id)),
        )
        conn.commit()
    finally:
        conn.close()


def finalize_vendor_paid(db_path: str, checkout_session_id: str, payment_intent_id: str = "", amount_cents: Optional[int] = None) -> Optional[Dict[str, Any]]:
    reg = get_vendor_registration_by_checkout(db_path, checkout_session_id)
    if not reg:
        return None
    if reg["status"] == "confirmed" and reg["payment_status"] == "paid":
        return reg
    capacity_exception = 0
    availability = vendor_registration_open(db_path, int(reg["show_id"]))
    package = next((p for p in availability["packages"] if int(p["id"]) == int(reg["package_id"])), None)
    if not package or (package["remaining_count"] is not None and package["remaining_count"] < 0):
        capacity_exception = 1
    if amount_cents is not None and int(amount_cents) != int(reg["amount_cents"] or 0):
        capacity_exception = 1
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute(
            """
            UPDATE vendor_registrations
            SET status = 'confirmed', payment_status = 'paid', stripe_payment_intent_id = ?,
                capacity_exception = ?, updated_at = datetime('now')
            WHERE id = ?
            """,
            ((payment_intent_id or reg.get("stripe_payment_intent_id") or ""), capacity_exception, int(reg["id"])),
        )
        conn.commit()
        return get_vendor_registration(db_path, int(reg["id"]))
    finally:
        conn.close()


def list_vendor_registrations(db_path: str, show_id: int, query: str = "", status: str = "", package_id: str = "") -> List[Dict[str, Any]]:
    clauses = ["vr.show_id = ?"]
    args: List[Any] = [int(show_id)]
    if status:
        clauses.append("vr.status = ?")
        args.append(status)
    if package_id:
        clauses.append("vr.package_id = ?")
        args.append(int(package_id))
    if query:
        like = f"%{query}%"
        clauses.append("(vr.business_name LIKE ? OR vr.contact_name LIKE ? OR vr.email LIKE ? OR vr.phone LIKE ? OR vr.confirmation_number LIKE ? OR svp.name LIKE ?)")
        args.extend([like, like, like, like, like, like])
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            f"""
            SELECT vr.*, svp.name package_name
            FROM vendor_registrations vr
            JOIN show_vendor_packages svp ON svp.id = vr.package_id
            WHERE {' AND '.join(clauses)}
            ORDER BY vr.business_name COLLATE NOCASE, vr.contact_name COLLATE NOCASE
            """,
            args,
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def update_vendor_admin(db_path: str, registration_id: int, data: Dict[str, Any]) -> None:
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.execute(
            """
            UPDATE vendor_registrations
            SET business_name = ?, contact_name = ?, email = ?, phone = ?, website_url = ?,
                products = ?, electricity_request = ?, special_space_request = ?,
                admin_notes = ?, booth_assignment = ?, updated_at = datetime('now')
            WHERE id = ?
            """,
            (
                (data.get("business_name") or "").strip(),
                (data.get("contact_name") or "").strip(),
                (data.get("email") or "").strip(),
                (data.get("phone") or "").strip(),
                (data.get("website_url") or "").strip(),
                (data.get("products") or "").strip(),
                1 if data.get("electricity_request") else 0,
                (data.get("special_space_request") or "").strip(),
                (data.get("admin_notes") or "").strip(),
                (data.get("booth_assignment") or "").strip(),
                int(registration_id),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def set_vendor_status(db_path: str, registration_id: int, status: str, release_slot: bool = True) -> None:
    updates = {
        "checked_in": "checked_in_at = datetime('now')",
        "canceled": "status = 'canceled', canceled_at = datetime('now'), cancellation_releases_slot = ?",
    }
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        if status == "checked_in":
            conn.execute("UPDATE vendor_registrations SET checked_in_at = datetime('now'), updated_at = datetime('now') WHERE id = ?", (int(registration_id),))
        elif status == "canceled":
            conn.execute(
                "UPDATE vendor_registrations SET status = 'canceled', canceled_at = datetime('now'), cancellation_releases_slot = ?, updated_at = datetime('now') WHERE id = ?",
                (1 if release_slot else 0, int(registration_id)),
            )
        conn.commit()
    finally:
        conn.close()


def vendor_dashboard(db_path: str, show_id: int) -> Dict[str, Any]:
    availability = vendor_registration_open(db_path, show_id)
    regs = list_vendor_registrations(db_path, show_id)
    return {
        "availability": availability,
        "registrations": regs,
        "payment_incomplete": [r for r in regs if r["status"] in ("hold", "expired") or r["payment_status"] not in ("paid", "unpaid")],
        "capacity_exceptions": [r for r in regs if int(r["capacity_exception"] or 0) == 1],
        "electricity": [r for r in regs if int(r["electricity_request"] or 0) == 1 and r["status"] == "confirmed"],
        "food": [r for r in regs if int(r["is_food_vendor"] or 0) == 1 and r["status"] == "confirmed"],
        "canceled": [r for r in regs if r["status"] == "canceled"],
    }


def vendor_csv_bytes(rows: List[Dict[str, Any]]) -> bytes:
    fields = [
        "confirmation_number", "status", "business_name", "contact_name", "email", "phone",
        "package_name", "amount_cents", "payment_status", "electricity_request",
        "is_food_vendor", "special_space_request", "booth_assignment", "checked_in_at",
        "capacity_exception", "created_at",
    ]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return output.getvalue().encode("utf-8-sig")
