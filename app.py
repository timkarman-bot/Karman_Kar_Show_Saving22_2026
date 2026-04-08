# Karman Kar Shows & Events — hardened app.py  04/06/2026
# 4-space indentation only (no tabs)

from dotenv import load_dotenv
load_dotenv()

import os
import io
import csv
import hmac
import secrets
import sqlite3
import hashlib
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Dict, Optional, Any, Callable
from zoneinfo import ZoneInfo
from urllib.parse import urlencode, urlparse
from werkzeug.utils import secure_filename
from flask import send_from_directory

import stripe
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    jsonify,
    session,
    send_file,
    abort,
    flash,
)

#4/3/2026 15:06
import smtplib
from email.message import EmailMessage

from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash
from functools import wraps
from sponsorship_blueprint import sponsorship_bp
from database import (
    init_db,
    ensure_default_show,
    build_snapshot_zip_bytes,
    get_active_show,
    get_show_by_slug,
    toggle_show_voting,
    set_show_voting_open,
    update_show_admin_settings,
    set_show_charity_connect,
    clear_show_charity_connect,
    count_registered_cars,
    show_has_capacity,
    update_person,
    update_show_car_details,
    mark_show_car_checked_in,
    get_show_car_public_by_token,
    get_show_car_private_by_token,
    create_registration_intent,
    get_registration_intent_by_token,
    attach_stripe_session_to_registration_intent,
    finalize_registration_intent_paid,
    create_vote_intent,
    attach_stripe_session_to_vote_intent,
    finalize_vote_intent_paid,
    reset_votes_for_show,
    export_votes_for_show,
    leaderboard_by_category,
    leaderboard_overall,
    create_placeholder_cars,
    list_show_cars_public,
    get_show_sponsors,
    upsert_sponsor,
    attach_sponsor_to_show,
    remove_sponsor_from_show,
    set_title_sponsor,
    create_attendee,
    record_field_metric,
    create_donation_row,
    attach_stripe_session_to_donation,
    mark_donation_paid,
    waiver_mark_received,
    has_processed_webhook_event,
    mark_webhook_event_processed,
    create_waiver_evidence_record,
    log_audit_event,
    rate_limit_increment,
    list_event_interest_signups,
    create_event_interest_signup,
    list_shows_admin,
    get_next_upcoming_show,
    create_show_admin,
    update_show_admin_record,
    set_active_show,
    set_upcoming_show,
    export_event_interest_signups_csv,
    set_past_show,
    list_waiver_templates,
    get_waiver_template_by_id,
    create_waiver_template,
    update_waiver_template,
    get_effective_waiver_template_for_show,
    get_next_available_car_number,
    search_show_cars_admin,
)


from waiver_system import (
    PRESET_LABELS,
    normalize_builder_config,
    builder_config_to_json,
    build_waiver_template_from_builder,
    preview_text_from_builder,
    sample_preview_show,
    render_waiver_text,
    validate_waiver_show_fields,
    waiver_sha256,
)

from sponsorship_system import (
    get_catalog_item,
    get_salesperson,
    get_sponsorship_sale,
    get_sponsorship_sale_by_checkout_session,
    mark_sponsorship_sale_paid_by_checkout_session,
    save_sponsorship_sale,
)

APP_ENV = os.getenv("APP_ENV", os.getenv("FLASK_ENV", "production")).strip().lower()
IS_DEV = APP_ENV in {"dev", "development", "local", "test", "testing"}


def _required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    raise RuntimeError(f"Missing required environment variable: {name}")


FLASK_SECRET = os.getenv("FLASK_SECRET", "").strip() if IS_DEV else _required_env("FLASK_SECRET")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "").strip()
ADMIN_PASSWORD_HASH = os.getenv("ADMIN_PASSWORD_HASH", "").strip()
if not IS_DEV and not (ADMIN_PASSWORD or ADMIN_PASSWORD_HASH):
    raise RuntimeError("Set ADMIN_PASSWORD_HASH or ADMIN_PASSWORD in the environment.")

app = Flask(__name__)
app.register_blueprint(sponsorship_bp)
app.secret_key = FLASK_SECRET or "dev-only-local-secret"
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=not IS_DEV,
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)

BASE_URL = os.getenv("BASE_URL", "").strip()
PLATFORM_STRIPE_SECRET_KEY = (
    os.getenv("PLATFORM_STRIPE_SECRET_KEY", "").strip()
    or os.getenv("STRIPE_SECRET_KEY", "").strip()
)
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "").strip()
STRIPE_CLIENT_ID = os.getenv("STRIPE_CLIENT_ID", "").strip()

stripe.api_key = PLATFORM_STRIPE_SECRET_KEY

CATEGORY_SLUGS: Dict[str, str] = {
    "army": "Army",
    "navy": "Navy",
    "air-force": "Air Force",
    "marines": "Marines",
    "coast-guard": "Coast Guard",
    "space-force": "Space Force",
    "peoples-choice": "People’s Choice",
}

DEFAULT_SHOW = {
    "slug": "karman-charity-show",
    "title": "Karman Charity Car Show",
    "date": "Saturday, April 26, 2026",
    "time": "Cars arrive at 10:00 AM",
    "location_name": "Children’s Mercy Park",
    "address": "1 Sporting Way, Kansas City, KS 66111",
    "benefiting": "Saving22 / 22 Survivor Awareness",
    "suggested_donation": "$35 suggested donation for show cars",
    "description": (
        "A charity car show supporting veteran suicide awareness with judged certificates "
        "by branch favorites and People’s Choice."
    ),
}

DEFAULT_UPCOMING_EVENT = {
    "heading": "Upcoming show",
    "title": "Show or Pop-Up Event",
    "display_date": "April 25 or 26, 2026",
    "visible": 1,
    "intro": "Check the newsletter QR code for the latest details on our next show or pop-up event.",
    "details": "Location TBA by April 1, 2026 • Date either April 25 or 26, 2026, TBA by April 1, 2026",
    "qr_message": "Use the QR code in the newsletter to get updated information as plans are finalized.",
}

init_db()
ensure_default_show(DEFAULT_SHOW)

CONSENT_TEXT_CAR_OWNER = (
    "By submitting this form, you agree Karman Kar Shows & Events may contact you about this event and future events "
    "if selected. Msg/data rates may apply. Opt out anytime."
)
CONSENT_VERSION = "2026-03-11"
ATTENDEE_CONSENT_TEXT = (
    "By selecting these options, you agree Karman Kar Shows & Events may contact you about the event and, "
    "if selected, share sponsor offers. Msg/data rates may apply. Opt out anytime."
)
ATTENDEE_CONSENT_VERSION = "2026-03-11"


def prereg_allowed(show) -> bool:
    if not show:
        return False
    ov = show["allow_prereg_override"] if "allow_prereg_override" in show.keys() else None
    if ov is not None:
        try:
            return int(ov) == 1
        except Exception:
            pass
    st = (show["show_type"] if "show_type" in show.keys() else "full") or "full"
    return str(st).strip().lower() == "full"


def _require_platform_stripe() -> None:
    if not PLATFORM_STRIPE_SECRET_KEY:
        abort(500, "Stripe platform key is not configured. Set PLATFORM_STRIPE_SECRET_KEY.")


def _abs_url(path: str) -> str:
    if BASE_URL:
        return BASE_URL.rstrip("/") + path
    return request.url_root.rstrip("/") + path


def _parse_dollars_to_cents(value: str, default_cents: int = 0) -> int:
    try:
        return max(0, int(round(float((value or "").strip()) * 100)))
    except Exception:
        return default_cents


def _show_payment_mode(show: Any) -> str:
    if not show:
        return "stripe"
    value = (show["payment_mode"] if "payment_mode" in show.keys() else "stripe") or "stripe"
    value = str(value).strip().lower()
    return value if value in {"stripe", "external"} else "stripe"


def _show_voting_mode(show: Any) -> str:
    if not show:
        return "fundraiser_unlimited"
    value = (show["voting_mode"] if "voting_mode" in show.keys() else "fundraiser_unlimited") or "fundraiser_unlimited"
    value = str(value).strip().lower()
    return value if value in {"fundraiser_unlimited", "restricted_single"} else "fundraiser_unlimited"


def _show_max_votes_per_checkout(show: Any) -> int:
    try:
        return max(1, int(show["max_votes_per_checkout"] or 50))
    except Exception:
        return 50


def _show_preset_vote_options(show: Any) -> list[int]:
    raw = ""
    try:
        raw = (show["preset_vote_options"] or "").strip()
    except Exception:
        raw = ""
    out = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            n = int(part)
            if n > 0 and n not in out:
                out.append(n)
    return out or [1, 5, 10, 20, 25]

def _flyer_upload_dir() -> Path:
    p = Path("/data/uploads/flyers") if os.path.isdir("/data") else Path(app.instance_path) / "uploads" / "flyers"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _allowed_flyer_file(filename: str) -> bool:
    if not filename or "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in {"jpg", "jpeg", "png", "webp"}


def _save_uploaded_flyer(file_storage, slug: str) -> str:
    if not file_storage or not file_storage.filename:
        return ""

    if not _allowed_flyer_file(file_storage.filename):
        raise ValueError("Flyer must be a JPG, JPEG, PNG, or WEBP file.")

    original = secure_filename(file_storage.filename)
    ext = original.rsplit(".", 1)[1].lower()
    stamp = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y%m%d-%H%M%S")
    safe_slug = secure_filename(slug or "show") or "show"
    filename = f"{safe_slug}-{stamp}.{ext}"

    save_path = _flyer_upload_dir() / filename
    file_storage.save(save_path)

    return f"/uploads/flyers/{filename}"

def _show_allow_custom_votes(show: Any) -> bool:
    try:
        return int(show["allow_custom_votes"] or 0) == 1
    except Exception:
        return True


def _connected_account_id(show) -> Optional[str]:
    if not show:
        return None
    acct = (show["charity_stripe_account_id"] or "").strip() if "charity_stripe_account_id" in show.keys() else ""
    status = (show["charity_connect_status"] or "").strip() if "charity_connect_status" in show.keys() else ""
    return acct if acct and status == "connected" else None

##
def _require_connected_account(show) -> str:
    acct = _connected_account_id(show)
    if not acct:
        abort(500, "No charity Stripe account is connected for this show.")
    return acct


def _stripe_connect_redirect_uri() -> str:
    return _abs_url(url_for("admin_connect_charity_stripe_callback"))


def _build_connect_authorize_url(show_id: int, show_slug: str) -> str:
    if not STRIPE_CLIENT_ID:
        abort(500, "Stripe Connect client ID is not configured. Set STRIPE_CLIENT_ID.")
    state_token = secrets.token_urlsafe(24)
    session["stripe_connect_state"] = state_token
    session["stripe_connect_show_id"] = int(show_id)
    session["stripe_connect_show_slug"] = show_slug
    params = {
        "response_type": "code",
        "client_id": STRIPE_CLIENT_ID,
        "scope": "read_write",
        "state": state_token,
        "redirect_uri": _stripe_connect_redirect_uri(),
    }
    return "https://connect.stripe.com/oauth/authorize?" + urlencode(params)


def _db_path() -> str:
    path = os.getenv("DB_PATH")
    if path:
        return path
    return "/data/app.db" if os.path.isdir("/data") else "app.db"


def _conn_direct() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _waiver_dir() -> Path:
    p = Path("/data/waivers") if os.path.isdir("/data") else Path(app.instance_path) / "waivers"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _client_ip() -> str:
    forwarded = request.headers.get("X-Forwarded-For", "").split(",")[0].strip()
    return forwarded or (request.remote_addr or "")


def _user_agent() -> str:
    return (request.headers.get("User-Agent", "") or "")[:1000]


def _show_with_rendered_waiver(show: Any) -> Any:
    if not show:
        return show

    if isinstance(show, sqlite3.Row):
        show_dict = {k: show[k] for k in show.keys()}
    else:
        show_dict = dict(show)

    legacy_text = (show_dict.get("waiver_text") or "").strip()
    legacy_version = (show_dict.get("waiver_version") or "").strip()

    try:
        validation_error = validate_waiver_show_fields(show_dict)
        if validation_error:
            raise ValueError(validation_error)

        template_row = get_effective_waiver_template_for_show(int(show_dict["id"]))
        if template_row:
            rendered_text = render_waiver_text(template_row["body_template"], show_dict)
            show_dict["waiver_text"] = rendered_text
            show_dict["waiver_version"] = (template_row["version"] or "").strip()
            show_dict["waiver_template_id"] = int(template_row["id"])
            return show_dict
    except Exception:
        pass

    show_dict["waiver_text"] = legacy_text
    show_dict["waiver_version"] = legacy_version
    return show_dict


def _waiver_builder_config_from_request() -> Dict[str, Any]:
    return normalize_builder_config({
        "preset_key": request.form.get("preset_key", "standard"),
        "include_assumption_of_risk": request.form.get("include_assumption_of_risk") == "on",
        "include_release_of_liability": request.form.get("include_release_of_liability") == "on",
        "include_indemnification": request.form.get("include_indemnification") == "on",
        "include_vehicle_responsibility": request.form.get("include_vehicle_responsibility") == "on",
        "include_rules_compliance": request.form.get("include_rules_compliance") == "on",
        "include_no_custody": request.form.get("include_no_custody") == "on",
        "include_media_release": request.form.get("include_media_release") == "on",
        "include_charity_clause": request.form.get("include_charity_clause") == "on",
        "include_venue_clause": request.form.get("include_venue_clause") == "on",
        "include_right_to_remove": request.form.get("include_right_to_remove") == "on",
        "custom_clause": request.form.get("custom_clause", ""),
        "use_advanced_editor": request.form.get("use_advanced_editor") == "on",
    })


def _waiver_editor_payload(waiver: Optional[Any] = None, *, form_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    if waiver:
        if isinstance(waiver, sqlite3.Row):
            data = {k: waiver[k] for k in waiver.keys()}
        else:
            data = dict(waiver)

    if form_override:
        data.update(form_override)

    builder_config = normalize_builder_config(data.get("builder_config"))
    if form_override and form_override.get("builder_config") is not None:
        builder_config = normalize_builder_config(form_override.get("builder_config"))

    if not data.get("body_template") and not builder_config.get("use_advanced_editor"):
        data["body_template"] = build_waiver_template_from_builder(builder_config)

    data["builder_config"] = builder_config
    data["preset_label"] = PRESET_LABELS.get(builder_config.get("preset_key", "standard"), "Standard Car Show")
    data["preview_text"] = preview_text_from_builder(builder_config) if not builder_config.get("use_advanced_editor") else render_waiver_text(data.get("body_template", ""), sample_preview_show())
    return data


def _log_event(action: str, show_id: Optional[int] = None, details: Optional[Dict[str, Any]] = None, actor_type: str = "system") -> None:
    try:
        log_audit_event(
            show_id=show_id,
            actor_type=actor_type,
            action=action,
            details=details or {},
            ip_address=_client_ip(),
            user_agent=_user_agent(),
        )
    except Exception:
        pass


def _same_origin_allowed() -> bool:
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return True
    if request.endpoint == "stripe_webhook":
        return True

    origin = request.headers.get("Origin", "").strip()
    referer = request.headers.get("Referer", "").strip()
    request_host = request.host

    if origin:
        return urlparse(origin).netloc == request_host
    if referer:
        return urlparse(referer).netloc == request_host

    return True


@app.before_request
def security_before_request():
    session.permanent = True
    if not _same_origin_allowed():
        abort(400, "Blocked request origin.")
    _maybe_auto_close_voting()


def _check_admin_password(raw_password: str) -> bool:
    if ADMIN_PASSWORD_HASH:
        try:
            return check_password_hash(ADMIN_PASSWORD_HASH, raw_password or "")
        except Exception:
            return False
    if ADMIN_PASSWORD:
        return hmac.compare_digest(ADMIN_PASSWORD, raw_password or "")
    return False


def rate_limit(bucket_name: str, limit: int, window_seconds: int) -> Callable:
    def decorator(view_func: Callable) -> Callable:
        @wraps(view_func)
        def wrapped(*args, **kwargs):
            ip = _client_ip() or "unknown"
            bucket_key = f"{bucket_name}:{request.endpoint}:{ip}"
            count = rate_limit_increment(bucket_key, window_seconds)
            if count > limit:
                if request.accept_mimetypes.accept_html:
                    return render_template("payment_not_complete.html"), 429
                return jsonify({"ok": False, "error": "Too many requests. Please slow down."}), 429
            return view_func(*args, **kwargs)
        return wrapped
    return decorator


def _save_waiver_capture_html(
    *,
    show: Any,
    car_number: int,
    owner_name: str,
    phone: str,
    email: str,
    year: str,
    make: str,
    model: str,
    opt_in_future: bool,
    sponsor_opt_in: bool,
    waiver_text: str,
    waiver_version: str,
    signed_name: str,
    intent_token: str,
    request_path: str,
    ip_address: str,
    user_agent: str,
) -> str:
    now_local = datetime.now(ZoneInfo("America/Chicago"))
    now_utc = datetime.now(timezone.utc)
    ts = now_local.strftime("%Y%m%d-%H%M%S")
    safe_token = "".join(ch for ch in intent_token if ch.isalnum())[:12] or "na"
    filename = f"waiver_{show['slug']}_car-{car_number}_{ts}_{safe_token}.html"
    out_path = _waiver_dir() / filename
    waiver_hash = hashlib.sha256((waiver_text or "").encode("utf-8")).hexdigest()

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Waiver Capture - Car {car_number}</title>
<style>
body {{ font-family: Arial, Helvetica, sans-serif; line-height: 1.45; margin: 40px; color: #111827; }}
h1, h2 {{ margin-bottom: 8px; }}
.box {{ border: 1px solid #CBD5E1; border-radius: 12px; padding: 16px; margin-bottom: 18px; }}
.small {{ color: #475569; font-size: 13px; }}
pre {{ white-space: pre-wrap; font-family: Arial, Helvetica, sans-serif; }}
</style>
</head>
<body>
<h1>Electronic Waiver Capture</h1>
<div class="small">Generated {escape(now_local.isoformat())} America/Chicago / {escape(now_utc.isoformat())} UTC</div>
<div class="small">Request Path: {escape(request_path)} | IP: {escape(ip_address)} | User Agent: {escape(user_agent)}</div>
<div class="box"><h2>Show</h2>
<div><strong>Title:</strong> {escape(str(show.get('title') or ''))}</div>
<div><strong>Slug:</strong> {escape(str(show.get('slug') or ''))}</div>
<div><strong>Car Number:</strong> #{car_number}</div>
<div><strong>Vehicle:</strong> {escape(year)} {escape(make)} {escape(model)}</div>
</div>
<div class="box"><h2>Owner</h2>
<div><strong>Name:</strong> {escape(owner_name)}</div>
<div><strong>Phone:</strong> {escape(phone)}</div>
<div><strong>Email:</strong> {escape(email)}</div>
<div><strong>Future Show Updates:</strong> {'Yes' if opt_in_future else 'No'}</div>
<div><strong>Sponsor Information:</strong> {'Yes' if sponsor_opt_in else 'No'}</div>
</div>
<div class="box"><h2>Waiver</h2>
<div><strong>Waiver Version:</strong> {escape(waiver_version)}</div>
<div><strong>Waiver SHA-256:</strong> {escape(waiver_hash)}</div>
<pre>{escape(waiver_text)}</pre>
</div>
<div class="box"><h2>Signature</h2>
<div><strong>Typed Signature:</strong> {escape(signed_name)}</div>
<div><strong>Intent Token:</strong> {escape(intent_token)}</div>
</div>
</body>
</html>
"""
    out_path.write_text(html_doc, encoding="utf-8")
    return str(out_path)


def _record_waiver_evidence(
    *,
    show: Any,
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
    waiver_text: str,
    waiver_version: str,
    signed_name: str,
    intent_token: str,
    html_path: str,
) -> None:
    now_local = datetime.now(ZoneInfo("America/Chicago")).isoformat()
    now_utc = datetime.now(timezone.utc).isoformat()
    create_waiver_evidence_record(
        show_id=int(show["id"]),
        registration_intent_id=registration_intent_id,
        show_car_id=show_car_id,
        car_number=car_number,
        owner_name=owner_name,
        phone=phone,
        email=email,
        year=year,
        make=make,
        model=model,
        opt_in_future=opt_in_future,
        sponsor_opt_in=sponsor_opt_in,
        waiver_version=waiver_version,
        waiver_text=waiver_text,
        signed_name=signed_name,
        waiver_accepted=True,
        intent_token=intent_token,
        html_path=html_path,
        request_path=request.path,
        ip_address=_client_ip(),
        user_agent=_user_agent(),
        created_at_utc=now_utc,
        created_at_local=now_local,
    )


def _finalize_placeholder_claim_paid(*, stripe_session_id: str, show_car_id: int) -> Dict[str, Any]:
    conn = _conn_direct()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        ri = cur.execute("SELECT * FROM registration_intents WHERE stripe_session_id = ? LIMIT 1", (stripe_session_id,)).fetchone()
        if not ri:
            raise ValueError("Registration intent not found.")
        if ri["finalized_show_car_id"]:
            sc = cur.execute("SELECT * FROM show_cars WHERE id = ? LIMIT 1", (int(ri["finalized_show_car_id"]),)).fetchone()
            conn.commit()
            return {"show_car_id": int(ri["finalized_show_car_id"]), "car_token": sc["car_token"] if sc else None, "already_finalized": True}

        sc = cur.execute("SELECT * FROM show_cars WHERE id = ? LIMIT 1", (show_car_id,)).fetchone()
        if not sc:
            raise ValueError("Placeholder car not found.")
        person_id = int(sc["person_id"])

        cur.execute(
            """
            UPDATE people
            SET name = ?, phone = ?, email = ?, opt_in_future = ?, sponsor_opt_in = ?, consent_text = ?, consent_version = ?
            WHERE id = ?
            """,
            (
                ri["owner_name"],
                ri["phone"],
                ri["email"],
                int(ri["opt_in_future"] or 0),
                int(ri["sponsor_opt_in"] or 0),
                CONSENT_TEXT_CAR_OWNER,
                CONSENT_VERSION,
                person_id,
            ),
        )

        cur.execute(
            """
            UPDATE show_cars
            SET year = ?,
                make = ?,
                model = ?,
                registration_payment_status = 'paid',
                registration_amount_cents = ?,
                registration_session_id = ?,
                waiver_signed_name = ?,
                waiver_signed_at = datetime('now'),
                waiver_version = ?,
                waiver_received = 1,
                waiver_received_at = datetime('now'),
                waiver_received_by = 'electronic',
                is_placeholder = 0,
                registration_state = 'paid'
            WHERE id = ?
            """,
            (
                ri["year"],
                ri["make"],
                ri["model"],
                int(ri["amount_cents"] or 0),
                stripe_session_id,
                ri["waiver_signed_name"],
                ri["waiver_version"],
                show_car_id,
            ),
        )

        cur.execute(
            "UPDATE registration_intents SET payment_status = 'paid', paid_at = datetime('now'), finalized_show_car_id = ? WHERE id = ?",
            (show_car_id, int(ri["id"])),
        )
        conn.commit()
        return {"show_car_id": show_car_id, "car_token": sc["car_token"], "already_finalized": False}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def require_admin(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("admin_authed"):
            return redirect(url_for("admin_page", next=request.path))
        return view_func(*args, **kwargs)
    return wrapped


def _maybe_auto_close_voting() -> None:
    end_raw = os.getenv("VOTING_END", "").strip()
    if not end_raw:
        return
    show = get_active_show()
    if not show or int(show["voting_open"]) != 1:
        return
    try:
        end_dt = datetime.strptime(end_raw, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo("America/Chicago"))
        if datetime.now(ZoneInfo("America/Chicago")) >= end_dt:
            set_show_voting_open(int(show["id"]), False)
    except Exception:
        return

def _save_sponsor_logo_upload(file_storage) -> str:
    if not file_storage or not getattr(file_storage, "filename", ""):
        return ""
    filename = secure_filename(file_storage.filename)
    if not filename:
        return ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in {"png", "jpg", "jpeg", "webp", "svg"}:
        return ""
    upload_dir = Path(app.static_folder) / "img" / "sponsors" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y%m%d-%H%M%S")
    final_name = f"sponsor-{stamp}-{filename}"
    out_path = upload_dir / final_name
    file_storage.save(out_path)
    return f"img/sponsors/uploads/{final_name}"

def _send_system_email(*, subject: str, body: str, reply_to: str = "") -> bool:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587").strip() or "587")
    smtp_username = os.getenv("SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    smtp_use_tls = os.getenv("SMTP_USE_TLS", "1").strip() != "0"
    smtp_from = os.getenv("SMTP_FROM_EMAIL", "info@karmankarshowsandevents.com").strip()
    target = "info@karmankarshowsandevents.com"

    if not smtp_host or not smtp_username or not smtp_password:
        app.logger.warning("SMTP not configured; email not sent. Subject=%s", subject)
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = target
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            if smtp_use_tls:
                server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        return True
    except Exception:
        app.logger.exception("Failed to send system email.")
        return False


@app.context_processor
def inject_globals():
    show = get_active_show()
    title_sponsor, sponsors = (None, [])
    registered_cars = 0
    if show:
        title_sponsor, sponsors = get_show_sponsors(int(show["id"])) or (None, [])
        registered_cars = count_registered_cars(int(show["id"]))
    return {
        "active_show": show,
        "CATEGORY_SLUGS": CATEGORY_SLUGS,
        "CATEGORY_NAMES": list(CATEGORY_SLUGS.values()),
        "title_sponsor": title_sponsor,
        "sponsors": sponsors,
        "is_admin": session.get("admin_authed", False),
        "prereg_allowed": prereg_allowed,
        "registered_cars": registered_cars,
    }


@app.get("/")
def home():
    show = get_active_show()
    if not show:
        return "No active show configured.", 500
    return render_template("home.html", show=show)

@app.get("/uploads/flyers/<path:filename>")
def uploaded_flyer(filename: str):
    return send_from_directory(_flyer_upload_dir(), filename)

@app.get("/instructions/<show_slug>")
def voting_instructions(show_slug: str):
    show = _show_with_rendered_waiver(get_show_by_slug(show_slug))
    if not show:
        return "Show not found.", 404
    return render_template("voting_instructions.html", show=show)

@app.get("/events")
def events():
    upcoming_show = get_next_upcoming_show()
    if not upcoming_show:
        upcoming_show = get_active_show()

    return render_template(
        "events.html",
        show=get_active_show(),
        upcoming_show=upcoming_show,
    )
    
@app.get("/contact")
def contact_page():
    return render_template("contact.html")

@app.post("/contact")
@rate_limit("contact_submit", 10, 300)
def contact_submit():
    name = request.form.get("name", "").strip()
    email = request.form.get("email", "").strip()
    subject = request.form.get("subject", "").strip()
    body = request.form.get("body", "").strip()

    if not (name and email and subject and body):
        flash("Please complete all contact form fields.", "error")
        return redirect(url_for("contact_page"))

    sent = _send_system_email(
        subject=f"Customer Service: {subject}",
        body=f"From: {name}\nEmail: {email}\n\n{body}",
        reply_to=email,
    )

    if sent:
        flash("Your message has been sent.", "ok")
    else:
        flash("Your message was saved, but email delivery is not configured yet.", "error")
    return redirect(url_for("contact_page"))


@app.get("/privacy")
def privacy_policy():
    return render_template("privacy.html", current_year=datetime.now().year)
        
@app.get("/terms")
def terms_page():
    return render_template("terms.html", current_year=datetime.now().year)

@app.get("/refund-policy")
def refund_policy_page():
    return render_template("refund_policy.html", current_year=datetime.now().year)

@app.get("/support")
def support_page():
    return render_template("support.html", current_year=datetime.now().year)

@app.get("/voting-disclosure")
def voting_disclosure_page():
    return render_template("voting_disclosure.html", current_year=datetime.now().year)

@app.get("/sponsor-agreement")
def sponsor_agreement_page():
    return render_template("sponsor_agreement.html", current_year=datetime.now().year)        
        
@app.post("/event-updates-signup")
@rate_limit("event_updates_signup", 20, 300)
def event_updates_signup():
    upcoming_show = get_next_upcoming_show()

    first_name = request.form.get("first_name", "").strip()
    last_name = request.form.get("last_name", "").strip()
    email = request.form.get("email", "").strip().lower()
    phone = request.form.get("phone", "").strip()
    wants_email = 1 if request.form.get("wants_email") else 0
    wants_text = 1 if request.form.get("wants_text") else 0
    source = request.form.get("source", "").strip() or "website"

    if not first_name:
        flash("First name is required.", "error")
        return redirect(url_for("events"))

    if not email and not phone:
        flash("Please provide an email address, a mobile phone number, or both.", "error")
        return redirect(url_for("events"))

    if wants_email and not email:
        flash("Email is required if you want email updates.", "error")
        return redirect(url_for("events"))

    if wants_text and not phone:
        flash("Mobile phone is required if you want text updates.", "error")
        return redirect(url_for("events"))

    create_event_interest_signup(
        show_id=int(upcoming_show["id"]) if upcoming_show else None,
        first_name=first_name,
        last_name=last_name,
        email=email,
        phone=phone,
        wants_email=wants_email,
        wants_text=wants_text,
        source=source,
    )

    flash("You're on the list. Updates and reminders coming soon.", "ok")
    return redirect(url_for("events"))


@app.get("/show/<slug>")
def show_page(slug: str):
    show = get_show_by_slug(slug)

    if not show and slug == "karman-charity-show":
        active_show = get_active_show()
        if active_show and active_show["slug"] != slug:
            return redirect(url_for("show_page", slug=active_show["slug"]), code=302)

    if not show:
        return render_template("show.html", show={"title": "Show Not Found"}, not_found=True)

    return render_template(
        "show.html",
        show=show,
        not_found=False,
    )

@app.get("/register")
def register_page():
    show = _show_with_rendered_waiver(get_active_show())
    if not show:
        return "No active show configured.", 500
    if not prereg_allowed(show):
        return render_template("registration_closed.html", show=show), 403
    return render_template("register.html", show=show)
    
@app.post("/register")
@rate_limit("register", 20, 300)
def register_submit():
    show = _show_with_rendered_waiver(get_active_show())
    if not show:
        return "No active show configured.", 500
    if not prereg_allowed(show):
        return render_template("registration_closed.html", show=show), 403
    if not show_has_capacity(int(show["id"])):
        return render_template("register.html", show=show, error="This show has reached its maximum number of cars.")

    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip().lower()
    opt_in_future = request.form.get("opt_in_future", "") == "on"
    sponsor_opt_in = request.form.get("sponsor_opt_in", "") == "on"
    year = request.form.get("year", "").strip()
    make = request.form.get("make", "").strip()
    model = request.form.get("model", "").strip()
    waiver_accepted = request.form.get("waiver_accepted", "") == "on"
    waiver_signed_name = request.form.get("waiver_signed_name", "").strip()

    if not (name and year and make and model and waiver_signed_name):
        return render_template("register.html", show=show, error="Please fill out all required fields.")
    if (opt_in_future or sponsor_opt_in) and not phone:
        return render_template("register.html", show=show, error="Phone number is required if you opt in to updates or sponsor information.")
    if not waiver_accepted:
        return render_template("register.html", show=show, error="You must accept the waiver to continue.")

    registration_fee_cents = int(show["registration_fee_cents"] or 0)
    waiver_text = (show.get("waiver_text") or "").strip()
    waiver_version = (show.get("waiver_version") or "").strip()
    waiver_template_id = int(show["waiver_template_id"]) if show.get("waiver_template_id") else None

    try:
        registration_intent_id, intent_token, assigned_car_number = create_registration_intent(
            show_id=int(show["id"]),
            owner_name=name,
            phone=phone,
            email=email,
            opt_in_future=opt_in_future,
            sponsor_opt_in=sponsor_opt_in,
            year=year,
            make=make,
            model=model,
            waiver_accepted=waiver_accepted,
            waiver_signed_name=waiver_signed_name,
            waiver_text=waiver_text,
            waiver_version=waiver_version,
            amount_cents=registration_fee_cents,
            waiver_template_id=waiver_template_id,
        )
    except ValueError as e:
        return render_template("register.html", show=show, error=str(e))

    html_path = _save_waiver_capture_html(
        show=show,
        car_number=assigned_car_number,
        owner_name=name,
        phone=phone,
        email=email,
        year=year,
        make=make,
        model=model,
        opt_in_future=opt_in_future,
        sponsor_opt_in=sponsor_opt_in,
        waiver_text=waiver_text,
        waiver_version=waiver_version,
        signed_name=waiver_signed_name,
        intent_token=intent_token,
        request_path=request.path,
        ip_address=_client_ip(),
        user_agent=_user_agent(),
    )
    _record_waiver_evidence(
        show=show,
        registration_intent_id=registration_intent_id,
        show_car_id=None,
        car_number=assigned_car_number,
        owner_name=name,
        phone=phone,
        email=email,
        year=year,
        make=make,
        model=model,
        opt_in_future=opt_in_future,
        sponsor_opt_in=sponsor_opt_in,
        waiver_text=waiver_text,
        waiver_version=waiver_version,
        signed_name=waiver_signed_name,
        intent_token=intent_token,
        html_path=html_path,
    )

    if registration_fee_cents <= 0:
        synthetic_session_id = f"free_reg_{intent_token}"
        attach_stripe_session_to_registration_intent(
            registration_intent_id,
            synthetic_session_id,
            stripe_payment_intent_id="",
        )
        result = finalize_registration_intent_paid(synthetic_session_id)
        car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
        _log_event(
            "registration.free_finalized",
            int(show["id"]),
            {"car_number": assigned_car_number, "registration_intent_id": registration_intent_id},
            actor_type="public",
        )
        return render_template("register_success.html", show=show, car=car)

    _require_platform_stripe()
    success_url = _abs_url(url_for("registration_success", show_slug=show["slug"], intent_token=intent_token)) + "?session_id={CHECKOUT_SESSION_ID}"
    cancel_url = _abs_url(url_for("register_page"))

    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "unit_amount": registration_fee_cents,
                "product_data": {"name": f"Registration – {show['title']} – Car #{assigned_car_number}"},
            },
            "quantity": 1,
        }],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "payment_item_type": "registration",
            "show_id": str(show["id"]),
            "show_slug": show["slug"],
            "registration_intent_id": str(registration_intent_id),
            "intent_token": intent_token,
        },
    )
    attach_stripe_session_to_registration_intent(
        registration_intent_id,
        session_obj.id,
        stripe_payment_intent_id="",
    )
    return render_template(
        "register_checkout.html",
        show=show,
        car={"year": year, "make": make, "model": model},
        car_number=assigned_car_number,
        checkout_url=session_obj.url,
    )
        
@app.get("/register-success/<show_slug>/<intent_token>")
def registration_success(show_slug: str, intent_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    ri = get_registration_intent_by_token(intent_token)
    if not ri or int(ri["show_id"]) != int(show["id"]):
        return "Registration not found.", 404

    session_id = request.args.get("session_id", "").strip()
    if ri["finalized_show_car_id"]:
        result = finalize_registration_intent_paid(ri["stripe_session_id"])
        car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
        return render_template("register_success.html", show=show, car=car)

    if not session_id:
        return render_template("payment_not_complete.html")

    _require_platform_stripe()
    try:
        sess = stripe.checkout.Session.retrieve(session_id)
    except Exception:
        return render_template("payment_not_complete.html")

    if sess.payment_status != "paid":
        return render_template("payment_not_complete.html")

    result = finalize_registration_intent_paid(sess.id)
    car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
    return render_template("register_success.html", show=show, car=car)

@app.get("/claim-success/<show_slug>/<intent_token>")
def placeholder_claim_success(show_slug: str, intent_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404

    ri = get_registration_intent_by_token(intent_token)
    if not ri or int(ri["show_id"]) != int(show["id"]):
        return "Registration not found.", 404

    session_id = request.args.get("session_id", "").strip()
    if ri["finalized_show_car_id"]:
        conn = _conn_direct()
        try:
            sc = conn.execute("SELECT * FROM show_cars WHERE id = ? LIMIT 1", (int(ri["finalized_show_car_id"]),)).fetchone()
        finally:
            conn.close()
        if not sc:
            return render_template("payment_not_complete.html")
        car = get_show_car_public_by_token(int(show["id"]), sc["car_token"])
        return render_template("placeholder_claim_success.html", show=show, car=car)

    if not session_id:
        return render_template("payment_not_complete.html")

    _require_platform_stripe()
    try:
        sess = stripe.checkout.Session.retrieve(session_id)
    except Exception:
        return render_template("payment_not_complete.html")

    if sess.payment_status != "paid":
        return render_template("payment_not_complete.html")

    md = sess.metadata or {}
    show_car_id = int(md.get("show_car_id", "0") or "0")
    if not show_car_id:
        return render_template("payment_not_complete.html")

    result = _finalize_placeholder_claim_paid(stripe_session_id=sess.id, show_car_id=show_car_id)
    car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
    return render_template("placeholder_claim_success.html", show=show, car=car)

@app.post("/claim/<show_slug>/<car_token>")
@rate_limit("claim", 20, 300)
def placeholder_claim_submit(show_slug: str, car_token: str):
    show = _show_with_rendered_waiver(get_show_by_slug(show_slug))
    if not show:
        return "Show not found.", 404
    car = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404

    owner_name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip().lower()
    opt_in_future = request.form.get("opt_in_future", "") == "on"
    sponsor_opt_in = request.form.get("sponsor_opt_in", "") == "on"
    year = request.form.get("year", "").strip()
    make = request.form.get("make", "").strip()
    model = request.form.get("model", "").strip()
    waiver_accepted = request.form.get("waiver_accepted", "") == "on"
    waiver_signed_name = request.form.get("waiver_signed_name", "").strip()

    if not (owner_name and year and make and model and waiver_signed_name):
        return render_template("placeholder_claim.html", show=show, car=car, error="Please fill out all required fields.")
    if (opt_in_future or sponsor_opt_in) and not phone:
        return render_template("placeholder_claim.html", show=show, car=car, error="Phone number is required if you opt in to updates or sponsor information.")
    if not waiver_accepted:
        return render_template("placeholder_claim.html", show=show, car=car, error="You must accept the waiver to continue.")

    car_number = int(car["car_number"])
    registration_fee_cents = int(show["registration_fee_cents"] or 0)
    waiver_text = (show.get("waiver_text") or "").strip()
    waiver_version = (show.get("waiver_version") or "").strip()
    waiver_template_id = int(show["waiver_template_id"]) if show.get("waiver_template_id") else None

    try:
        registration_intent_id, intent_token, assigned_car_number = create_registration_intent(
            show_id=int(show["id"]),
            owner_name=owner_name,
            phone=phone,
            email=email,
            opt_in_future=opt_in_future,
            sponsor_opt_in=sponsor_opt_in,
            year=year,
            make=make,
            model=model,
            waiver_accepted=True,
            waiver_signed_name=waiver_signed_name,
            waiver_text=waiver_text,
            waiver_version=waiver_version,
            amount_cents=registration_fee_cents,
            waiver_template_id=waiver_template_id,
            reserved_car_number=car_number,
        )
    except ValueError:
        conn = _conn_direct()
        cur = conn.cursor()
        try:
            intent_token = secrets.token_urlsafe(18)
            assigned_car_number = car_number
            cur.execute(
                """
                INSERT INTO registration_intents (
                    show_id, intent_token, owner_name, phone, email, opt_in_future, sponsor_opt_in,
                    car_number, year, make, model,
                    waiver_accepted, waiver_signed_name, waiver_text, waiver_version, waiver_text_sha256,
                    waiver_template_id, amount_cents, payment_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    int(show["id"]),
                    intent_token,
                    owner_name,
                    phone,
                    email,
                    1 if opt_in_future else 0,
                    1 if sponsor_opt_in else 0,
                    car_number,
                    year,
                    make,
                    model,
                    1,
                    waiver_signed_name,
                    waiver_text,
                    waiver_version,
                    hashlib.sha256(waiver_text.encode("utf-8")).hexdigest(),
                    waiver_template_id,
                    registration_fee_cents,
                ),
            )
            conn.commit()
            registration_intent_id = int(cur.lastrowid)
        finally:
            conn.close()

    html_path = _save_waiver_capture_html(
        show=show,
        car_number=assigned_car_number,
        owner_name=owner_name,
        phone=phone,
        email=email,
        year=year,
        make=make,
        model=model,
        opt_in_future=opt_in_future,
        sponsor_opt_in=sponsor_opt_in,
        waiver_text=waiver_text,
        waiver_version=waiver_version,
        signed_name=waiver_signed_name,
        intent_token=intent_token,
        request_path=request.path,
        ip_address=_client_ip(),
        user_agent=_user_agent(),
    )
    _record_waiver_evidence(
        show=show,
        registration_intent_id=registration_intent_id,
        show_car_id=int(car["id"]),
        car_number=assigned_car_number,
        owner_name=owner_name,
        phone=phone,
        email=email,
        year=year,
        make=make,
        model=model,
        opt_in_future=opt_in_future,
        sponsor_opt_in=sponsor_opt_in,
        waiver_text=waiver_text,
        waiver_version=waiver_version,
        signed_name=waiver_signed_name,
        intent_token=intent_token,
        html_path=html_path,
    )

    if registration_fee_cents <= 0:
        synthetic_session_id = f"free_claim_{intent_token}"
        attach_stripe_session_to_registration_intent(
            registration_intent_id,
            synthetic_session_id,
            stripe_payment_intent_id="",
        )
        result = _finalize_placeholder_claim_paid(
            stripe_session_id=synthetic_session_id,
            show_car_id=int(car["id"]),
        )
        final_car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
        return render_template("placeholder_claim_success.html", show=show, car=final_car)

    _require_platform_stripe()
    success_url = _abs_url(url_for("placeholder_claim_success", show_slug=show["slug"], intent_token=intent_token)) + "?session_id={CHECKOUT_SESSION_ID}"
    cancel_url = _abs_url(url_for("placeholder_claim_page", show_slug=show["slug"], car_token=car_token))
    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "unit_amount": registration_fee_cents,
                "product_data": {"name": f"Registration – {show['title']} – Car #{assigned_car_number}"},
            },
            "quantity": 1,
        }],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "payment_item_type": "placeholder_claim",
            "show_id": str(show["id"]),
            "show_slug": show["slug"],
            "registration_intent_id": str(registration_intent_id),
            "intent_token": intent_token,
            "show_car_id": str(car["id"]),
        },
    )
    attach_stripe_session_to_registration_intent(
        registration_intent_id,
        session_obj.id,
        stripe_payment_intent_id="",
    )
    return render_template(
        "register_checkout.html",
        show=show,
        car={"year": year, "make": make, "model": model},
        car_number=assigned_car_number,
        checkout_url=session_obj.url,
    )

@app.get("/r/<show_slug>/<car_token>")
def registration_complete(show_slug: str, car_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    car = get_show_car_public_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404
    return render_template("registration_complete.html", show=show, car=car)


@app.get("/car-card/<slug>/<token>", endpoint="car_card")
def car_card(slug: str, token: str):
    show = get_show_by_slug(slug)
    if not show:
        return "Show not found.", 404
    car = get_show_car_public_by_token(int(show["id"]), token)
    if not car:
        return "Car not found.", 404
    return render_template("registration_complete.html", show=show, car=car)


@app.get("/checkin/<show_slug>/<car_token>")
def checkin_page(show_slug: str, car_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    car_private = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car_private:
        return "Car not found.", 404
    return render_template("checkin.html", show=show, car=car_private)


@app.post("/checkin/<show_slug>/<car_token>")
@rate_limit("checkin", 30, 300)
def checkin_submit(show_slug: str, car_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    car_private = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car_private:
        return "Car not found.", 404

    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip().lower()
    opt_in_future = request.form.get("opt_in_future", "") == "on"
    year = request.form.get("year", "").strip()
    make = request.form.get("make", "").strip()
    model = request.form.get("model", "").strip()

    if not (name and phone and email and year and make and model):
        return render_template("checkin.html", show=show, car=car_private, error="Please fill out all required fields.")

    update_person(
        person_id=int(car_private["person_id"]),
        name=name,
        phone=phone,
        email=email,
        opt_in_future=opt_in_future,
        sponsor_opt_in=bool(car_private["sponsor_opt_in"]) if "sponsor_opt_in" in car_private.keys() else False,
        consent_text=car_private["consent_text"] if "consent_text" in car_private.keys() else CONSENT_TEXT_CAR_OWNER,
        consent_version=car_private["consent_version"] if "consent_version" in car_private.keys() else CONSENT_VERSION,
    )
    update_show_car_details(int(car_private["id"]), year=year, make=make, model=model)
    mark_show_car_checked_in(int(car_private["id"]))
    _log_event(
        "checkin.completed",
        int(show["id"]),
        {"show_car_id": int(car_private["id"]), "car_number": int(car_private["car_number"])},
        actor_type="public",
    )
    car_private2 = get_show_car_private_by_token(int(show["id"]), car_token)
    return render_template("checkin.html", show=show, car=car_private2, success="Check-in complete. You're all set!")


@app.get("/waiver/<show_slug>/<car_token>")
def waiver_print(show_slug: str, car_token: str):
    show = _show_with_rendered_waiver(get_show_by_slug(show_slug))
    if not show:
        return "Show not found.", 404
    car = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404

    waiver_text = (car["waiver_text"] or "").strip() if "waiver_text" in car.keys() else ""
    waiver_version = (car["waiver_version"] or "").strip() if "waiver_version" in car.keys() else ""
    if not waiver_text:
        waiver_text = (show.get("waiver_text") or "").strip()
    if not waiver_version:
        waiver_version = (show.get("waiver_version") or "").strip()

    return render_template("waiver_print.html", show=show, car=car, waiver_text=waiver_text, waiver_version=waiver_version)


@app.get("/attend/<show_slug>")
def attendee_page(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    return render_template("attendee.html", show=show)


@app.post("/attend/<show_slug>")
@rate_limit("attendee", 30, 300)
def attendee_submit(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404

    first_name = request.form.get("first_name", "").strip()
    last_name = request.form.get("last_name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip().lower()
    zip_code = request.form.get("zip", "").strip()
    sponsor_opt_in = request.form.get("sponsor_opt_in", "") == "on"
    updates_opt_in = request.form.get("updates_opt_in", "") == "on"

    if not (first_name and last_name):
        return render_template("attendee.html", show=show, error="First and last name are required.")
    if (sponsor_opt_in or updates_opt_in) and not phone:
        return render_template("attendee.html", show=show, error="Phone number is required if you choose to receive updates or sponsor information.")

    attendee_id = create_attendee(
        int(show["id"]),
        first_name,
        last_name,
        phone,
        email,
        zip_code,
        sponsor_opt_in,
        updates_opt_in,
        ATTENDEE_CONSENT_TEXT,
        ATTENDEE_CONSENT_VERSION,
    )
    record_field_metric(int(show["id"]), "phone", bool(phone))
    record_field_metric(int(show["id"]), "email", bool(email))
    return redirect(url_for("attendee_fee_page", show_slug=show_slug, attendee_id=attendee_id))


@app.get("/attend/<show_slug>/fee/<int:attendee_id>")
def attendee_fee_page(show_slug: str, attendee_id: int):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    return render_template("attendee_fee.html", show=show, attendee_id=attendee_id)


@app.get("/attend/<show_slug>/donate/<int:attendee_id>")
def attendee_donate_page(show_slug: str, attendee_id: int):
    return redirect(url_for("attendee_fee_page", show_slug=show_slug, attendee_id=attendee_id))


@app.post("/attend/create-fee-checkout")
@app.post("/attend/create-donation-checkout")
@rate_limit("attendee_checkout", 20, 300)
def create_attendee_fee_checkout():
    show_slug = request.form.get("show_slug", "").strip()
    attendee_id_raw = request.form.get("attendee_id", "").strip()
    show = get_show_by_slug(show_slug)
    if not show:
        return jsonify({"ok": False, "error": "Show not found."}), 404

    try:
        attendee_id = int(attendee_id_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid attendee."}), 400

    fixed_fee_cents = int(show["attendee_fee_cents"] or 0)
    skip_fee = request.form.get("skip_fee", "").strip() == "1"
    if skip_fee or fixed_fee_cents <= 0:
        create_donation_row(int(show["id"]), attendee_id, 0, "skipped")
        return jsonify({"ok": True, "skipped": True, "redirect_url": url_for("attendee_done", show_slug=show_slug)})

    _require_platform_stripe()
    fee_row_id = create_donation_row(int(show["id"]), attendee_id, fixed_fee_cents, "pending")
    success_url = _abs_url(url_for("attendee_fee_success", show_slug=show_slug)) + "?session_id={CHECKOUT_SESSION_ID}"
    cancel_url = _abs_url(url_for("attendee_fee_page", show_slug=show_slug, attendee_id=attendee_id))
    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "unit_amount": fixed_fee_cents,
                "product_data": {"name": f"Attendance Fee – {show['title']}"},
            },
            "quantity": 1,
        }],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "payment_item_type": "attendance_fee",
            "show_id": str(show["id"]),
            "show_slug": show_slug,
            "donation_id": str(fee_row_id),
        },
    )
    attach_stripe_session_to_donation(fee_row_id, session_obj.id, stripe_payment_intent_id="")
    return jsonify({"ok": True, "checkout_url": session_obj.url})
    
@app.get("/attend/<show_slug>/fee-success")
@app.get("/donation-success")
def attendee_fee_success(show_slug: Optional[str] = None):
    session_id = request.args.get("session_id", "").strip()
    if not session_id:
        return "Missing session_id.", 400
    if not show_slug:
        show_slug = request.args.get("show_slug", "").strip()

    show = get_show_by_slug(show_slug) if show_slug else get_active_show()
    if not show:
        return "Show not found.", 404

    _require_platform_stripe()
    try:
        sess = stripe.checkout.Session.retrieve(session_id)
    except Exception:
        return render_template("payment_not_complete.html")

    if sess.payment_status != "paid":
        return render_template("payment_not_complete.html")

    mark_donation_paid(sess.id)
    return redirect(url_for("attendee_done", show_slug=show["slug"]))

@app.get("/attend/<show_slug>/done")
def attendee_done(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    return render_template("attendee_done.html", show=show)


@app.get("/v/<show_slug>/<car_token>/<category_slug>")
def vote_qty_page(show_slug: str, car_token: str, category_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    if category_slug not in CATEGORY_SLUGS:
        return "Invalid category.", 404

    car = get_show_car_public_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404

    if int(show["voting_open"]) != 1:
        return render_template("voting_closed.html", show=show)

    return render_template(
        "vote_qty.html",
        show=show,
        car=car,
        category_slug=category_slug,
        category_name=CATEGORY_SLUGS[category_slug],
        vote_price_cents=int(show["vote_price_cents"] or 100),
        payment_mode=_show_payment_mode(show),
        voting_mode=_show_voting_mode(show),
        preset_vote_options=_show_preset_vote_options(show),
        allow_custom_votes=_show_allow_custom_votes(show),
        max_votes_per_checkout=_show_max_votes_per_checkout(show),
    )


@app.post("/create-checkout-session")
@rate_limit("vote_checkout", 25, 300)
def create_checkout_session():
    show_slug = request.form.get("show_slug", "").strip()
    car_token = request.form.get("car_token", "").strip()
    category_slug = request.form.get("category_slug", "").strip()
    qty_raw = request.form.get("vote_qty", "1").strip()

    show = get_show_by_slug(show_slug)
    if not show:
        return jsonify({"ok": False, "error": "Show not found."}), 404

    if int(show["voting_open"]) != 1:
        return jsonify({"ok": False, "error": "Voting is currently closed."}), 403

    if category_slug not in CATEGORY_SLUGS:
        return jsonify({"ok": False, "error": "Invalid category."}), 400

    car = get_show_car_public_by_token(int(show["id"]), car_token)
    if not car:
        return jsonify({"ok": False, "error": "Car not found."}), 404

    try:
        vote_qty = int(qty_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid vote quantity."}), 400

    max_votes = _show_max_votes_per_checkout(show)
    if vote_qty < 1 or vote_qty > max_votes:
        return jsonify({"ok": False, "error": f"Vote quantity must be between 1 and {max_votes}."}), 400

    vote_price_cents = int(show["vote_price_cents"] or 100)
    amount_cents = vote_qty * vote_price_cents
    payment_mode = _show_payment_mode(show)

    if payment_mode == "external":
        external_payment_url = (show["external_payment_url"] or "").strip() if "external_payment_url" in show.keys() else ""
        if not external_payment_url:
            return jsonify({"ok": False, "error": "External payment URL is not configured for this show."}), 400

        return jsonify({
            "ok": True,
            "payment_mode": "external",
            "checkout_url": external_payment_url,
        })

    acct = _connected_account_id(show)
    if not acct:
        return jsonify({"ok": False, "error": "The charity payment account is not connected for this show."}), 400

    _require_platform_stripe()
    vote_intent_id = create_vote_intent(
        int(show["id"]),
        int(car["id"]),
        CATEGORY_SLUGS[category_slug],
        vote_qty,
        amount_cents,
    )

    success_url = _abs_url(url_for("vote_success")) + "?session_id={CHECKOUT_SESSION_ID}&show_slug=" + show_slug
    cancel_url = _abs_url(url_for("vote_qty_page", show_slug=show_slug, car_token=car_token, category_slug=category_slug))

    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "unit_amount": vote_price_cents,
                "product_data": {
                    "name": f"Vote – {CATEGORY_SLUGS[category_slug]} (Car #{car['car_number']})"
                },
            },
            "quantity": vote_qty,
        }],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "payment_item_type": "vote",
            "show_id": str(show["id"]),
            "show_slug": show_slug,
            "vote_intent_id": str(vote_intent_id),
            "show_car_id": str(car["id"]),
            "category": CATEGORY_SLUGS[category_slug],
            "vote_qty": str(vote_qty),
        },
#        stripe_account=acct,
    )

    attach_stripe_session_to_vote_intent(vote_intent_id, session_obj.id, stripe_payment_intent_id="")
    return jsonify({
        "ok": True,
        "payment_mode": "stripe",
        "checkout_url": session_obj.url,
    })


@app.get("/success")
def vote_success():
    session_id = request.args.get("session_id", "").strip()
    show_slug = request.args.get("show_slug", "").strip()
    if not session_id:
        return "Missing session_id.", 400

    show = get_show_by_slug(show_slug) if show_slug else get_active_show()
    if not show:
        return "Show not found.", 404

    acct = _connected_account_id(show)
    if not acct:
        return render_template("payment_not_complete.html")

    _require_platform_stripe()
    try:
        sess = stripe.checkout.Session.retrieve(session_id, stripe_account=acct)
    except Exception:
        return render_template("payment_not_complete.html")

    if sess.payment_status != "paid":
        return render_template("payment_not_complete.html")

    finalize_vote_intent_paid(sess.id)
    return render_template("vote_success.html", show=show)

@app.post("/sponsorship/submit")
@rate_limit("sponsorship_submit", 20, 300)
def sponsorship_public_submit():
    show_slug = request.form.get("show_slug", "").strip()
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
        
    agree_sponsor_terms = request.form.get("agree_sponsor_terms", "").strip()
    if agree_sponsor_terms != "yes":
        flash("You must agree to the sponsorship terms before continuing.", "error")
        return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))
        

    catalog_id_raw = request.form.get("catalog_id", "").strip()
    if not catalog_id_raw.isdigit():
        flash("Please select a sponsorship level.", "error")
        return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))
    catalog_id = int(catalog_id_raw)

    catalog = get_catalog_item(catalog_id)
    if not catalog or int(catalog["show_id"]) != int(show["id"]):
        flash("That sponsorship is not valid for this show.", "error")
        return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))

    from sponsorship_system import list_sponsorship_catalog
    current_items = list_sponsorship_catalog(int(show["id"]), public_only=False)
    current = next((x for x in current_items if int(x["id"]) == catalog_id), None)
    if not current or current["effective_public_status"] == "sold_out":
        flash("That sponsorship is no longer available.", "error")
        return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))

    payment_method_choice = request.form.get("payment_method_choice", "card").strip().lower()
    if payment_method_choice not in {"card", "check", "invoice"}:
        payment_method_choice = "card"

    salesperson_id_raw = request.form.get("salesperson_id", "").strip()
    salesperson_id = int(salesperson_id_raw) if salesperson_id_raw.isdigit() else None
    salesperson = get_salesperson(salesperson_id) if salesperson_id else None
    commission_percent = float((salesperson or {}).get("default_commission_percent") or 0)
    logo_path = _save_sponsor_logo_upload(request.files.get("logo_file"))

    sale_id = save_sponsorship_sale(
        sale_id=None,
        show_id=int(show["id"]),
        catalog_id=catalog_id,
        sponsor_business_name=request.form.get("sponsor_business_name", "").strip(),
        contact_name=request.form.get("contact_name", "").strip(),
        phone=request.form.get("phone", "").strip(),
        email=request.form.get("email", "").strip(),
        mailing_address_line1=request.form.get("mailing_address_line1", "").strip(),
        mailing_address_line2=request.form.get("mailing_address_line2", "").strip(),
        mailing_city=request.form.get("mailing_city", "").strip(),
        mailing_state=request.form.get("mailing_state", "").strip(),
        mailing_zip=request.form.get("mailing_zip", "").strip(),
        website_url=request.form.get("website_url", "").strip(),
        salesperson_id=salesperson_id,
        commission_percent=commission_percent,
        logo_path=logo_path,
        logo_pending=1 if request.form.get("logo_pending") in {"1", "on"} else 0,
        placement=request.form.get("placement", "standard").strip(),
        payment_method_type="checkout" if payment_method_choice == "card" else payment_method_choice,
        payment_status="pending" if payment_method_choice != "invoice" else "invoice_requested",
        status="open" if payment_method_choice != "invoice" else "invoice_requested",
        notes=request.form.get("notes", "").strip(),
    )
#######
    if payment_method_choice == "card":
        _require_platform_stripe()

        success_url = _abs_url(url_for("sponsorship_checkout_success", sale_id=sale_id)) + "?session_id={CHECKOUT_SESSION_ID}"
        cancel_url = _abs_url(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))

        session_obj = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "unit_amount": int(catalog["price_cents"] or 0),
                    "product_data": {
                        "name": f"Sponsorship – {catalog['package_name']} ({show['title']})"
                    },
                },
                "quantity": 1,
            }],
            success_url=success_url,
            cancel_url=cancel_url,
            customer_email=request.form.get("email", "").strip() or None,
            metadata={
                "payment_item_type": "sponsorship",
                "show_id": str(show["id"]),
                "show_slug": show_slug,
                "sale_id": str(sale_id),
                "catalog_id": str(catalog_id),
            },
        )

        save_sponsorship_sale(
            sale_id=sale_id,
            show_id=int(show["id"]),
            catalog_id=catalog_id,
            sponsor_business_name=request.form.get("sponsor_business_name", "").strip(),
            contact_name=request.form.get("contact_name", "").strip(),
            phone=request.form.get("phone", "").strip(),
            email=request.form.get("email", "").strip(),
            mailing_address_line1=request.form.get("mailing_address_line1", "").strip(),
            mailing_address_line2=request.form.get("mailing_address_line2", "").strip(),
            mailing_city=request.form.get("mailing_city", "").strip(),
            mailing_state=request.form.get("mailing_state", "").strip(),
            mailing_zip=request.form.get("mailing_zip", "").strip(),
            website_url=request.form.get("website_url", "").strip(),
            salesperson_id=salesperson_id,
            commission_percent=commission_percent,
            logo_path=logo_path,
            logo_pending=1 if request.form.get("logo_pending") in {"1", "on"} else 0,
            placement=request.form.get("placement", "standard").strip(),
            payment_method_type="checkout",
            payment_status="pending",
            status="open",
            stripe_checkout_session_id=session_obj.id,
            notes=request.form.get("notes", "").strip(),
        )

        return redirect(session_obj.url)

    email_subject = f"Sponsorship {payment_method_choice} request – {request.form.get('sponsor_business_name', '').strip()}"
    email_body = (
        f"Show: {show['title']}\n"
        f"Sponsor business: {request.form.get('sponsor_business_name', '').strip()}\n"
        f"Contact: {request.form.get('contact_name', '').strip()}\n"
        f"Phone: {request.form.get('phone', '').strip()}\n"
        f"Email: {request.form.get('email', '').strip()}\n"
        f"Address 1: {request.form.get('mailing_address_line1', '').strip()}\n"
        f"Address 2: {request.form.get('mailing_address_line2', '').strip()}\n"
        f"City: {request.form.get('mailing_city', '').strip()}\n"
        f"State: {request.form.get('mailing_state', '').strip()}\n"
        f"ZIP: {request.form.get('mailing_zip', '').strip()}\n"
        f"Website: {request.form.get('website_url', '').strip()}\n"
        f"Package: {catalog['package_name']}\n"
        f"Amount: ${float(int(catalog['price_cents'] or 0) / 100):.2f}\n"
        f"Salesperson: {((salesperson or {}).get('name') or '').strip()}\n"
        f"Logo later: {'Yes' if request.form.get('logo_pending') in {'1', 'on'} else 'No'}\n"
        f"Notes: {request.form.get('notes', '').strip()}\n"
        f"Requested payment method: {payment_method_choice}\n"
    )

    _send_system_email(
        subject=email_subject,
        body=email_body,
        reply_to=request.form.get("email", "").strip(),
    )

    if payment_method_choice == "invoice":
        flash("Thank you. Your sponsorship request has been received. An invoice will be sent to the email provided within 1 business day.", "ok")
    else:
        flash("Thank you. Your sponsorship has been recorded as a check / salesperson-collected sale and our team has been notified.", "ok")

    return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))

@app.get("/sponsorship/checkout-success/<int:sale_id>")
def sponsorship_checkout_success(sale_id: int):
    sale = get_sponsorship_sale(sale_id)
    if not sale:
        return "Sponsorship sale not found.", 404

    session_id = request.args.get("session_id", "").strip()
    if not session_id:
        return render_template("payment_not_complete.html")

    _require_platform_stripe()
    try:
        sess = stripe.checkout.Session.retrieve(session_id)
    except Exception:
        return render_template("payment_not_complete.html")

    if sess.payment_status != "paid":
        return render_template("payment_not_complete.html")

    show_slug = ""
    try:
        show_slug = (sess.metadata or {}).get("show_slug", "").strip()
    except Exception:
        show_slug = ""
    show = get_show_by_slug(show_slug) if show_slug else get_active_show()
    if not show:
        return render_template("payment_not_complete.html")

    receipt_url = ""
    try:
        if getattr(sess, "payment_intent", None):
            pi = stripe.PaymentIntent.retrieve(sess.payment_intent)
            if getattr(pi, "latest_charge", None):
                ch = stripe.Charge.retrieve(pi.latest_charge)
                receipt_url = getattr(ch, "receipt_url", "") or ""
    except Exception:
        receipt_url = ""

    mark_sponsorship_sale_paid_by_checkout_session(sess.id, receipt_url=receipt_url)
    sale = get_sponsorship_sale_by_checkout_session(sess.id) or sale
    sponsor_name = (sale.get("sponsor_business_name") or "").strip()
    if sponsor_name:
        sponsor_id = upsert_sponsor(
            name=sponsor_name,
            logo_path=(sale.get("logo_path") or "").strip(),
            website_url=(sale.get("website_url") or "").strip(),
        )
        attach_sponsor_to_show(
            int(show["id"]),
            sponsor_id,
            placement=(sale.get("placement") or "standard").strip(),
            sort_order=100,
        )

    flash("Payment received. Stripe will send your receipt automatically.", "ok")
    return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show["slug"]))

@app.get("/admin")
def admin_page():
    show = get_active_show()
    next_url = request.args.get("next", "")
    registered_cars = count_registered_cars(int(show["id"])) if show else 0

    if not session.get("admin_authed"):
        return render_template(
            "admin.html",
            show=show,
            authed=False,
            next=next_url,
            registered_cars=registered_cars,
        )

    return render_template(
        "admin.html",
        show=show,
        authed=True,
        next=next_url,
        registered_cars=registered_cars,
    )

@app.get("/admin/car-search")
@require_admin
def admin_car_search():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    q = request.args.get("q", "").strip()
    results = search_show_cars_admin(int(show["id"]), q)

    return render_template(
        "admin_car_search.html",
        show=show,
        q=q,
        results=results,
    )
    
@app.get("/admin/command-center")
@require_admin
def admin_command_center():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    q = request.args.get("q", "").strip()
    search_results = search_show_cars_admin(int(show["id"]), q) if q else []
    cars = list_show_cars_public(int(show["id"]))

    registered_paid = [c for c in cars if (c["registration_payment_status"] or "") == "paid"]
    placeholders = [c for c in cars if int(c["is_placeholder"] or 0) == 1]
    checked_in = [c for c in cars if c["checked_in_at"]]

    return render_template(
        "admin_command_center.html",
        show=show,
        q=q,
        search_results=search_results,
        cars=cars,
        registered_paid=registered_paid,
        placeholders=placeholders,
        checked_in=checked_in,
    )
    
@app.post("/admin/login")
@rate_limit("admin_login", 10, 900)
def admin_login():
    pw = request.form.get("password", "")
    next_url = request.form.get("next", "") or url_for("admin_page")
    show = get_active_show()
    if _check_admin_password(pw):
        session["admin_authed"] = True
        _log_event("admin.login_success", int(show["id"]) if show else None, {"next": next_url}, actor_type="admin")
        return redirect(next_url)
    _log_event("admin.login_failed", int(show["id"]) if show else None, {"next": next_url}, actor_type="admin")
    return render_template("admin.html", show=show, authed=False, login_error="Incorrect password.", next=next_url)


@app.post("/admin/logout")
@require_admin
def admin_logout():
    show = get_active_show()
    session.pop("admin_authed", None)
    _log_event("admin.logout", int(show["id"]) if show else None, actor_type="admin")
    return redirect(url_for("admin_page"))


@app.get("/admin/stripe/connect")
@require_admin
def admin_connect_charity_stripe():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    _require_platform_stripe()
    return redirect(_build_connect_authorize_url(int(show["id"]), show["slug"]))


@app.get("/admin/stripe/connect/callback")
@require_admin
def admin_connect_charity_stripe_callback():
    _require_platform_stripe()
    state = request.args.get("state", "").strip()
    code = request.args.get("code", "").strip()
    error = request.args.get("error", "").strip()
    expected_state = session.get("stripe_connect_state")
    show_id = session.get("stripe_connect_show_id")

    if error:
        flash(f"Stripe connection was not completed: {error}", "error")
        _log_event("admin.stripe_connect_error", show_id, {"error": error}, actor_type="admin")
        return redirect(url_for("admin_page"))

    if not state or not expected_state or state != expected_state or not show_id:
        flash("Invalid Stripe Connect state. Please try again.", "error")
        return redirect(url_for("admin_page"))

    if not code:
        flash("Missing Stripe authorization code.", "error")
        return redirect(url_for("admin_page"))

    try:
        token_resp = stripe.OAuth.token(grant_type="authorization_code", code=code)
        stripe_account_id = token_resp.get("stripe_user_id", "")
        connect_email = ""
        if stripe_account_id:
            acct = stripe.Account.retrieve(stripe_account_id)
            connect_email = getattr(acct, "email", "") or ""
        if not stripe_account_id:
            flash("Stripe did not return a connected account ID.", "error")
            return redirect(url_for("admin_page"))

        set_show_charity_connect(int(show_id), stripe_account_id, connect_email=connect_email)
        _log_event(
            "admin.stripe_connected",
            int(show_id),
            {"stripe_account_id": stripe_account_id, "connect_email": connect_email},
            actor_type="admin",
        )
        flash("Charity Stripe account connected successfully.", "ok")
        return redirect(url_for("admin_page"))
    except Exception as e:
        flash(f"Unable to connect Stripe account: {e}", "error")
        return redirect(url_for("admin_page"))


@app.post("/admin/stripe/disconnect")
@require_admin
def admin_disconnect_charity_stripe():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    clear_show_charity_connect(int(show["id"]))
    _log_event("admin.stripe_disconnected", int(show["id"]), actor_type="admin")
    flash("Charity Stripe connection removed from this show.", "ok")
    return redirect(url_for("admin_page"))


@app.post("/admin/show-settings")
@require_admin
def admin_show_settings():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    show_type = (request.form.get("show_type") or "full").strip().lower()
    ov_raw = request.form.get("allow_prereg_override", "").strip()
    ov = None if ov_raw == "" else int(ov_raw) if ov_raw.isdigit() else None
    max_cars_raw = request.form.get("max_cars", "").strip()
    max_cars = None if max_cars_raw == "" else int(max_cars_raw) if max_cars_raw.isdigit() else None

    registration_fee_cents = _parse_dollars_to_cents(
        request.form.get("registration_fee_dollars", ""),
        int(show["registration_fee_cents"] or 0),
    )
    attendee_fee_cents = _parse_dollars_to_cents(
        request.form.get("attendee_fee_dollars", ""),
        int(show["attendee_fee_cents"] or 0),
    )
    vote_price_cents = _parse_dollars_to_cents(
        request.form.get("vote_price_dollars", ""),
        int(show["vote_price_cents"] or 100),
    )
    if vote_price_cents <= 0:
        vote_price_cents = 100

    update_show_admin_settings(
        int(show["id"]),
        show_type=show_type,
        allow_prereg_override=ov,
        max_cars=max_cars,
        registration_fee_cents=registration_fee_cents,
        attendee_fee_cents=attendee_fee_cents,
        vote_price_cents=vote_price_cents,
        public_vote_disclosure=request.form.get("public_vote_disclosure", ""),
        public_registration_disclosure=request.form.get("public_registration_disclosure", ""),
        public_donation_disclosure=request.form.get("public_donation_disclosure", ""),
        voting_mode=request.form.get("voting_mode", "fundraiser_unlimited").strip(),
        payment_mode=request.form.get("payment_mode", "stripe").strip(),
        external_payment_url=request.form.get("external_payment_url", "").strip(),
        allow_custom_votes=1 if request.form.get("allow_custom_votes") else 0,
        preset_vote_options=request.form.get("preset_vote_options", "1,5,10,20,25").strip(),
        max_votes_per_checkout=max(
            1,
            int(request.form.get("max_votes_per_checkout", "50") or "50")
        ) if (request.form.get("max_votes_per_checkout", "50") or "50").isdigit() else 50,
    )
    _log_event("admin.show_settings_saved", int(show["id"]), {"show_type": show_type, "max_cars": max_cars}, actor_type="admin")
    flash("Show settings saved.", "ok")
    return redirect(url_for("admin_page"))


@app.get("/admin/shows")
@require_admin
def admin_shows():
    return render_template(
        "admin_shows.html",
        shows=list_shows_admin(),
        show=get_active_show(),
        waiver_templates=list_waiver_templates(),
    )


@app.post("/admin/shows/create")
@require_admin
def admin_shows_create():
    slug = request.form.get("slug", "").strip()
    title = request.form.get("title", "").strip()

    if not slug or not title:
        flash("Title and slug are required.", "error")
        return redirect(url_for("admin_shows"))

    try:
        sort_order = int(request.form.get("sort_order", "100") or "100")
    except ValueError:
        sort_order = 100

    try:
        waiver_template_id = int(request.form.get("waiver_template_id", "0") or "0") or None
    except ValueError:
        waiver_template_id = None

    voting_mode = request.form.get("voting_mode", "fundraiser_unlimited").strip()
    payment_mode = request.form.get("payment_mode", "stripe").strip()
    external_payment_url = request.form.get("external_payment_url", "").strip()
    allow_custom_votes = 1 if request.form.get("allow_custom_votes") else 0
    preset_vote_options = request.form.get("preset_vote_options", "1,5,10,20,25").strip()
    max_votes_per_checkout_raw = request.form.get("max_votes_per_checkout", "50").strip()

    try:
        max_votes_per_checkout = max(1, int(max_votes_per_checkout_raw))
    except ValueError:
        max_votes_per_checkout = 50

    flyer_image_path = request.form.get("flyer_image_path", "").strip()
    flyer_file = request.files.get("flyer_image")
    if flyer_file and flyer_file.filename:
        try:
            flyer_image_path = _save_uploaded_flyer(flyer_file, slug)
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("admin_shows"))

    create_show_admin(
        slug=slug,
        flyer_image_path=flyer_image_path,
        title=title,
        date=request.form.get("date", "").strip(),
        time=request.form.get("time", "").strip(),
        cars_arrive_time=request.form.get("cars_arrive_time", "").strip(),
        day_of_registration_time=request.form.get("day_of_registration_time", "").strip(),
        show_start_time=request.form.get("show_start_time", "").strip(),
        show_end_time=request.form.get("show_end_time", "").strip(),
        location_name=request.form.get("location_name", "").strip(),
        address=request.form.get("address", "").strip(),
        benefiting=request.form.get("benefiting", "").strip(),
        suggested_donation=request.form.get("suggested_donation", "").strip(),
        description=request.form.get("description", "").strip(),
        status=request.form.get("status", "draft").strip(),
        short_details=request.form.get("short_details", "").strip(),
        qr_message=request.form.get("qr_message", "").strip(),
        cta_label=request.form.get("cta_label", "").strip(),
        cta_url=request.form.get("cta_url", "").strip(),
        show_on_site=1 if request.form.get("show_on_site") == "on" else 0,
        sort_order=sort_order,
        hide_address=1 if request.form.get("hide_address") == "on" else 0,
        waiver_template_id=waiver_template_id,
        organizer_name=request.form.get("organizer_name", "").strip(),
        venue_name=request.form.get("venue_name", "").strip(),
        venue_address_line1=request.form.get("venue_address_line1", "").strip(),
        venue_address_line2=request.form.get("venue_address_line2", "").strip(),
        venue_city=request.form.get("venue_city", "").strip(),
        venue_state=request.form.get("venue_state", "").strip(),
        venue_zip=request.form.get("venue_zip", "").strip(),
        charity_name=request.form.get("charity_name", "").strip(),
        charity_description=request.form.get("charity_description", "").strip(),
        voting_mode=voting_mode,
        payment_mode=payment_mode,
        external_payment_url=external_payment_url,
        allow_custom_votes=allow_custom_votes,
        preset_vote_options=preset_vote_options,
        max_votes_per_checkout=max_votes_per_checkout,
    )

    flash("Show created.", "ok")
    return redirect(url_for("admin_shows"))


@app.post("/admin/shows/<int:show_id>/update")
@require_admin
def admin_shows_update(show_id: int):
    try:
        sort_order = int(request.form.get("sort_order", "100") or "100")
    except ValueError:
        sort_order = 100

    try:
        waiver_template_id = int(request.form.get("waiver_template_id", "0") or "0") or None
    except ValueError:
        waiver_template_id = None

    voting_mode = request.form.get("voting_mode", "fundraiser_unlimited").strip()
    payment_mode = request.form.get("payment_mode", "stripe").strip()
    external_payment_url = request.form.get("external_payment_url", "").strip()
    allow_custom_votes = 1 if request.form.get("allow_custom_votes") else 0
    preset_vote_options = request.form.get("preset_vote_options", "1,5,10,20,25").strip()
    max_votes_per_checkout_raw = request.form.get("max_votes_per_checkout", "50").strip()

    try:
        max_votes_per_checkout = max(1, int(max_votes_per_checkout_raw))
    except ValueError:
        max_votes_per_checkout = 50

    slug = request.form.get("slug", "").strip()
    flyer_image_path = request.form.get("flyer_image_path", "").strip()
    flyer_file = request.files.get("flyer_image")
    if flyer_file and flyer_file.filename:
        try:
            flyer_image_path = _save_uploaded_flyer(flyer_file, slug)
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("admin_shows"))

    update_show_admin_record(
        show_id,
        slug=slug,
        title=request.form.get("title", "").strip(),
        flyer_image_path=flyer_image_path,
        date=request.form.get("date", "").strip(),
        time=request.form.get("time", "").strip(),
        cars_arrive_time=request.form.get("cars_arrive_time", "").strip(),
        day_of_registration_time=request.form.get("day_of_registration_time", "").strip(),
        show_start_time=request.form.get("show_start_time", "").strip(),
        show_end_time=request.form.get("show_end_time", "").strip(),
        location_name=request.form.get("location_name", "").strip(),
        address=request.form.get("address", "").strip(),
        benefiting=request.form.get("benefiting", "").strip(),
        suggested_donation=request.form.get("suggested_donation", "").strip(),
        description=request.form.get("description", "").strip(),
        status=request.form.get("status", "draft").strip(),
        short_details=request.form.get("short_details", "").strip(),
        qr_message=request.form.get("qr_message", "").strip(),
        cta_label=request.form.get("cta_label", "").strip(),
        cta_url=request.form.get("cta_url", "").strip(),
        show_on_site=1 if request.form.get("show_on_site") == "on" else 0,
        sort_order=sort_order,
        hide_address=1 if request.form.get("hide_address") == "on" else 0,
        waiver_template_id=waiver_template_id,
        organizer_name=request.form.get("organizer_name", "").strip(),
        venue_name=request.form.get("venue_name", "").strip(),
        venue_address_line1=request.form.get("venue_address_line1", "").strip(),
        venue_address_line2=request.form.get("venue_address_line2", "").strip(),
        venue_city=request.form.get("venue_city", "").strip(),
        venue_state=request.form.get("venue_state", "").strip(),
        venue_zip=request.form.get("venue_zip", "").strip(),
        charity_name=request.form.get("charity_name", "").strip(),
        charity_description=request.form.get("charity_description", "").strip(),
        voting_mode=voting_mode,
        payment_mode=payment_mode,
        external_payment_url=external_payment_url,
        allow_custom_votes=allow_custom_votes,
        preset_vote_options=preset_vote_options,
        max_votes_per_checkout=max_votes_per_checkout,
    )

    flash("Show updated.", "ok")
    return redirect(url_for("admin_shows"))


@app.post("/admin/shows/<int:show_id>/set-active")
@require_admin
def admin_shows_set_active(show_id: int):
    set_active_show(show_id)
    _log_event("admin.show_set_active", show_id, actor_type="admin")
    flash("Show set as active.", "ok")
    return redirect(url_for("admin_shows"))


@app.post("/admin/shows/<int:show_id>/set-upcoming")
@require_admin
def admin_shows_set_upcoming(show_id: int):
    set_upcoming_show(show_id)
    _log_event("admin.show_set_upcoming", show_id, actor_type="admin")
    flash("Show set as upcoming.", "ok")
    return redirect(url_for("admin_shows"))


@app.post("/admin/shows/<int:show_id>/set-past")
@require_admin
def admin_shows_set_past(show_id: int):
    set_past_show(show_id)
    _log_event("admin.show_set_past", show_id, actor_type="admin")
    flash("Show moved to past.", "ok")
    return redirect(url_for("admin_shows"))


@app.get("/admin/waivers")
@require_admin
def admin_waivers():
    return render_template("admin_waivers.html", templates=list_waiver_templates(), show=get_active_show(), preset_labels=PRESET_LABELS)


@app.get("/admin/waivers/new")
@require_admin
def admin_waiver_new():
    waiver = _waiver_editor_payload()
    return render_template("admin_waiver_edit.html", waiver=waiver, show=get_active_show(), preset_labels=PRESET_LABELS)


@app.post("/admin/waivers/new")
@require_admin
def admin_waiver_create():
    title = request.form.get("title", "").strip()
    version = request.form.get("version", "").strip()
    is_default = request.form.get("is_default", "") == "on"
    builder_config = _waiver_builder_config_from_request()
    body_template = request.form.get("body_template", "").strip()
    if not builder_config.get("use_advanced_editor"):
        body_template = build_waiver_template_from_builder(builder_config)

    waiver = _waiver_editor_payload(form_override={
        "title": title,
        "version": version,
        "body_template": body_template,
        "is_default": 1 if is_default else 0,
        "builder_config": builder_config,
    })

    if not (title and version and body_template):
        flash("Title, version, and waiver content are required.", "error")
        return render_template("admin_waiver_edit.html", waiver=waiver, show=get_active_show(), preset_labels=PRESET_LABELS)

    create_waiver_template(
        title=title,
        version=version,
        body_template=body_template,
        is_default=is_default,
        preset_key=builder_config.get("preset_key", "standard"),
        builder_config=builder_config_to_json(builder_config),
    )
    flash("Waiver template created.", "ok")
    return redirect(url_for("admin_waivers"))


@app.get("/admin/waivers/<int:waiver_template_id>/edit")
@require_admin
def admin_waiver_edit(waiver_template_id: int):
    waiver = get_waiver_template_by_id(waiver_template_id)
    if not waiver:
        return "Waiver template not found.", 404
    return render_template("admin_waiver_edit.html", waiver=_waiver_editor_payload(waiver), show=get_active_show(), preset_labels=PRESET_LABELS)


@app.post("/admin/waivers/<int:waiver_template_id>/edit")
@require_admin
def admin_waiver_update(waiver_template_id: int):
    existing = get_waiver_template_by_id(waiver_template_id)
    if not existing:
        return "Waiver template not found.", 404

    title = request.form.get("title", "").strip()
    version = request.form.get("version", "").strip()
    is_default = request.form.get("is_default", "") == "on"
    builder_config = _waiver_builder_config_from_request()
    body_template = request.form.get("body_template", "").strip()
    if not builder_config.get("use_advanced_editor"):
        body_template = build_waiver_template_from_builder(builder_config)

    waiver = _waiver_editor_payload(existing, form_override={
        "id": waiver_template_id,
        "title": title,
        "version": version,
        "body_template": body_template,
        "is_default": 1 if is_default else 0,
        "builder_config": builder_config,
    })

    if not (title and version and body_template):
        flash("Title, version, and waiver content are required.", "error")
        return render_template("admin_waiver_edit.html", waiver=waiver, show=get_active_show(), preset_labels=PRESET_LABELS)

    update_waiver_template(
        waiver_template_id=waiver_template_id,
        title=title,
        version=version,
        body_template=body_template,
        is_default=is_default,
        preset_key=builder_config.get("preset_key", "standard"),
        builder_config=builder_config_to_json(builder_config),
    )
    flash("Waiver template updated.", "ok")
    return redirect(url_for("admin_waivers"))


@app.get("/admin/print-cards.pdf")
@require_admin
def admin_print_cards_pdf():
    from utils.print_cards import build_landscape_cards_pdf

    show = get_active_show()
    if not show:
        return "No active show.", 500

    ids_raw = request.args.get("ids", "").strip()
    all_raw = request.args.get("all", "").strip()
    include_back = request.args.get("back", "").strip() == "1"

    cars = list_show_cars_public(int(show["id"]))
    if not cars:
        return "No cars to print.", 400

    selected = cars
    if all_raw != "1":
        want_ids = set()
        if ids_raw:
            for part in ids_raw.split(","):
                part = part.strip()
                if part.isdigit():
                    want_ids.add(int(part))

        if not want_ids:
            return "No cars selected.", 400

        selected = [r for r in cars if int(r["id"]) in want_ids]

    title_sponsor, sponsors = get_show_sponsors(int(show["id"])) or (None, [])

    pdf_bytes = build_landscape_cards_pdf(
        show=dict(show),
        cars_rows=[dict(r) for r in selected],
        base_url=_abs_url(""),
        static_root=os.path.join(app.root_path, "static"),
        title_sponsor=title_sponsor,
        sponsors=sponsors,
        include_back=include_back,
        mirror_back_pages=False,
    )

    _log_event(
        "admin.print_cards_exported",
        int(show["id"]),
        {"count": len(selected), "include_back": include_back},
        actor_type="admin",
    )

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"{show['slug']}-voting-cards-landscape.pdf",
    )


@app.get("/admin/export-snapshot.zip")
@require_admin
def admin_export_snapshot_zip():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    zip_bytes, filename = build_snapshot_zip_bytes(int(show["id"]))
    _log_event("admin.snapshot_exported", int(show["id"]), {"filename": filename}, actor_type="admin")
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip", as_attachment=True, download_name=filename)


@app.post("/admin/close-voting-and-export")
@require_admin
def admin_close_voting_and_export():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    set_show_voting_open(int(show["id"]), False)
    zip_bytes, filename = build_snapshot_zip_bytes(int(show["id"]))
    _log_event("admin.voting_closed_and_exported", int(show["id"]), {"filename": filename}, actor_type="admin")
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip", as_attachment=True, download_name=filename)


@app.post("/admin/toggle-voting")
@require_admin
def admin_toggle_voting():
    show = get_active_show()
    if show:
        toggle_show_voting(int(show["id"]))
        _log_event("admin.voting_toggled", int(show["id"]), actor_type="admin")
    return redirect(url_for("admin_page"))


@app.post("/admin/open-voting")
@require_admin
def admin_open_voting():
    show = get_active_show()
    if show:
        set_show_voting_open(int(show["id"]), True)
        _log_event("admin.voting_opened", int(show["id"]), actor_type="admin")
    return redirect(url_for("admin_page"))


@app.post("/admin/close-voting")
@require_admin
def admin_close_voting():
    show = get_active_show()
    if show:
        set_show_voting_open(int(show["id"]), False)
        _log_event("admin.voting_closed", int(show["id"]), actor_type="admin")
    return redirect(url_for("admin_page"))


@app.post("/admin/reset-votes")
@require_admin
def admin_reset_votes():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    zip_bytes, filename = build_snapshot_zip_bytes(int(show["id"]))
    reset_votes_for_show(int(show["id"]))
    _log_event("admin.votes_reset", int(show["id"]), {"backup_filename": filename}, actor_type="admin")
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip", as_attachment=True, download_name=filename)


@app.get("/admin/leads")
@require_admin
def admin_leads():
    show_id_raw = request.args.get("show_id", "").strip()
    selected_show_id = int(show_id_raw) if show_id_raw.isdigit() else None

    leads = list_event_interest_signups(selected_show_id)
    shows = list_shows_admin()

    return render_template(
        "admin_leads.html",
        show=get_active_show(),
        shows=shows,
        leads=leads,
        selected_show_id=selected_show_id,
    )


@app.get("/admin/leads/export.csv")
@require_admin
def admin_leads_export():
    show_id_raw = request.args.get("show_id", "").strip()
    selected_show_id = int(show_id_raw) if show_id_raw.isdigit() else None

    csv_bytes = export_event_interest_signups_csv(selected_show_id)
    filename = "event-leads.csv" if selected_show_id is None else f"event-leads-show-{selected_show_id}.csv"

    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.get("/admin/leaderboard")
@require_admin
def admin_leaderboard():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    return render_template(
        "leaderboard.html",
        show=show,
        by_category=leaderboard_by_category(int(show["id"])),
        overall=leaderboard_overall(int(show["id"])),
    )


@app.get("/admin/export-votes.csv")
@require_admin
def admin_export_votes():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    rows = export_votes_for_show(int(show["id"]))
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "created_at",
        "category",
        "vote_qty",
        "amount_cents",
        "stripe_session_id",
        "car_number",
        "year",
        "make",
        "model",
        "owner_name",
        "owner_phone",
        "owner_email",
        "opt_in_future",
    ])
    for r in rows:
        w.writerow([
            r["created_at"],
            r["category"],
            r["vote_qty"],
            r["amount_cents"],
            r["stripe_session_id"],
            r["car_number"],
            r["year"],
            r["make"],
            r["model"],
            r["owner_name"],
            r["owner_phone"],
            r["owner_email"],
            r["opt_in_future"],
        ])
    _log_event("admin.votes_exported", int(show["id"]), {"row_count": len(rows)}, actor_type="admin")
    mem = io.BytesIO(buf.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="votes_export.csv")


@app.get("/admin/placeholders")
@require_admin
def admin_placeholders():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    return render_template("admin_placeholders.html", show=show, cars=list_show_cars_public(int(show["id"])))


@app.post("/admin/placeholders/create")
@require_admin
def admin_placeholders_create():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    start_raw = request.form.get("start_number", "1").strip()
    count_raw = request.form.get("count", "50").strip()
    try:
        start_number = int(start_raw)
        count = int(count_raw)
        if start_number < 1 or count < 1 or count > 1000:
            raise ValueError()
    except ValueError:
        flash("Invalid placeholder range. Count must be 1–1000.", "error")
        return redirect(url_for("admin_placeholders"))
    created = create_placeholder_cars(int(show["id"]), start_number=start_number, count=count)
    _log_event(
        "admin.placeholders_created",
        int(show["id"]),
        {"start_number": start_number, "count_requested": count, "count_created": created},
        actor_type="admin",
    )
    flash(f"Created {created} placeholder cars.", "ok")
    return redirect(url_for("admin_placeholders"))


@app.post("/admin/waiver-received")
@require_admin
def admin_waiver_received():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    show_car_id_raw = request.form.get("show_car_id", "").strip()
    if not show_car_id_raw.isdigit():
        return redirect(url_for("admin_placeholders"))
    show_car_id = int(show_car_id_raw)
    waiver_mark_received(int(show["id"]), show_car_id, received_by="admin")
    _log_event("admin.waiver_marked_received", int(show["id"]), {"show_car_id": show_car_id}, actor_type="admin")
    flash("Waiver marked as received.", "ok")
    return redirect(url_for("admin_placeholders"))




@app.get("/admin/debug/routes")
@require_admin
def admin_debug_routes():
    routes = []
    for rule in app.url_map.iter_rules():
        methods = sorted(m for m in rule.methods if m not in ("HEAD", "OPTIONS"))
        routes.append({"rule": str(rule), "endpoint": rule.endpoint, "methods": methods})
    routes.sort(key=lambda r: r["rule"])
    return {"count": len(routes), "routes": routes}


@app.post("/stripe/webhook")
def stripe_webhook():
    _require_platform_stripe()
    payload = request.get_data(as_text=False)
    sig_header = request.headers.get("Stripe-Signature", "")
    if not STRIPE_WEBHOOK_SECRET:
        return "Webhook secret not configured.", 500

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        return "Invalid payload.", 400
    except stripe.error.SignatureVerificationError:
        return "Invalid signature.", 400

    event_id = event.get("id", "")
    event_type = event.get("type", "")
    if event_id and has_processed_webhook_event(event_id):
        return jsonify({"ok": True, "duplicate": True})

    try:
        if event_type in ("checkout.session.completed", "checkout.session.async_payment_succeeded"):
            obj = event["data"]["object"]
            session_id = obj.get("id", "")
            payment_status = obj.get("payment_status", "")
            metadata = obj.get("metadata", {}) or {}
            item_type = metadata.get("payment_item_type", "")
            if session_id and payment_status == "paid":
                if item_type == "registration":
                    finalize_registration_intent_paid(session_id)
                elif item_type == "placeholder_claim":
                    show_car_id = int(metadata.get("show_car_id", "0") or "0")
                    if show_car_id:
                        _finalize_placeholder_claim_paid(stripe_session_id=session_id, show_car_id=show_car_id)
                elif item_type == "vote":
                    finalize_vote_intent_paid(session_id)
                elif item_type == "attendance_fee":
                    mark_donation_paid(session_id)

        if event_id:
            mark_webhook_event_processed(event_id, event_type)
        return jsonify({"ok": True})
    except Exception as e:
        return f"Webhook processing error: {e}", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
