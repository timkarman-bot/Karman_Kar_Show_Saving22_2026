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
    finalize_external_vote_intent,
    list_pending_vote_reviews,
    reject_external_vote_intent,
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

        vote_intent_id = create_vote_intent(
            int(show["id"]),
            int(car["id"]),
            CATEGORY_SLUGS[category_slug],
            vote_qty,
            amount_cents,
        )

        return jsonify({
            "ok": True,
            "payment_mode": "external",
            "redirect_url": url_for(
                "external_vote_payment_page",
                vote_intent_id=vote_intent_id,
                show_slug=show_slug,
                car_token=car_token,
            ),
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
    

@app.get("/vote/external/<int:vote_intent_id>")
def external_vote_payment_page(vote_intent_id: int):
    conn = _conn_direct()
    try:
        vote_intent = conn.execute(
            "SELECT * FROM vote_intents WHERE id = ? LIMIT 1",
            (vote_intent_id,),
        ).fetchone()
    finally:
        conn.close()

    if not vote_intent:
        return "Vote intent not found.", 404

    show_slug = request.args.get("show_slug", "").strip()
    show = get_show_by_slug(show_slug) if show_slug else get_active_show()
    if not show or int(show["id"]) != int(vote_intent["show_id"]):
        return "Show not found.", 404

    car_token = request.args.get("car_token", "").strip()
    car = get_show_car_public_by_token(int(show["id"]), car_token) if car_token else None

    external_payment_url = (show["external_payment_url"] or "").strip() if "external_payment_url" in show.keys() else ""
    if not external_payment_url:
        return "External payment URL is not configured for this show.", 400

    category_code_map = {
        "People’s Choice": "PC",
        "Army": "ARMY",
        "Navy": "NAVY",
        "Air Force": "AF",
        "Marines": "MAR",
        "Coast Guard": "CG",
        "Space Force": "SF",
    }

    show_code = "".join(ch for ch in (show["slug"] or "").upper() if ch.isalnum())[:6] or "SHOW"
    car_number = int(car["car_number"]) if car else 0
    category_code = category_code_map.get(vote_intent["category"], "VOTE")
    vote_qty = int(vote_intent["vote_qty"] or 0)
    note_token = f"{show_code}-{car_number:03d}-{category_code}-{vote_qty}"

    return render_template(
        "external_vote_payment.html",
        show=show,
        car=car,
        vote_intent=vote_intent,
        external_payment_url=external_payment_url,
        amount_cents=int(vote_intent["amount_cents"] or 0),
        note_token=note_token,
    )


@app.post("/vote/external/confirm")
@rate_limit("external_vote_confirm", 20, 300)
def external_vote_confirm():
    vote_intent_id_raw = request.form.get("vote_intent_id", "").strip()
    payer_name = request.form.get("payer_name", "").strip()
    payment_note = request.form.get("payment_note", "").strip().upper()

    try:
        vote_intent_id = int(vote_intent_id_raw)
    except ValueError:
        return "Invalid vote intent.", 400

    conn = _conn_direct()
    try:
        vote_intent = conn.execute(
            "SELECT * FROM vote_intents WHERE id = ? LIMIT 1",
            (vote_intent_id,),
        ).fetchone()
        if not vote_intent:
            return "Vote intent not found.", 404

        car = conn.execute(
            "SELECT car_number FROM show_cars WHERE id = ? LIMIT 1",
            (int(vote_intent["show_car_id"]),),
        ).fetchone()

        show = conn.execute(
            "SELECT slug FROM shows WHERE id = ? LIMIT 1",
            (int(vote_intent["show_id"]),),
        ).fetchone()

        if not car or not show:
            return "Vote details not found.", 404

        category_code_map = {
            "People’s Choice": "PC",
            "Army": "ARMY",
            "Navy": "NAVY",
            "Air Force": "AF",
            "Marines": "MAR",
            "Coast Guard": "CG",
            "Space Force": "SF",
        }

        show_code = "".join(ch for ch in (show["slug"] or "").upper() if ch.isalnum())[:6] or "SHOW"
        expected_note = f"{show_code}-{int(car['car_number']):03d}-{category_code_map.get(vote_intent['category'], 'VOTE')}-{int(vote_intent['vote_qty'])}"

        approval_reference = f"external:{payer_name} | {payment_note}".strip()

        if payment_note == expected_note:
            conn.close()
            finalize_external_vote_intent(
                vote_intent_id,
                approval_reference=approval_reference,
            )
            flash("Payment note matched. Your votes were approved automatically.", "ok")
        else:
            conn.execute(
                """
                UPDATE vote_intents
                SET payment_status = 'pending_review',
                    stripe_payment_intent_id = ?
                WHERE id = ?
                """,
                (approval_reference, vote_intent_id),
            )
            conn.commit()
            flash("Payment submitted. It has been sent for review before votes are counted.", "ok")
    finally:
        try:
            conn.close()
        except Exception:
            pass

    active_show = get_active_show()
    if active_show:
        return redirect(url_for("show_page", slug=active_show["slug"]))
    return redirect(url_for("home"))


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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)