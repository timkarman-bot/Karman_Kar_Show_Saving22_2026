# Karman Kar Shows & Events — hardened app.py  04/06/2026
# 4-space indentation only (no tabs)

from dotenv import load_dotenv
load_dotenv()

import os
import io
import csv
import hmac
import re
import secrets
import sqlite3
import hashlib
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Dict, Optional, Any, Callable, List
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
from werkzeug.security import check_password_hash, generate_password_hash
from functools import wraps
from sponsorship_blueprint import sponsorship_bp
from database import (
    init_db,
    ensure_default_show,
    build_snapshot_zip_bytes,
    get_active_show,
    get_show_by_slug,
    get_show_by_id,
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
    find_vote_car_by_number,
    normalize_voting_method,
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
    list_marketing_contacts,
    create_event_interest_signup,
    list_shows_admin,
    list_public_registerable_shows,
    get_next_upcoming_show,
    create_show_admin,
    update_show_admin_record,
    set_active_show,
    set_upcoming_show,
    export_event_interest_signups_csv,
    export_marketing_contacts_csv,
    set_past_show,
    list_waiver_templates,
    get_waiver_template_by_id,
    create_waiver_template,
    update_waiver_template,
    get_effective_waiver_template_for_show,
    get_next_available_car_number,
    search_show_cars_admin,
    get_show_car_admin_by_id,
    update_show_car_admin_registration,
    remove_show_car_registration,
    get_vote_intent,
    finalize_external_vote_intent,
    list_pending_vote_reviews,
    reject_external_vote_intent,
    list_registration_slots,
    save_registration_slots_for_show,
    get_registration_slot,
    show_has_registration_slots,
    show_slot_has_capacity,
    get_show_registration_slot_selection_mode,
    list_judging_classes,
    save_judging_classes_for_show,
    list_paper_ballot_classes,
    create_paper_ballot_with_votes,
    list_recent_paper_ballots,
    build_paper_ballot_csv_template,
    import_paper_ballot_csv,
    find_matching_judging_class,
    ensure_placeholder_cards_up_to_max,
    import_judging_classes_for_show,
    import_registered_cars_for_show,
    archive_show,
    get_admin_user_by_email,
    get_admin_user_by_id,
    create_admin_user,
    list_admin_users,
    set_admin_user_active,
    assign_admin_user_show_role,
    remove_admin_user_show_role,
    list_admin_user_show_roles,
    admin_user_can_access_show,
    list_show_ids_for_admin_user,
    list_shows_admin_for_user,
    create_judge_voter,
    get_or_create_participant_voter,
    get_show_voter_by_token,
    activate_show_voter,
    list_show_voters,
    get_restricted_vote,
    upsert_restricted_vote,
    restricted_vote_progress,
    create_contact_message,
    mark_contact_message_email_result,
    list_contact_messages,
    get_contact_message,
    mark_contact_message_read,
    archive_contact_message,
    count_new_contact_messages,
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

from settlement_tools import build_settlement_workbook, parse_stripe_csv
from organizer_system import (
    authenticate_organizer,
    create_organizer,
    create_organizer_show,
    get_organizer,
    get_organizer_show,
    init_organizer_tables,
    list_organizer_shows,
    list_platform_organizers,
    list_platform_organizer_shows,
    show_reference,
    show_transaction_report,
)
from platform_operations import (
    init_platform_operations,
    list_show_categories,
    parse_category_csv,
    save_show_categories,
    update_show_platform_pricing,
    upsert_platform_payment_record,
)
from vendor_system import (
    attach_vendor_checkout,
    cleanup_expired_vendor_holds,
    create_vendor_hold,
    finalize_vendor_paid,
    get_vendor_registration,
    get_vendor_registration_by_token,
    get_vendor_settings,
    init_vendor_tables,
    list_vendor_registrations,
    package_availability,
    save_vendor_packages,
    save_vendor_settings,
    set_vendor_status,
    update_vendor_admin,
    vendor_csv_bytes,
    vendor_dashboard,
    vendor_registration_open,
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
try:
    LOCAL_TZ = LOCAL_TZ
except Exception:
    LOCAL_TZ = timezone(timedelta(hours=-6), "America/Chicago")

CATEGORY_SLUGS: Dict[str, str] = {
    "army": "Army",
    "navy": "Navy",
    "air-force": "Air Force",
    "marines": "Marines",
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
init_organizer_tables(os.getenv("DB_PATH") or ("/data/app.db" if os.path.isdir("/data") else "app.db"))
init_platform_operations(os.getenv("DB_PATH") or ("/data/app.db" if os.path.isdir("/data") else "app.db"))
init_vendor_tables(os.getenv("DB_PATH") or ("/data/app.db" if os.path.isdir("/data") else "app.db"))

CONSENT_TEXT_CAR_OWNER = (
    "By submitting this form, you agree Karman Kar Shows & Events may contact you about this event and future events "
    "if selected. Msg/data rates may apply. Opt out anytime."
)
CONSENT_VERSION = "2026-06-25"
ATTENDEE_CONSENT_TEXT = (
    "By selecting these options, you agree Karman Kar Shows & Events may contact you about the event and, "
    "if selected, share sponsor offers or information from the benefiting charity. "
    "Each permission is optional. Msg/data rates may apply. Opt out anytime."
)
ATTENDEE_CONSENT_VERSION = "2026-06-25"

DEFAULT_PUBLIC_VOTE_DISCLOSURE = (
    "Vote payments are processed through the event payment system. "
    "A portion of each vote payment supports the named charity, "
    "and votes are counted after payment verification."
)

# ==========================================================
# Version Information
# ==========================================================

APP_VERSION = "0.10.3-beta"
APP_RELEASE_STAGE = "beta"
APP_RELEASE_NAME = "Consent, Restricted Voting, and Event-Day Safety Beta"


def _event_date_status(show: Any) -> Dict[str, Any]:
    raw = str(show["date"] or "").strip() if show else ""
    if not raw:
        return {"parsed": False, "is_past": False, "weekday_mismatch": False, "message": ""}
    supplied_weekday = raw.split(",", 1)[0].strip() if "," in raw else ""
    date_part = raw.split(",", 1)[1].strip() if supplied_weekday else raw
    parsed = None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%B %d %Y", "%b %d %Y"):
        try:
            parsed = datetime.strptime(date_part, fmt).date()
            break
        except ValueError:
            continue
    if not parsed:
        # Handle public-facing ranges such as "TBA but will be April 25 or 26, 2026".
        range_match = re.search(
            r"\b([A-Za-z]+)\s+(\d{1,2})(?:\s+or\s+(\d{1,2}))?,?\s+(\d{4})\b",
            raw,
            flags=re.IGNORECASE,
        )
        if range_match:
            month, first_day, second_day, year = range_match.groups()
            latest_day = max(int(first_day), int(second_day or first_day))
            for fmt in ("%B %d %Y", "%b %d %Y"):
                try:
                    parsed = datetime.strptime(f"{month} {latest_day} {year}", fmt).date()
                    break
                except ValueError:
                    continue
    if not parsed:
        return {"parsed": False, "is_past": False, "weekday_mismatch": False, "message": ""}
    expected_weekday = parsed.strftime("%A")
    mismatch = bool(supplied_weekday and supplied_weekday.lower() != expected_weekday.lower())
    return {
        "parsed": True,
        "date": parsed.isoformat(),
        "is_past": parsed < datetime.now(LOCAL_TZ).date(),
        "weekday_mismatch": mismatch,
        "expected_weekday": expected_weekday,
        "message": f"Configured weekday should be {expected_weekday}." if mismatch else "",
    }


def _get_public_active_show():
    """Return the configured active show only while its event date is current or future."""
    show = get_active_show()
    if show and _event_date_status(show)["is_past"]:
        return None
    return show


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
    return str(st).strip().lower() in {"full", "cruise_in"}


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


def _parse_optional_datetime(value: str) -> Optional[datetime]:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _slot_payloads_from_request() -> list[dict[str, Any]]:
    """Parse up to five registration day/session slots from the admin show form."""
    payloads: list[dict[str, Any]] = []
    for idx in range(1, 6):
        label = request.form.get(f"slot_{idx}_label", "").strip()
        slot_id = request.form.get(f"slot_{idx}_id", "").strip()
        if not label and not slot_id:
            continue
        payloads.append({
            "id": slot_id,
            "slot_label": label,
            "slot_date": request.form.get(f"slot_{idx}_date", "").strip(),
            "cars_arrive_time": request.form.get(f"slot_{idx}_cars_arrive_time", "").strip(),
            "start_time": request.form.get(f"slot_{idx}_start_time", "").strip(),
            "end_time": request.form.get(f"slot_{idx}_end_time", "").strip(),
            "participant_instructions": request.form.get(f"slot_{idx}_participant_instructions", "").strip(),
            "capacity": request.form.get(f"slot_{idx}_capacity", "0").strip(),
            "sort_order": request.form.get(f"slot_{idx}_sort_order", str(idx * 10)).strip(),
            "is_active": "on" if request.form.get(f"slot_{idx}_is_active") == "on" else "0",
        })
    return payloads



def _judging_class_payloads_from_request() -> list[dict[str, Any]]:
    """Parse up to 50 judging classes from the admin show form."""
    payloads: list[dict[str, Any]] = []
    for idx in range(1, 51):
        name = request.form.get(f"class_{idx}_name", "").strip()
        code = request.form.get(f"class_{idx}_code", "").strip()
        if not name and not code:
            continue
        payloads.append({
            "class_name": name or code,
            "class_code": code,
            "description": request.form.get(f"class_{idx}_description", "").strip(),
            "sort_order": request.form.get(f"class_{idx}_sort_order", str(idx * 10)).strip(),
            "is_active": "1" if request.form.get(f"class_{idx}_is_active") == "on" else "0",
            "year_min": request.form.get(f"class_{idx}_year_min", "").strip(),
            "year_max": request.form.get(f"class_{idx}_year_max", "").strip(),
            "make_contains": request.form.get(f"class_{idx}_make_contains", "").strip(),
            "model_contains": request.form.get(f"class_{idx}_model_contains", "").strip(),
            "keyword_contains": request.form.get(f"class_{idx}_keyword_contains", "").strip(),
            "award_places": request.form.get(f"class_{idx}_award_places", "3").strip(),
        })
    return payloads


def _auto_class_for_vehicle(show_id: int, year: str, make: str, model: str) -> tuple[Optional[int], int]:
    try:
        return find_matching_judging_class(int(show_id), year, make, model)
    except Exception:
        return None, 1

def _registration_slots_for_public(show_id: int):
    return list_registration_slots(show_id, public_only=True)


def _registration_slot_selection_mode(show: Any) -> str:
    try:
        value = (show["registration_slot_selection_mode"] if "registration_slot_selection_mode" in show.keys() else "single") or "single"
    except Exception:
        value = "single"
    value = str(value).strip().lower()
    return value if value in {"single", "multiple"} else "single"


def _selected_registration_slot_ids(show: Any) -> list[int]:
    show_id = int(show["id"])
    mode = _registration_slot_selection_mode(show)
    raw_values = request.form.getlist("registration_slot_ids")
    raw_single = request.form.get("registration_slot_id", "").strip()
    if raw_single:
        raw_values.append(raw_single)
    out: list[int] = []
    for raw in raw_values:
        raw = str(raw or "").strip()
        if not raw.isdigit():
            continue
        slot_id = int(raw)
        if slot_id in out:
            continue
        if get_registration_slot(show_id, slot_id):
            out.append(slot_id)
    if mode == "single" and len(out) > 1:
        return out[:1]
    return out


def _primary_registration_slot_id(slot_ids: list[int]) -> Optional[int]:
    return int(slot_ids[0]) if slot_ids else None


def _show_payment_mode(show: Any) -> str:
    if not show:
        return "stripe"
    value = (show["payment_mode"] if "payment_mode" in show.keys() else "stripe") or "stripe"
    value = str(value).strip().lower()
    return value if value in {"stripe", "external", "none"} else "stripe"


def _show_payment_reference(show: Any) -> str:
    try:
        value = (show["payment_reference"] or "").strip()
    except Exception:
        value = ""
    return value or show_reference(int(show["id"]))


def _stripe_payment_fields(show: Any, item_type: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    reference = _show_payment_reference(show)
    try:
        organizer_id = str(show["organizer_id"] or "")
        platform_fee_percent = str(float(show["platform_fee_percent"] or 0))
        processing_fee_percent = str(float(show["processing_fee_percent"] or 0))
        processing_fee_fixed_cents = str(int(show["processing_fee_fixed_cents"] or 0))
        collection_mode = (show["payment_collection_mode"] or "platform").strip()
    except Exception:
        organizer_id = ""
        platform_fee_percent = "0"
        processing_fee_percent = "0"
        processing_fee_fixed_cents = "0"
        collection_mode = "platform"
    metadata = {
        "payment_item_type": item_type,
        "show_id": str(show["id"]),
        "show_slug": show["slug"],
        "show_reference": reference,
        "organizer_id": organizer_id,
        "collection_mode": collection_mode,
        "platform_fee_percent": platform_fee_percent,
        "processing_fee_percent": processing_fee_percent,
        "processing_fee_fixed_cents": processing_fee_fixed_cents,
    }
    for key, value in (extra or {}).items():
        metadata[str(key)] = str(value)
    description = f"{reference} | {show['title']} | {item_type.replace('_', ' ').title()}"
    return {
        "client_reference_id": f"{reference}:{item_type}"[:200],
        "metadata": metadata,
        "payment_intent_data": {
            "description": description[:500],
            "metadata": metadata,
        },
    }


def _stripe_value(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _sync_actual_stripe_fee(checkout_session_id: str) -> Dict[str, Any]:
    _require_platform_stripe()
    checkout = stripe.checkout.Session.retrieve(checkout_session_id)
    metadata = _stripe_value(checkout, "metadata", {}) or {}
    show_id = int(metadata.get("show_id", "0") or 0)
    if not show_id:
        raise ValueError("Stripe checkout is missing show_id metadata.")
    payment_intent_ref = _stripe_value(checkout, "payment_intent", "")
    payment_intent_id = _stripe_value(payment_intent_ref, "id", payment_intent_ref) or ""
    if not payment_intent_id:
        raise ValueError("Stripe checkout has no payment intent yet.")
    payment_intent = stripe.PaymentIntent.retrieve(
        payment_intent_id,
        expand=["latest_charge.balance_transaction"],
    )
    charge_ref = _stripe_value(payment_intent, "latest_charge")
    if not charge_ref:
        raise ValueError("Stripe payment intent has no charge yet.")
    charge = charge_ref
    if isinstance(charge_ref, str):
        charge = stripe.Charge.retrieve(charge_ref, expand=["balance_transaction"])
    balance_ref = _stripe_value(charge, "balance_transaction")
    if not balance_ref:
        raise ValueError("Stripe charge has no balance transaction yet.")
    balance = balance_ref if not isinstance(balance_ref, str) else stripe.BalanceTransaction.retrieve(balance_ref)
    balance_id = _stripe_value(balance, "id", balance_ref if isinstance(balance_ref, str) else "")
    gross_cents = int(_stripe_value(checkout, "amount_total", 0) or _stripe_value(balance, "amount", 0) or 0)
    processing_fee_cents = int(_stripe_value(balance, "fee", 0) or 0)
    net_cents = int(_stripe_value(balance, "net", gross_cents - processing_fee_cents) or 0)
    organizer_raw = metadata.get("organizer_id", "")
    upsert_platform_payment_record(
        _db_path(),
        {
            "show_id": show_id,
            "organizer_id": int(organizer_raw) if str(organizer_raw).isdigit() else None,
            "item_type": metadata.get("payment_item_type", "payment"),
            "checkout_session_id": checkout_session_id,
            "payment_intent_id": payment_intent_id,
            "balance_transaction_id": balance_id,
            "gross_amount_cents": gross_cents,
            "processing_fee_cents": processing_fee_cents,
            "net_amount_cents": net_cents,
        },
    )
    return {"show_id": show_id, "fee_cents": processing_fee_cents, "balance_transaction_id": balance_id}


def _show_voting_mode(show: Any) -> str:
    if not show:
        return "fundraiser_unlimited"
    value = (show["voting_mode"] if "voting_mode" in show.keys() else "fundraiser_unlimited") or "fundraiser_unlimited"
    value = str(value).strip().lower()
    return value if value in {
        "fundraiser_unlimited", "restricted_single", "participant_restricted",
        "participant_only", "judge_only", "none",
    } else "fundraiser_unlimited"


def _show_voting_method(show: Any) -> str:
    value = (show["voting_method"] if show and "voting_method" in show.keys() else "qr_only") or "qr_only"
    return normalize_voting_method(value, default="qr_only")


def _vote_method_session_key(show_id: int) -> str:
    return f"car_number_vote_confirmed_{int(show_id)}"


def _show_allows_vote_entry_method(show: Any, entry_method: str) -> bool:
    method = _show_voting_method(show)
    entry = "car_number" if (entry_method or "").strip().lower() == "car_number" else "car_qr"
    if method == "disabled":
        return False
    if entry == "car_number":
        return method in {"both", "number_only"}
    return method in {"both", "qr_only"}


def _vote_car_is_publicly_eligible(car: Any) -> bool:
    if not car:
        return False
    if int(car["is_placeholder"] or 0) == 1:
        return False
    payment_status = str(car["registration_payment_status"] or "").strip().lower()
    registration_state = str(car["registration_state"] or "").strip().lower()
    blocked = {"removed", "canceled", "cancelled", "refunded", "inactive"}
    if payment_status in blocked or registration_state in blocked:
        return False
    return bool(str(car["checked_in_at"] or "").strip())


def _car_public_photo_url(car: Any) -> str:
    if not car:
        return ""
    for key in ("photo_url", "photo_path", "photo_filename", "vehicle_photo_url", "vehicle_image_path"):
        if key in car.keys() and car[key]:
            value = str(car[key]).strip()
            if not value:
                continue
            if key == "photo_filename" and not value.startswith(("http://", "https://", "/")):
                return url_for("static", filename=f"uploads/{value}")
            return value
    return ""


def _show_participant_voting(show: Any) -> bool:
    return _show_voting_mode(show) in {"participant_restricted", "participant_only", "judge_only"}


def _restricted_voter_allowed(show: Any, voter_type: str) -> bool:
    mode = _show_voting_mode(show)
    voter_type = (voter_type or "").strip().lower()
    if mode == "participant_only":
        return voter_type == "participant"
    if mode == "judge_only":
        return voter_type == "judge"
    return mode == "participant_restricted" and voter_type in {"participant", "judge"}


def _voter_session_key(show_id: int) -> str:
    return f"restricted_voter_token_{int(show_id)}"


def _active_restricted_voter(show: Any):
    if not show:
        return None
    token = session.get(_voter_session_key(int(show["id"])), "")
    if not token:
        return None
    return get_show_voter_by_token(int(show["id"]), token)


def _participant_category_keys() -> list[str]:
    return list(CATEGORY_SLUGS.keys())


def _participant_category_name(category_key: str) -> str:
    return CATEGORY_SLUGS.get(category_key, category_key)


def _show_voting_disabled(show: Any) -> bool:
    """True for cruise-ins or events where voting is explicitly disabled."""
    if not show:
        return True
    try:
        if int(show["voting_open"] or 0) != 1:
            return True
    except Exception:
        return True
    if _show_voting_mode(show) == "none":
        return True
    if _show_voting_method(show) == "disabled":
        return True
    if _show_payment_mode(show) == "none" and _show_voting_mode(show) not in {"participant_restricted", "restricted_single"}:
        return True
    try:
        return str(show["show_type"] or "").strip().lower() == "cruise_in"
    except Exception:
        return False




def sponsorship_allowed(show: Any) -> bool:
    """True when a show should display public sponsorship links/forms."""
    if not show:
        return False
    try:
        return int(show["allow_sponsorships"] if "allow_sponsorships" in show.keys() else 1) == 1
    except Exception:
        try:
            return int(getattr(show, "allow_sponsorships", 1) or 1) == 1
        except Exception:
            return True

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
    
def _show_external_payment_label(show: Any) -> str:
    if not show:
        return "Charity payment"
    try:
        value = (show["charity_processor_label"] or "").strip()
    except Exception:
        value = ""
    return value or "Charity payment"


def _show_external_payment_url(show: Any) -> str:
    if not show:
        return ""
    try:
        return (show["external_payment_url"] or "").strip()
    except Exception:
        return ""


def _show_external_payment_notice(show: Any) -> str:
    label = _show_external_payment_label(show)
    return f"Vote payment will be completed through {label}. Votes will count after payment review."    

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
    stamp = datetime.now(LOCAL_TZ).strftime("%Y%m%d-%H%M%S")
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


def _csrf_token() -> str:
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def _valid_csrf() -> bool:
    expected = session.get("_csrf_token", "")
    supplied = request.form.get("_csrf_token", "") or request.headers.get("X-CSRF-Token", "")
    return bool(expected and supplied and hmac.compare_digest(str(expected), str(supplied)))


@app.before_request
def security_before_request():
    session.permanent = True
    if not _same_origin_allowed():
        abort(400, "Blocked request origin.")
    if (
        request.method in {"POST", "PUT", "PATCH", "DELETE"}
        and request.endpoint != "stripe_webhook"
        and request.path.startswith("/admin")
        and not _valid_csrf()
    ):
        abort(400, "Security token expired. Refresh the page and try again.")
    _maybe_auto_close_voting()


@app.after_request
def security_headers(response):
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    if not IS_DEV:
        response.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
    return response


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
    charity_opt_in: bool,
    waiver_text: str,
    waiver_version: str,
    signed_name: str,
    intent_token: str,
    request_path: str,
    ip_address: str,
    user_agent: str,
) -> str:
    now_local = datetime.now(LOCAL_TZ)
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
<div><strong>Charity Information:</strong> {'Yes' if charity_opt_in else 'No'}</div>
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
    charity_opt_in: bool,
    waiver_text: str,
    waiver_version: str,
    signed_name: str,
    intent_token: str,
    html_path: str,
) -> None:
    now_local = datetime.now(LOCAL_TZ).isoformat()
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
        charity_opt_in=charity_opt_in,
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
        show_id = int(ri["show_id"])
        slot_ids = []

        if "registration_slot_ids" in ri.keys() and ri["registration_slot_ids"]:
            try:
                slot_ids = __import__("json").loads(ri["registration_slot_ids"] or "[]")
            except Exception:
                slot_ids = []

        if not slot_ids and "registration_slot_id" in ri.keys() and ri["registration_slot_id"]:
            slot_ids = [int(ri["registration_slot_id"])]

        registration_slot_id = int(slot_ids[0]) if slot_ids else None
        sc = cur.execute("SELECT * FROM show_cars WHERE id = ? LIMIT 1", (show_car_id,)).fetchone()

        if not sc:
            raise ValueError("Placeholder car not found.")
        person_id = int(sc["person_id"])

        cur.execute(
            """
            UPDATE people
            SET name = ?, phone = ?, email = ?, opt_in_future = ?, sponsor_opt_in = ?, charity_opt_in = ?, consent_text = ?, consent_version = ?
            WHERE id = ?
            """,
            (
                ri["owner_name"],
                ri["phone"],
                ri["email"],
                int(ri["opt_in_future"] or 0),
                int(ri["sponsor_opt_in"] or 0),
                int(ri["charity_opt_in"] or 0),
                CONSENT_TEXT_CAR_OWNER,
                CONSENT_VERSION,
                person_id,
            ),
        )

        class_id, class_needs_review = _auto_class_for_vehicle(show_id, ri["year"], ri["make"], ri["model"])

        cur.execute(
            """
            UPDATE show_cars
            SET year = ?,
                make = ?,
                model = ?,
                judging_class_id = ?,
                class_needs_review = ?,
                insurance_carrier = ?,
                registration_slot_id = ?,
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
                registration_state = 'claimed / paid'
            WHERE id = ?
            """,
            (
                ri["year"],
                ri["make"],
                ri["model"],
                class_id,
                int(class_needs_review),
                ri["insurance_carrier"] if "insurance_carrier" in ri.keys() else "",
                registration_slot_id,
                int(ri["amount_cents"] or 0),
                stripe_session_id,
                ri["waiver_signed_name"],
                ri["waiver_version"],
                show_car_id,
            ),
        )
        cur.execute("DELETE FROM show_car_registration_slots WHERE show_car_id = ?", (show_car_id,))
        for slot_id in slot_ids:
            cur.execute(
                """
                INSERT OR IGNORE INTO show_car_registration_slots (show_id, show_car_id, registration_slot_id)
                VALUES (?, ?, ?)
                """,
                (show_id, show_car_id, int(slot_id)),
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


def _finalize_placeholder_claim_cash(*, intent_token: str, show_car_id: int) -> Dict[str, Any]:
    """Finalize a placeholder card when registration is collected as cash/check at the show.
    This keeps the QR code/card usable for voting and check-in while clearly marking payment as cash_pending.
    """
    conn = _conn_direct()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        ri = cur.execute("SELECT * FROM registration_intents WHERE intent_token = ? LIMIT 1", (intent_token,)).fetchone()
        if not ri:
            raise ValueError("Registration intent not found.")
        if ri["finalized_show_car_id"]:
            sc = cur.execute("SELECT * FROM show_cars WHERE id = ? LIMIT 1", (int(ri["finalized_show_car_id"]),)).fetchone()
            conn.commit()
            return {"show_car_id": int(ri["finalized_show_car_id"]), "car_token": sc["car_token"] if sc else None, "already_finalized": True}

        show_id = int(ri["show_id"])
        slot_ids = []

        if "registration_slot_ids" in ri.keys() and ri["registration_slot_ids"]:
            try:
                slot_ids = __import__("json").loads(ri["registration_slot_ids"] or "[]")
            except Exception:
                slot_ids = []

        if not slot_ids and "registration_slot_id" in ri.keys() and ri["registration_slot_id"]:
            slot_ids = [int(ri["registration_slot_id"])]

        registration_slot_id = int(slot_ids[0]) if slot_ids else None

        sc = cur.execute("SELECT * FROM show_cars WHERE id = ? LIMIT 1", (show_car_id,)).fetchone()
        if not sc:
            raise ValueError("Placeholder car not found.")
        if int(sc["is_placeholder"] or 0) != 1:
            raise ValueError("This car number has already been assigned.")

        person_id = int(sc["person_id"])
        cur.execute(
            """
            UPDATE people
            SET name = ?, phone = ?, email = ?, opt_in_future = ?, sponsor_opt_in = ?, charity_opt_in = ?, consent_text = ?, consent_version = ?
            WHERE id = ?
            """,
            (ri["owner_name"], ri["phone"], ri["email"], int(ri["opt_in_future"] or 0), int(ri["sponsor_opt_in"] or 0), int(ri["charity_opt_in"] or 0), CONSENT_TEXT_CAR_OWNER, CONSENT_VERSION, person_id),
        )

        class_id, class_needs_review = _auto_class_for_vehicle(show_id, ri["year"], ri["make"], ri["model"])

        cur.execute(
            """
            UPDATE show_cars
            SET year = ?,
                make = ?,
                model = ?,
                judging_class_id = ?,
                class_needs_review = ?,
                insurance_carrier = ?,
                registration_slot_id = ?,
                registration_payment_status = 'paid_cash',
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
                registration_state = 'claimed / paid cash'
            WHERE id = ?
            """,
            (
                ri["year"],
                ri["make"],
                ri["model"],
                class_id,
                int(class_needs_review),
                ri["insurance_carrier"] if "insurance_carrier" in ri.keys() else "",
                registration_slot_id,
                int(ri["amount_cents"] or 0),
                f"cash_claim_{intent_token}",
                ri["waiver_signed_name"],
                ri["waiver_version"],
                ri["waiver_text"],
                ri["waiver_text_sha256"],
                ri["waiver_template_id"] if "waiver_template_id" in ri.keys() else None,
                show_car_id,
            ),
        )

        cur.execute("DELETE FROM show_car_registration_slots WHERE show_car_id = ?", (show_car_id,))
        for slot_id in slot_ids:
            cur.execute(
                """
                INSERT OR IGNORE INTO show_car_registration_slots (show_id, show_car_id, registration_slot_id)
                VALUES (?, ?, ?)
                """,
                (show_id, show_car_id, int(slot_id)),
            )

        cur.execute(
            "UPDATE registration_intents SET payment_status = 'paid_cash', finalized_show_car_id = ? WHERE id = ?",
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

def _current_admin_user_id() -> Optional[int]:
    raw = session.get("admin_user_id")
    try:
        return int(raw) if raw is not None else None
    except Exception:
        return None


def _current_admin_role() -> str:
    if not session.get("admin_authed"):
        return ""
    role = (session.get("admin_role") or "").strip().lower()
    return role or "super_admin"


def _admin_is_super() -> bool:
    return bool(session.get("admin_authed")) and _current_admin_role() == "super_admin"


def _current_admin_label() -> str:
    return session.get("admin_name") or session.get("admin_email") or ("Super Admin" if _admin_is_super() else "Admin")


def _admin_allowed_show_ids() -> list[int]:
    if _admin_is_super():
        return []
    admin_user_id = _current_admin_user_id()
    if not admin_user_id:
        return []
    try:
        return list_show_ids_for_admin_user(admin_user_id)
    except Exception:
        return []


def _admin_can_access_show(show_id: int) -> bool:
    if _admin_is_super():
        return True
    admin_user_id = _current_admin_user_id()
    if not admin_user_id:
        return False
    try:
        return admin_user_can_access_show(admin_user_id, int(show_id))
    except Exception:
        return False


def _require_show_access(show_id: int) -> None:
    if not _admin_can_access_show(int(show_id)):
        abort(403, "You do not have access to this show.")


def _admin_roles_for_show(show_id: int) -> set[str]:
    if _admin_is_super():
        return {"super_admin"}
    admin_user_id = _current_admin_user_id()
    if not admin_user_id:
        return set()
    return {
        (row["role"] or "").strip().lower()
        for row in list_admin_user_show_roles(admin_user_id)
        if int(row["show_id"]) == int(show_id)
    }


def _require_show_permission(show_id: int, allowed_roles: set[str]) -> None:
    _require_show_access(show_id)
    if _admin_is_super():
        return
    if not (_admin_roles_for_show(show_id) & allowed_roles):
        abort(403, "Your role does not allow this action.")


def require_super_admin(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("admin_authed"):
            return redirect(url_for("admin_page", next=request.path))
        if not _admin_is_super():
            abort(403, "Super admin access required.")
        return view_func(*args, **kwargs)
    return wrapped


def _admin_visible_shows():
    return list_shows_admin_for_user(_current_admin_user_id(), _admin_is_super())


def _admin_current_show():
    """Return active show for super admin, or the first accessible active/upcoming show for scoped users."""
    if _admin_is_super():
        return get_active_show()

    shows = _admin_visible_shows()
    if not shows:
        return None

    active = get_active_show()
    if active and any(int(s["id"]) == int(active["id"]) for s in shows):
        return active

    return shows[0]



def _maybe_auto_close_voting() -> None:
    end_raw = os.getenv("VOTING_END", "").strip()
    if not end_raw:
        return
    show = get_active_show()
    if not show or int(show["voting_open"]) != 1:
        return
    try:
        end_dt = datetime.strptime(end_raw, "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL_TZ)
        if datetime.now(LOCAL_TZ) >= end_dt:
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
    stamp = datetime.now(LOCAL_TZ).strftime("%Y%m%d-%H%M%S")
    final_name = f"sponsor-{stamp}-{filename}"
    out_path = upload_dir / final_name
    file_storage.save(out_path)
    return f"img/sponsors/uploads/{final_name}"

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _send_system_email(*, subject: str, body: str, reply_to: str = "") -> tuple[bool, str]:
    """Send a system notification email.

    The contact form always saves the message first. Email is only a notification layer.
    Set EMAIL_ENABLED=false in Railway to skip SMTP entirely while still saving messages.
    """
    if not _env_bool("EMAIL_ENABLED", True):
        msg = "Email notifications are disabled by EMAIL_ENABLED=false."
        app.logger.info("%s Subject=%s", msg, subject)
        return False, msg

    smtp_host = (os.getenv("MAIL_SERVER", "").strip() or os.getenv("SMTP_HOST", "").strip())
    smtp_port_raw = (os.getenv("MAIL_PORT", "").strip() or os.getenv("SMTP_PORT", "587").strip() or "587")
    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        smtp_port = 587

    timeout_raw = os.getenv("MAIL_TIMEOUT", os.getenv("SMTP_TIMEOUT", "5")).strip()
    try:
        timeout = max(1, min(int(timeout_raw), 10))
    except ValueError:
        timeout = 5

    smtp_username = (os.getenv("MAIL_USERNAME", "").strip() or os.getenv("SMTP_USERNAME", "").strip())
    smtp_password = (os.getenv("MAIL_PASSWORD", "").strip() or os.getenv("SMTP_PASSWORD", "").strip())
    smtp_from = (
        os.getenv("MAIL_FROM", "").strip()
        or os.getenv("SMTP_FROM_EMAIL", "").strip()
        or smtp_username
        or "info@karmankarshowsandevents.com"
    )
    target = (
        os.getenv("CONTACT_EMAIL", "").strip()
        or os.getenv("MAIL_TO", "").strip()
        or "info@karmankarshowsandevents.com"
    )

    use_ssl = _env_bool("MAIL_USE_SSL", _env_bool("SMTP_USE_SSL", False))
    use_tls = _env_bool("MAIL_USE_TLS", _env_bool("SMTP_USE_TLS", not use_ssl))

    if not smtp_host or not smtp_username or not smtp_password:
        msg = "SMTP is not configured; email was not sent."
        app.logger.warning("%s Subject=%s", msg, subject)
        return False, msg

    email_msg = EmailMessage()
    email_msg["Subject"] = subject
    email_msg["From"] = smtp_from
    email_msg["To"] = target
    if reply_to:
        email_msg["Reply-To"] = reply_to
    email_msg.set_content(body)

    try:
        app.logger.info("Sending system email via %s:%s ssl=%s tls=%s timeout=%ss", smtp_host, smtp_port, use_ssl, use_tls, timeout)
        if use_ssl:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=timeout) as server:
                server.login(smtp_username, smtp_password)
                server.send_message(email_msg)
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=timeout) as server:
                if use_tls:
                    server.starttls()
                server.login(smtp_username, smtp_password)
                server.send_message(email_msg)
        return True, ""
    except Exception as exc:
        app.logger.exception("Failed to send system email.")
        return False, str(exc)


@app.context_processor
def inject_globals():
    show = _get_public_active_show()
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
        "current_admin_role": _current_admin_role(),
        "current_admin_label": _current_admin_label(),
        "admin_is_super": _admin_is_super(),
        "csrf_token": _csrf_token(),
        "prereg_allowed": prereg_allowed,
        "sponsorship_allowed": sponsorship_allowed,
        "registered_cars": registered_cars,
        "active_show_date_status": _event_date_status(show),
    }


@app.get("/")
def home():
    show = _get_public_active_show()
    return render_template("home.html", show=show, event_date_status=_event_date_status(show))

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
    if upcoming_show and _event_date_status(upcoming_show)["is_past"]:
        upcoming_show = None
    if not upcoming_show:
        active_show = get_active_show()
        if active_show and not _event_date_status(active_show)["is_past"]:
            upcoming_show = active_show

    return render_template(
        "events.html",
        show=get_active_show(),
        upcoming_show=upcoming_show,
        event_date_status=_event_date_status(upcoming_show),
    )
    
@app.get("/contact")
def contact_page():
    return render_template("contact.html")

@app.post("/contact")
@rate_limit("contact_submit", 10, 300)
def contact_submit():
    name = request.form.get("name", "").strip()
    email = request.form.get("email", "").strip()
    phone = request.form.get("phone", "").strip()
    subject = request.form.get("subject", "").strip()
    body = request.form.get("body", "").strip()

    if not (name and email and subject and body):
        flash("Please complete the required contact form fields.", "error")
        return redirect(url_for("contact_page"))

    message_id = create_contact_message(
        name=name,
        email=email,
        phone=phone,
        subject=subject,
        message=body,
        source_page=request.referrer or "contact",
    )

    email_body = (
        f"New Contact Us message received.\n\n"
        f"Message ID: {message_id}\n"
        f"Name: {name}\n"
        f"Email: {email}\n"
        f"Phone: {phone or 'Not provided'}\n"
        f"Subject: {subject}\n\n"
        f"Message:\n{body}\n\n"
        f"View in admin: {_abs_url(url_for('admin_contact_messages'))}"
    )
    sent, error = _send_system_email(
        subject=f"Karman Kar Contact: {subject}",
        body=email_body,
        reply_to=email,
    )
    mark_contact_message_email_result(message_id, sent=sent, error=error)

    flash("Thank you. Your message has been received.", "ok")
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
        registration_slots=_registration_slots_for_public(int(show["id"])),
        event_date_status=_event_date_status(show),
    )


@app.get("/show/<slug>/calendar.ics")
def show_calendar(slug: str):
    show = get_show_by_slug(slug)
    if not show:
        return "Show not found.", 404
    status = _event_date_status(show)
    if not status.get("parsed"):
        return "The event date is not calendar-compatible yet.", 400
    start = datetime.strptime(status["date"], "%Y-%m-%d").date()
    end = start + timedelta(days=1)
    def ics(value: str) -> str:
        return str(value or "").replace("\\", "\\\\").replace(",", "\\,").replace(";", "\\;").replace("\n", "\\n")
    payload = "\r\n".join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Karman Kar Shows//Event Calendar//EN",
        "BEGIN:VEVENT",
        f"UID:{show['slug']}-{show['id']}@karmankarshowsandevents.com",
        f"DTSTART;VALUE=DATE:{start.strftime('%Y%m%d')}",
        f"DTEND;VALUE=DATE:{end.strftime('%Y%m%d')}",
        f"SUMMARY:{ics(show['title'])}",
        f"LOCATION:{ics(show['address'] or show['location_name'])}",
        f"DESCRIPTION:{ics(show['description'])}",
        "END:VEVENT",
        "END:VCALENDAR",
        "",
    ])
    return app.response_class(
        payload,
        mimetype="text/calendar",
        headers={"Content-Disposition": f"attachment; filename={show['slug']}.ics"},
    )

def _registration_availability(show: Any) -> Dict[str, Any]:
    """Return public registration availability for one show.

    This keeps the /register picker, direct /register/<slug> page, and final
    POST protection using the same rules.
    """
    if not show:
        return {
            "can_register": False,
            "status": "closed",
            "label": "Registration unavailable",
            "message": "This show is not available.",
        }

    if not prereg_allowed(show):
        return {
            "can_register": False,
            "status": "closed",
            "label": "Registration closed",
            "message": "Pre-registration has been closed by the event administrator or this event is using a day-of registration workflow.",
        }

    show_id = int(show["id"])
    slots = _registration_slots_for_public(show_id)
    if slots:
        open_slots = [slot for slot in slots if int(slot["is_full"] or 0) != 1]
        if not open_slots:
            return {
                "can_register": False,
                "status": "full",
                "label": "Registration full",
                "message": "All available registration days/sessions for this show are full.",
            }
        return {
            "can_register": True,
            "status": "open",
            "label": "Register",
            "message": "Pre-registration is open.",
        }

    if not show_has_capacity(show_id):
        return {
            "can_register": False,
            "status": "full",
            "label": "Registration full",
            "message": "This show has reached its maximum number of cars.",
        }

    return {
        "can_register": True,
        "status": "open",
        "label": "Register",
        "message": "Pre-registration is open.",
    }


def _registration_show_or_response(show_slug: Optional[str] = None):
    """Return the selected show for registration, or a Flask response tuple.

    Public registration should be locked to a show slug whenever possible so
    multiple upcoming shows can accept registrations at the same time without
    depending on whichever show is currently active.
    """
    if show_slug:
        show = get_show_by_slug(show_slug)
        if not show:
            return None, (render_template("registration_closed.html", show=None, error="That show was not found."), 404)
    else:
        show = get_active_show()
        if not show:
            return None, ("No active show configured.", 500)
    show = _show_with_rendered_waiver(show)
    availability = _registration_availability(show)
    if not availability["can_register"]:
        return None, (
            render_template(
                "registration_closed.html",
                show=show,
                error=availability["message"],
                registration_status=availability["status"],
            ),
            403,
        )
    return show, None


def _public_registration_show_cards() -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for row in list_public_registerable_shows():
        show_dict = {k: row[k] for k in row.keys()}
        availability = _registration_availability(row)
        show_dict["registration_availability"] = availability
        show_dict["can_register"] = bool(availability["can_register"])
        show_dict["registration_status"] = availability["status"]
        show_dict["registration_label"] = availability["label"]
        show_dict["registration_message"] = availability["message"]
        cards.append(show_dict)
    return cards


@app.get("/register")
def register_page():
    """Public registration picker.

    When more than one show is active/upcoming, the top navigation Register button
    should not send visitors straight into whichever show is marked active. This
    page lets the visitor pick the correct event without scrolling through all
    show details.
    """
    shows = _public_registration_show_cards()
    return render_template("register_picker.html", shows=shows)


@app.get("/register/<show_slug>")
def register_page_for_show(show_slug: str):
    show, response = _registration_show_or_response(show_slug)
    if response:
        return response
    return render_template("register.html", show=show, registration_slots=_registration_slots_for_public(int(show["id"])))
    

@app.post("/register")
@rate_limit("register", 20, 300)
def register_submit():
    return _register_submit_impl(None)


@app.post("/register/<show_slug>")
@rate_limit("register", 20, 300)
def register_submit_for_show(show_slug: str):
    return _register_submit_impl(show_slug)


def _register_submit_impl(show_slug: Optional[str] = None):
    show, response = _registration_show_or_response(show_slug)
    if response:
        return response
    if not show:
        return "No active show configured.", 500
    if not prereg_allowed(show):
        return render_template("registration_closed.html", show=show), 403

    registration_slots = _registration_slots_for_public(int(show["id"]))
    registration_slot_ids = _selected_registration_slot_ids(show)
    registration_slot_id = _primary_registration_slot_id(registration_slot_ids)
    if registration_slots:
        if not registration_slot_ids:
            return render_template("register.html", show=show, registration_slots=registration_slots, error="Please select which day/session/activity you are registering for.")
        for selected_slot_id in registration_slot_ids:
            if not show_slot_has_capacity(int(show["id"]), selected_slot_id):
                return render_template("register.html", show=show, registration_slots=registration_slots, error="One of the selected days/sessions is full.")
    elif not show_has_capacity(int(show["id"])):
        return render_template("register.html", show=show, registration_slots=registration_slots, error="This show has reached its maximum number of cars.")

    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip().lower()
    opt_in_future = request.form.get("opt_in_future", "") == "on"
    sponsor_opt_in = request.form.get("sponsor_opt_in", "") == "on"
    charity_opt_in = request.form.get("charity_opt_in", "") == "on"
    year = request.form.get("year", "").strip()
    make = request.form.get("make", "").strip()
    model = request.form.get("model", "").strip()
    insurance_carrier = request.form.get("insurance_carrier", "").strip()
    waiver_accepted = request.form.get("waiver_accepted", "") == "on"
    waiver_signed_name = request.form.get("waiver_signed_name", "").strip()
    registration_payment_method = request.form.get("registration_payment_method", "card").strip().lower()
    if registration_payment_method not in {"card", "cash"}:
        registration_payment_method = "card"

    if not (name and year and make and model and waiver_signed_name):
        return render_template("register.html", show=show, registration_slots=registration_slots, error="Please fill out all required fields.")
    if (opt_in_future or sponsor_opt_in or charity_opt_in) and not (phone or email):
        return render_template("register.html", show=show, registration_slots=registration_slots, error="Provide a phone number or email address when selecting contact permissions.")
    if not waiver_accepted:
        return render_template("register.html", show=show, registration_slots=registration_slots, error="You must accept the waiver to continue.")

    registration_fee_cents = int(show["registration_fee_cents"] or 0)
    waiver_text = (show.get("waiver_text") or "").strip()
    waiver_version = (show.get("waiver_version") or "").strip()
    if not waiver_text or not waiver_version:
        return render_template("register.html", show=show, registration_slots=registration_slots, error="Registration is temporarily unavailable because the event waiver has not been configured.")
    waiver_template_id = int(show["waiver_template_id"]) if show.get("waiver_template_id") else None

    try:
        registration_intent_id, intent_token, assigned_car_number = create_registration_intent(
            show_id=int(show["id"]),
            owner_name=name,
            phone=phone,
            email=email,
            opt_in_future=opt_in_future,
            sponsor_opt_in=sponsor_opt_in,
            charity_opt_in=charity_opt_in,
            year=year,
            make=make,
            model=model,
            insurance_carrier=insurance_carrier,
            waiver_accepted=waiver_accepted,
            waiver_signed_name=waiver_signed_name,
            waiver_text=waiver_text,
            waiver_version=waiver_version,
            amount_cents=registration_fee_cents,
            waiver_template_id=waiver_template_id,
            registration_slot_id=registration_slot_id,
            registration_slot_ids=registration_slot_ids,
        )
    except ValueError as e:
        return render_template("register.html", show=show, registration_slots=registration_slots, error=str(e))

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
        charity_opt_in=charity_opt_in,
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
        charity_opt_in=charity_opt_in,
        waiver_text=waiver_text,
        waiver_version=waiver_version,
        signed_name=waiver_signed_name,
        intent_token=intent_token,
        html_path=html_path,
    )

    if registration_fee_cents > 0 and registration_payment_method == "cash":
        synthetic_session_id = f"cash_reg_{intent_token}"
        attach_stripe_session_to_registration_intent(
            registration_intent_id,
            synthetic_session_id,
            stripe_payment_intent_id="paid_cash",
        )
        result = finalize_registration_intent_paid(synthetic_session_id)
        conn = _conn_direct()
        try:
            conn.execute(
                """
                UPDATE show_cars
                SET registration_payment_status = 'paid_cash', registration_state = 'claimed / paid cash'
                WHERE id = ?
                """,
                (int(result["show_car_id"]),),
            )
            conn.execute("UPDATE registration_intents SET payment_status = 'paid_cash' WHERE id = ?", (registration_intent_id,))
            conn.commit()
        finally:
            conn.close()
        car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
        _log_event("registration.cash_finalized", int(show["id"]), {"car_number": assigned_car_number, "registration_intent_id": registration_intent_id}, actor_type="public")
        return render_template("register_success.html", show=show, car=car)

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
    cancel_url = _abs_url(url_for("register_page_for_show", show_slug=show["slug"]))

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

@app.get("/claim/<show_slug>/<car_token>")
def placeholder_claim_page(show_slug: str, car_token: str):
    show = _show_with_rendered_waiver(get_show_by_slug(show_slug))
    if not show:
        return "Show not found.", 404

    car = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404

    # If the QR code is scanned after the placeholder has been claimed,
    # keep the windshield card useful instead of showing an error.
    if int(car["is_placeholder"] or 0) != 1 or str(car["registration_state"] or "").lower() != "placeholder":
        return redirect(url_for("car_card", slug=show_slug, token=car_token))

    if str(car["registration_payment_status"] or "").lower() == "paid":
        return redirect(url_for("car_card", slug=show_slug, token=car_token))

    return render_template("placeholder_claim.html", show=show, car=car, registration_slots=_registration_slots_for_public(int(show["id"])))


@app.post("/claim/<show_slug>/<car_token>")
@rate_limit("claim", 20, 300)
def placeholder_claim_submit(show_slug: str, car_token: str):
    show = _show_with_rendered_waiver(get_show_by_slug(show_slug))
    if not show:
        return "Show not found.", 404
    car = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404
    if int(car["is_placeholder"] or 0) != 1:
        return "This car number has already been assigned.", 400

    if str(car["registration_state"] or "").lower() != "placeholder":
        return "This car has already been claimed.", 400

    if str(car["registration_payment_status"] or "").lower() == "paid":
        return "This car is already registered.", 400

    registration_slots = _registration_slots_for_public(int(show["id"]))
    registration_slot_ids = _selected_registration_slot_ids(show)
    registration_slot_id = _primary_registration_slot_id(registration_slot_ids)
    if registration_slots:
        if not registration_slot_ids:
            return render_template("placeholder_claim.html", show=show, car=car, registration_slots=registration_slots, error="Please select which day/session/activity you are registering for.")
        for selected_slot_id in registration_slot_ids:
            if not show_slot_has_capacity(int(show["id"]), selected_slot_id):
                return render_template("placeholder_claim.html", show=show, car=car, registration_slots=registration_slots, error="One of the selected days/sessions is full.")
    
    owner_name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip().lower()
    opt_in_future = request.form.get("opt_in_future", "") == "on"
    sponsor_opt_in = request.form.get("sponsor_opt_in", "") == "on"
    charity_opt_in = request.form.get("charity_opt_in", "") == "on"
    year = request.form.get("year", "").strip()
    make = request.form.get("make", "").strip()
    model = request.form.get("model", "").strip()
    insurance_carrier = request.form.get("insurance_carrier", "").strip()
    waiver_accepted = request.form.get("waiver_accepted", "") == "on"
    waiver_signed_name = request.form.get("waiver_signed_name", "").strip()
    # Placeholder cards are handed out after payment is collected at the booth.
    # Do not send day-of claimants to online checkout.
    registration_payment_method = "cash"

    if not (owner_name and year and make and model and waiver_signed_name):
        return render_template("placeholder_claim.html", show=show, car=car, registration_slots=registration_slots, error="Please fill out all required fields.")
    if (opt_in_future or sponsor_opt_in or charity_opt_in) and not (phone or email):
        return render_template("placeholder_claim.html", show=show, car=car, registration_slots=registration_slots, error="Provide a phone number or email address when selecting contact permissions.")
    if not waiver_accepted:
        return render_template("placeholder_claim.html", show=show, car=car, registration_slots=registration_slots, error="You must accept the waiver to continue.")

    car_number = int(car["car_number"])
    registration_fee_cents = int(show["registration_fee_cents"] or 0)
    waiver_text = (show.get("waiver_text") or "").strip()
    waiver_version = (show.get("waiver_version") or "").strip()
    if not waiver_text or not waiver_version:
        return render_template("placeholder_claim.html", show=show, car=car, registration_slots=registration_slots, error="Registration is temporarily unavailable because the event waiver has not been configured.")
    waiver_template_id = int(show["waiver_template_id"]) if show.get("waiver_template_id") else None

    try:
        registration_intent_id, intent_token, assigned_car_number = create_registration_intent(
            show_id=int(show["id"]),
            owner_name=owner_name,
            phone=phone,
            email=email,
            opt_in_future=opt_in_future,
            sponsor_opt_in=sponsor_opt_in,
            charity_opt_in=charity_opt_in,
            year=year,
            make=make,
            model=model,
            insurance_carrier=insurance_carrier,
            waiver_accepted=True,
            waiver_signed_name=waiver_signed_name,
            waiver_text=waiver_text,
            waiver_version=waiver_version,
            amount_cents=registration_fee_cents,
            waiver_template_id=waiver_template_id,
            reserved_car_number=car_number,
            registration_slot_id=registration_slot_id,
            registration_slot_ids=registration_slot_ids,
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
                    show_id, registration_slot_id, registration_slot_ids, intent_token, owner_name, phone, email, opt_in_future, sponsor_opt_in, charity_opt_in,
                    car_number, year, make, model, insurance_carrier,
                    waiver_accepted, waiver_signed_name, waiver_text, waiver_version, waiver_text_sha256,
                    waiver_template_id, amount_cents, payment_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    int(show["id"]),
                    int(registration_slot_id) if registration_slot_id else None,
                    __import__('json').dumps(registration_slot_ids),
                    intent_token,
                    owner_name,
                    phone,
                    email,
                    1 if opt_in_future else 0,
                    1 if sponsor_opt_in else 0,
                    1 if charity_opt_in else 0,
                    car_number,
                    year,
                    make,
                    model,
                    insurance_carrier,
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
        charity_opt_in=charity_opt_in,
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
        charity_opt_in=charity_opt_in,
        waiver_text=waiver_text,
        waiver_version=waiver_version,
        signed_name=waiver_signed_name,
        intent_token=intent_token,
        html_path=html_path,
    )

    if registration_fee_cents > 0 and registration_payment_method == "cash":
        result = _finalize_placeholder_claim_cash(intent_token=intent_token, show_car_id=int(car["id"]))
        final_car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
        _log_event("placeholder_claim.cash_finalized", int(show["id"]), {"car_number": assigned_car_number, "registration_intent_id": registration_intent_id}, actor_type="public")
        return render_template("placeholder_claim_success.html", show=show, car=final_car)

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
    insurance_carrier = request.form.get("insurance_carrier", "").strip()

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
    update_show_car_details(
        int(car_private["id"]),
        year=year,
        make=make,
        model=model,
        insurance_carrier=insurance_carrier,
    )
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
    charity_opt_in = request.form.get("charity_opt_in", "") == "on"

    if not first_name:
        first_name = "Guest"
    # Phone and email are encouraged, but attendance should still be counted when guests skip contact details.

    attendee_id = create_attendee(
        int(show["id"]),
        first_name,
        last_name,
        phone,
        email,
        zip_code,
        sponsor_opt_in,
        updates_opt_in,
        charity_opt_in,
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
    entry_method = "car_number" if request.args.get("entry_method", "").strip().lower() == "car_number" else "car_qr"
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    if category_slug not in CATEGORY_SLUGS:
        return "Invalid category.", 404

    car = get_show_car_public_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404

    if _show_voting_disabled(show):
        return render_template("voting_closed.html", show=show)
    if not _show_allows_vote_entry_method(show, entry_method):
        return render_template("voting_closed.html", show=show), 403
    if entry_method == "car_number":
        expected = session.get(_vote_method_session_key(int(show["id"])), {})
        if not isinstance(expected, dict) or expected.get("car_token") != car_token or expected.get("category_slug") != category_slug:
            return render_template("voting_closed.html", show=show), 403
    if not _show_participant_voting(show) and not _vote_car_is_publicly_eligible(car):
        return render_template("voting_closed.html", show=show), 403

    if _show_participant_voting(show):
        voter = _active_restricted_voter(show)
        if not voter or not _restricted_voter_allowed(show, voter["voter_type"] if voter else ""):
            return render_template(
                "restricted_vote_not_authorized.html",
                show=show,
                car=car,
                category_slug=category_slug,
                category_name=CATEGORY_SLUGS[category_slug],
            ), 403
        existing_vote = get_restricted_vote(int(show["id"]), int(voter["id"]), category_slug)
        progress = restricted_vote_progress(int(show["id"]), int(voter["id"]), _participant_category_keys())
        return render_template(
            "restricted_vote.html",
            show=show,
            car=car,
            voter=voter,
            category_slug=category_slug,
            category_name=CATEGORY_SLUGS[category_slug],
            existing_vote=existing_vote,
            progress=progress,
            category_names=CATEGORY_SLUGS,
        )

    return render_template(
        "vote_qty.html",
        show=show,
        car=car,
        category_slug=category_slug,
        category_name=CATEGORY_SLUGS[category_slug],
        vote_price_cents=int(show["vote_price_cents"] or 100),
        payment_mode=_show_payment_mode(show),
        voting_mode=_show_voting_mode(show),
        voting_method=_show_voting_method(show),
        entry_method=entry_method,
        car_photo_url=_car_public_photo_url(car),
        preset_vote_options=_show_preset_vote_options(show),
        allow_custom_votes=_show_allow_custom_votes(show),
        max_votes_per_checkout=_show_max_votes_per_checkout(show),
    )


@app.route("/vote/<show_slug>", methods=["GET", "POST"])
@rate_limit("car_number_vote_lookup", 40, 300)
def car_number_vote_page(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    if _show_voting_disabled(show) or not _show_allows_vote_entry_method(show, "car_number"):
        return render_template("voting_closed.html", show=show), 403
    if _show_participant_voting(show):
        return render_template("voting_closed.html", show=show), 403

    selected_category = request.form.get("category_slug", request.args.get("category_slug", "peoples-choice")).strip()
    if selected_category not in CATEGORY_SLUGS:
        selected_category = "peoples-choice"

    lookup_result = None
    if request.method == "POST":
        car_number = request.form.get("car_number", "")
        lookup_result = find_vote_car_by_number(int(show["id"]), car_number)
        if lookup_result.get("status") == "ok":
            car = lookup_result["car"]
            session[_vote_method_session_key(int(show["id"]))] = {
                "car_token": car["car_token"],
                "category_slug": selected_category,
            }
            return render_template(
                "vote_car_number_confirm.html",
                show=show,
                car=car,
                category_slug=selected_category,
                category_name=CATEGORY_SLUGS[selected_category],
                car_photo_url=_car_public_photo_url(car),
            )

    return render_template(
        "vote_car_number_entry.html",
        show=show,
        categories=CATEGORY_SLUGS,
        selected_category=selected_category,
        lookup_result=lookup_result,
    )


@app.get("/vote-access/<show_slug>/<car_token>")
def participant_vote_access(show_slug: str, car_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    if not _show_participant_voting(show):
        return render_template("restricted_vote_not_authorized.html", show=show, car=None, category_slug="", category_name=""), 403
    if not _restricted_voter_allowed(show, "participant"):
        return render_template("restricted_vote_not_authorized.html", show=show, car=None, category_slug="", category_name=""), 403
    car = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car:
        return "Voting access not found.", 404
    if int(car["is_placeholder"] or 0) == 1:
        return render_template("restricted_vote_not_authorized.html", show=show, car=car, category_slug="", category_name=""), 403
    eligible_statuses = {"paid", "paid_cash", "manual_paid", "comped"}
    if str(car["registration_payment_status"] or "").strip().lower() not in eligible_statuses:
        return render_template("restricted_vote_not_authorized.html", show=show, car=car, category_slug="", category_name=""), 403
    voter = get_or_create_participant_voter(int(show["id"]), int(car["id"]))
    voter = activate_show_voter(int(show["id"]), voter["voter_token"])
    session[_voter_session_key(int(show["id"]))] = voter["voter_token"]
    progress = restricted_vote_progress(int(show["id"]), int(voter["id"]), _participant_category_keys())
    return render_template(
        "voter_activate.html",
        show=show,
        car=car,
        voter=voter,
        progress=progress,
        category_names=CATEGORY_SLUGS,
    )


@app.get("/judge-access/<show_slug>/<voter_token>")
def judge_vote_access(show_slug: str, voter_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    if not _show_participant_voting(show):
        return render_template("restricted_vote_not_authorized.html", show=show, car=None, category_slug="", category_name=""), 403
    if not _restricted_voter_allowed(show, "judge"):
        return render_template("restricted_vote_not_authorized.html", show=show, car=None, category_slug="", category_name=""), 403
    voter = activate_show_voter(int(show["id"]), voter_token)
    if not voter:
        return "Judge access not found or inactive.", 404
    session[_voter_session_key(int(show["id"]))] = voter["voter_token"]
    progress = restricted_vote_progress(int(show["id"]), int(voter["id"]), _participant_category_keys())
    return render_template(
        "voter_activate.html",
        show=show,
        car=None,
        voter=voter,
        progress=progress,
        category_names=CATEGORY_SLUGS,
    )


@app.post("/restricted-vote")
@rate_limit("restricted_vote", 60, 300)
def restricted_vote_submit():
    show_slug = request.form.get("show_slug", "").strip()
    car_token = request.form.get("car_token", "").strip()
    category_slug = request.form.get("category_slug", "").strip()

    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    if _show_voting_disabled(show) or not _show_participant_voting(show):
        return render_template("voting_closed.html", show=show), 403
    if category_slug not in CATEGORY_SLUGS:
        return "Invalid category.", 404

    voter = _active_restricted_voter(show)
    if not voter:
        return render_template("restricted_vote_not_authorized.html", show=show, car=None, category_slug=category_slug, category_name=CATEGORY_SLUGS[category_slug]), 403
    if not _restricted_voter_allowed(show, voter["voter_type"]):
        return render_template("restricted_vote_not_authorized.html", show=show, car=None, category_slug=category_slug, category_name=CATEGORY_SLUGS[category_slug]), 403

    car = get_show_car_public_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404

    if voter["voter_type"] == "participant" and voter["show_car_id"] and int(voter["show_car_id"]) == int(car["id"]):
        existing_vote = get_restricted_vote(int(show["id"]), int(voter["id"]), category_slug)
        progress = restricted_vote_progress(int(show["id"]), int(voter["id"]), _participant_category_keys())
        return render_template(
            "restricted_vote.html",
            show=show,
            car=car,
            voter=voter,
            category_slug=category_slug,
            category_name=CATEGORY_SLUGS[category_slug],
            existing_vote=existing_vote,
            progress=progress,
            category_names=CATEGORY_SLUGS,
            error="You cannot vote for your own vehicle in participant voting.",
        ), 400

    existing_vote = get_restricted_vote(int(show["id"]), int(voter["id"]), category_slug)
    if existing_vote and int(show["participant_vote_change_allowed"] or 0) != 1:
        return render_template(
            "restricted_vote.html",
            show=show,
            car=car,
            voter=voter,
            category_slug=category_slug,
            category_name=CATEGORY_SLUGS[category_slug],
            existing_vote=existing_vote,
            progress=restricted_vote_progress(int(show["id"]), int(voter["id"]), _participant_category_keys()),
            category_names=CATEGORY_SLUGS,
            error="Your vote for this category is already locked.",
        ), 409

    upsert_restricted_vote(
        int(show["id"]),
        int(voter["id"]),
        category_slug,
        int(car["id"]),
        int(car["judging_class_id"]) if "judging_class_id" in car.keys() and car["judging_class_id"] else None,
        1,
    )
    progress = restricted_vote_progress(int(show["id"]), int(voter["id"]), _participant_category_keys())
    if progress.get("is_complete"):
        return redirect(url_for("restricted_vote_complete", show_slug=show_slug))
    return render_template(
        "restricted_vote_success.html",
        show=show,
        car=car,
        voter=voter,
        category_slug=category_slug,
        category_name=CATEGORY_SLUGS[category_slug],
        progress=progress,
        category_names=CATEGORY_SLUGS,
    )


@app.get("/restricted-vote/<show_slug>/complete")
def restricted_vote_complete(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    voter = _active_restricted_voter(show)
    if not voter:
        return render_template("restricted_vote_not_authorized.html", show=show, car=None, category_slug="", category_name=""), 403
    progress = restricted_vote_progress(int(show["id"]), int(voter["id"]), _participant_category_keys())
    return render_template(
        "restricted_vote_complete.html",
        show=show,
        voter=voter,
        progress=progress,
        category_names=CATEGORY_SLUGS,
    )


@app.post("/create-checkout-session")
@rate_limit("vote_checkout", 25, 300)
def create_checkout_session():
    show_slug = request.form.get("show_slug", "").strip()
    car_token = request.form.get("car_token", "").strip()
    category_slug = request.form.get("category_slug", "").strip()
    entry_method = "car_number" if request.form.get("entry_method", "").strip().lower() == "car_number" else "car_qr"
    qty_raw = request.form.get("vote_qty", "1").strip()

    show = get_show_by_slug(show_slug)
    if not show:
        return jsonify({"ok": False, "error": "Show not found."}), 404

    if _show_voting_disabled(show):
        return jsonify({"ok": False, "error": "Voting is disabled or currently closed for this event."}), 403
    if not _show_allows_vote_entry_method(show, entry_method):
        return jsonify({"ok": False, "error": "This voting method is not enabled for this event."}), 403

    if category_slug not in CATEGORY_SLUGS:
        return jsonify({"ok": False, "error": "Invalid category."}), 400

    car = get_show_car_public_by_token(int(show["id"]), car_token)
    if not car:
        return jsonify({"ok": False, "error": "Car not found."}), 404
    if entry_method == "car_number":
        expected = session.get(_vote_method_session_key(int(show["id"])), {})
        if not isinstance(expected, dict) or expected.get("car_token") != car_token or expected.get("category_slug") != category_slug:
            return jsonify({"ok": False, "error": "Please confirm the car number before continuing."}), 403
    if not _vote_car_is_publicly_eligible(car):
        return jsonify({"ok": False, "error": "That car is not eligible for voting right now."}), 403

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
    if payment_mode == "none":
        return jsonify({"ok": False, "error": "Voting payments are disabled for this event."}), 403

    if payment_mode == "external":
        external_payment_url = _show_external_payment_url(show)
        if not external_payment_url:
            return jsonify({"ok": False, "error": "External payment link is not configured for this show."}), 400

        vote_intent_id = create_vote_intent(
            int(show["id"]),
            int(car["id"]),
            CATEGORY_SLUGS[category_slug],
            vote_qty,
            amount_cents,
            entry_method=entry_method,
        )

        conn = _conn_direct()
        try:
            conn.execute(
                "UPDATE vote_intents SET payment_status = 'pending_review' WHERE id = ?",
                (int(vote_intent_id),),
            )
            conn.commit()
        finally:
            conn.close()

        return jsonify({
            "ok": True,
            "payment_mode": "external",
            "redirect_url": url_for("external_vote_payment_page", vote_intent_id=vote_intent_id),
        })
        
    # LIVE FIX 2026-04-25:
    # Use the platform Stripe account for votes when payment_mode is "stripe".
    # The previous code required a connected charity Stripe account here, which blocked checkout.
    _require_platform_stripe()
    vote_intent_id = create_vote_intent(
        int(show["id"]),
        int(car["id"]),
        CATEGORY_SLUGS[category_slug],
        vote_qty,
        amount_cents,
        entry_method=entry_method,
    )

    success_url = _abs_url(url_for("vote_success")) + "?session_id={CHECKOUT_SESSION_ID}&show_slug=" + show_slug
    if entry_method == "car_number":
        cancel_url = _abs_url(url_for("vote_qty_page", show_slug=show_slug, car_token=car_token, category_slug=category_slug, entry_method="car_number"))
    else:
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
            "entry_method": entry_method,
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
    vote_intent = get_vote_intent(vote_intent_id)
    if not vote_intent:
        return "Vote request not found.", 404

    show = None
    car = None
    conn = _conn_direct()
    try:
        show = conn.execute(
            "SELECT * FROM shows WHERE id = ? LIMIT 1",
            (int(vote_intent["show_id"]),),
        ).fetchone()
        car = conn.execute(
            """
            SELECT sc.*, p.name AS owner_name
            FROM show_cars sc
            JOIN people p ON p.id = sc.person_id
            WHERE sc.id = ?
            LIMIT 1
            """,
            (int(vote_intent["show_car_id"]),),
        ).fetchone()
    finally:
        conn.close()

    return render_template(
        "external_vote_payment.html",
        show=show,
        vote_intent=vote_intent,
        car=car,
        external_payment_url=_show_external_payment_url(show),
        external_payment_label=_show_external_payment_label(show),
    )


@app.post("/vote/external/<int:vote_intent_id>/submitted")
@rate_limit("vote_external_submitted", 20, 300)
def external_vote_mark_submitted(vote_intent_id: int):
    vote_intent = get_vote_intent(vote_intent_id)
    if not vote_intent:
        return "Vote request not found.", 404

    conn = _conn_direct()
    try:
        conn.execute(
            """
            UPDATE vote_intents
            SET payment_status = 'pending_review'
            WHERE id = ?
            """,
            (int(vote_intent_id),),
        )
        conn.commit()
    finally:
        conn.close()

    return redirect(url_for("vote_success", pending=1))


@app.get("/success")
def vote_success():
    pending = request.args.get("pending", "").strip()
    if pending == "1":
        show = get_active_show()
        return render_template("vote_success.html", show=show)

    session_id = request.args.get("session_id", "").strip()
    show_slug = request.args.get("show_slug", "").strip()
    if not session_id:
        return "Missing session_id.", 400
        
    show = get_show_by_slug(show_slug) if show_slug else get_active_show()
    if not show:
        return "Show not found.", 404

    # LIVE FIX 2026-04-25:
    # Vote checkout is created on the platform Stripe account, so retrieve it from platform Stripe.
    _require_platform_stripe()
    try:
        sess = stripe.checkout.Session.retrieve(session_id)
    except Exception:
        return render_template("payment_not_complete.html")

    if sess.payment_status != "paid":
        return render_template("payment_not_complete.html")

    try:
        finalize_vote_intent_paid(sess.id)
    except Exception:
        pass
    return render_template("vote_success.html", show=show)

@app.post("/sponsorship/submit")
@rate_limit("sponsorship_submit", 20, 300)
def sponsorship_public_submit():
    show_slug = request.form.get("show_slug", "").strip()
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    if not sponsorship_allowed(show):
        return render_template("sponsorship_closed.html", show=show), 403
        
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
    if payment_method_choice not in {"card", "check", "invoice", "cash"}:
        payment_method_choice = "card"

    base_price_cents = int(catalog["price_cents"] or 0)
    discount_cents = _parse_dollars_to_cents(request.form.get("discount_dollars", "0"), 0)
    discount_reason = request.form.get("discount_reason", "").strip()
    final_price_cents = max(0, base_price_cents - discount_cents)
    sponsor_notes = request.form.get("notes", "").strip()
    if discount_cents > 0:
        sponsor_notes = (sponsor_notes + "\n" if sponsor_notes else "") + f"Discount applied: ${discount_cents / 100:.2f}. Reason: {discount_reason}"

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
        notes=sponsor_notes,
    )
#######
    if payment_method_choice == "card" and final_price_cents <= 0:
        flash("Thank you. Your sponsorship has been recorded with the approved discount.", "ok")
        return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))

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
                    "unit_amount": final_price_cents,
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
            notes=sponsor_notes,
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
        f"Amount: ${float(final_price_cents / 100):.2f}\n"
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

@app.get("/admin/contact-messages")
@require_admin
def admin_contact_messages():
    status = request.args.get("status", "open").strip().lower()
    q = request.args.get("q", "").strip()
    messages = list_contact_messages(status=status, query=q, limit=250)
    return render_template("admin_contact_messages.html", messages=messages, status=status, q=q)


@app.post("/admin/contact-messages/<int:message_id>/read")
@require_admin
def admin_contact_message_mark_read(message_id: int):
    if not get_contact_message(message_id):
        abort(404)
    mark_contact_message_read(message_id)
    flash("Message marked as read.", "ok")
    return redirect(request.referrer or url_for("admin_contact_messages"))


@app.post("/admin/contact-messages/<int:message_id>/archive")
@require_admin
def admin_contact_message_archive(message_id: int):
    if not get_contact_message(message_id):
        abort(404)
    archive_contact_message(message_id)
    flash("Message archived.", "ok")
    return redirect(request.referrer or url_for("admin_contact_messages"))


@app.get("/admin/version")
@require_admin
def admin_version():
    return jsonify({
        "app": "Karman Kar Shows Platform",
        "version": APP_VERSION,
        "release_stage": APP_RELEASE_STAGE,
        "release_name": APP_RELEASE_NAME,
    })


def require_organizer(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("organizer_id"):
            return redirect(url_for("organizer_portal", next=request.path))
        return view_func(*args, **kwargs)
    return wrapped


def _current_organizer() -> Optional[Dict[str, Any]]:
    organizer_id = session.get("organizer_id")
    return get_organizer(_db_path(), int(organizer_id)) if organizer_id else None


@app.get("/organizer")
def organizer_portal():
    organizer = _current_organizer()
    if organizer:
        return redirect(url_for("organizer_dashboard"))
    return render_template("organizer_portal.html", next=request.args.get("next", ""))


@app.post("/organizer/signup")
@rate_limit("organizer_signup", 8, 900)
def organizer_signup():
    organization_name = request.form.get("organization_name", "").strip()
    contact_name = request.form.get("contact_name", "").strip()
    email = request.form.get("email", "").strip().lower()
    phone = request.form.get("phone", "").strip()
    password = request.form.get("password", "")
    if not organization_name or not contact_name or "@" not in email or len(password) < 10:
        flash("Complete every required field and use a password of at least 10 characters.", "error")
        return redirect(url_for("organizer_portal"))
    try:
        organizer_id = create_organizer(
            _db_path(),
            organization_name=organization_name,
            contact_name=contact_name,
            email=email,
            phone=phone,
            password=password,
        )
    except sqlite3.IntegrityError:
        flash("An organizer account already exists for that email address.", "error")
        return redirect(url_for("organizer_portal"))
    session["organizer_id"] = organizer_id
    _log_event("organizer.signup", details={"organizer_id": organizer_id}, actor_type="organizer")
    return redirect(url_for("organizer_dashboard"))


@app.post("/organizer/login")
@rate_limit("organizer_login", 10, 900)
def organizer_login():
    organizer = authenticate_organizer(
        _db_path(),
        request.form.get("email", ""),
        request.form.get("password", ""),
    )
    if not organizer:
        flash("Email or password was not recognized.", "error")
        return redirect(url_for("organizer_portal"))
    session["organizer_id"] = int(organizer["id"])
    return redirect(request.form.get("next", "") or url_for("organizer_dashboard"))


@app.post("/organizer/logout")
@require_organizer
def organizer_logout():
    session.pop("organizer_id", None)
    return redirect(url_for("organizer_portal"))


@app.get("/organizer/dashboard")
@require_organizer
def organizer_dashboard():
    organizer = _current_organizer()
    shows = list_organizer_shows(_db_path(), int(organizer["id"]))
    return render_template("organizer_dashboard.html", organizer=organizer, shows=shows)


def _category_rows_from_request() -> list[dict[str, Any]]:
    upload = request.files.get("category_csv")
    if upload and upload.filename:
        return parse_category_csv(upload.read())
    rows = []
    for idx, line in enumerate(request.form.get("categories_text", "").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split(",", 1)]
        if len(parts) == 2:
            slug, name = parts
        else:
            slug, name = "", parts[0]
        rows.append({"slug": slug, "name": name, "sort_order": idx * 10})
    return rows


def _vendor_form_from_request() -> Dict[str, Any]:
    return {
        "package_id": request.form.get("package_id", "").strip(),
        "business_name": request.form.get("business_name", "").strip(),
        "contact_name": request.form.get("contact_name", "").strip(),
        "email": request.form.get("email", "").strip(),
        "phone": request.form.get("phone", "").strip(),
        "website_url": request.form.get("website_url", "").strip(),
        "products": request.form.get("products", "").strip(),
        "electricity_request": request.form.get("electricity_request") == "on",
        "special_space_request": request.form.get("special_space_request", "").strip(),
        "is_food_vendor": request.form.get("is_food_vendor") == "on",
        "food_details": request.form.get("food_details", "").strip(),
        "insurance_ack": request.form.get("insurance_ack") == "on",
        "rules_accepted": request.form.get("rules_accepted") == "on",
        "refund_accepted": request.form.get("refund_accepted") == "on",
    }


def _validate_vendor_form(data: Dict[str, Any]) -> List[str]:
    errors = []
    if not data["package_id"].isdigit():
        errors.append("Select an available vendor category.")
    if not data["business_name"]:
        errors.append("Business name is required.")
    if not data["contact_name"]:
        errors.append("Contact name is required.")
    if "@" not in data["email"] or "." not in data["email"].split("@")[-1]:
        errors.append("Enter a valid email address.")
    if data["website_url"]:
        parsed = urlparse(data["website_url"])
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            errors.append("Website or social URL must start with http:// or https://.")
    if not data["products"]:
        errors.append("Describe the products or services offered.")
    if not data["rules_accepted"]:
        errors.append("Vendor rules must be accepted before payment.")
    if not data["refund_accepted"]:
        errors.append("No-refund policy must be accepted before payment.")
    return errors


@app.route("/shows/<show_slug>/vendors", methods=["GET", "POST"])
def vendor_registration_page(show_slug: str):
    cleanup_expired_vendor_holds(_db_path())
    show = get_show_by_slug(show_slug)
    if not show:
        abort(404)
    availability = vendor_registration_open(_db_path(), int(show["id"]))
    form_data = _vendor_form_from_request() if request.method == "POST" else {}
    errors: List[str] = []
    if request.method == "POST":
        errors = _validate_vendor_form(form_data)
        if not errors:
            try:
                reg = create_vendor_hold(
                    _db_path(),
                    int(show["id"]),
                    int(form_data["package_id"]),
                    form_data,
                    availability["settings"].get("vendor_agreement") or "Vendor payments are non-refundable once confirmed.",
                    availability["settings"].get("vendor_policy_version") or "vendor-policy-2026-07",
                )
            except ValueError as exc:
                errors.append(str(exc))
            else:
                if int(reg.get("amount_cents") or 0) <= 0:
                    attach_vendor_checkout(_db_path(), int(reg["id"]), f"free_vendor_{reg['hold_token']}")
                    finalize_vendor_paid(_db_path(), f"free_vendor_{reg['hold_token']}", amount_cents=0)
                    return redirect(url_for("vendor_confirmation", show_slug=show_slug, hold_token=reg["hold_token"]))
                if not PLATFORM_STRIPE_SECRET_KEY:
                    flash("Vendor hold created. Stripe test key is not configured in this sandbox, so checkout cannot open yet.", "error")
                    return redirect(url_for("vendor_confirmation", show_slug=show_slug, hold_token=reg["hold_token"]))
                _require_platform_stripe()
                success_url = _abs_url(url_for("vendor_confirmation", show_slug=show_slug, hold_token=reg["hold_token"])) + "?session_id={CHECKOUT_SESSION_ID}"
                cancel_url = _abs_url(url_for("vendor_registration_page", show_slug=show_slug))
                session_obj = stripe.checkout.Session.create(
                    mode="payment",
                    payment_method_types=["card"],
                    line_items=[{
                        "price_data": {
                            "currency": "usd",
                            "unit_amount": int(reg["amount_cents"]),
                            "product_data": {"name": f"Vendor - {show['title']} - {reg['package_name']}"},
                        },
                        "quantity": 1,
                    }],
                    success_url=success_url,
                    cancel_url=cancel_url,
                    **_stripe_payment_fields(show, "vendor", {
                        "vendor_registration_id": str(reg["id"]),
                        "vendor_confirmation_number": reg["confirmation_number"],
                        "vendor_package_id": str(reg["package_id"]),
                    }),
                )
                attach_vendor_checkout(_db_path(), int(reg["id"]), session_obj.id)
                return render_template("vendor_checkout.html", show=show, registration=reg, checkout_url=session_obj.url)
    return render_template(
        "vendor_registration.html",
        show=show,
        availability=availability,
        packages=[package for package in availability["packages"] if int(package.get("is_active") or 0) == 1],
        settings=availability["settings"],
        errors=errors,
        form_data=form_data,
    )


@app.get("/shows/<show_slug>/vendors/confirmation/<hold_token>")
def vendor_confirmation(show_slug: str, hold_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        abort(404)
    reg = get_vendor_registration_by_token(_db_path(), hold_token)
    if not reg or int(reg["show_id"]) != int(show["id"]):
        abort(404)
    session_id = request.args.get("session_id", "").strip()
    if session_id and session_id == reg.get("checkout_session_id") and reg.get("payment_status") != "paid":
        try:
            sess = stripe.checkout.Session.retrieve(session_id) if PLATFORM_STRIPE_SECRET_KEY else None
            if sess and sess.payment_status == "paid":
                finalize_vendor_paid(_db_path(), session_id, getattr(sess, "payment_intent", "") or "", int(getattr(sess, "amount_total", 0) or 0))
                reg = get_vendor_registration_by_token(_db_path(), hold_token) or reg
        except Exception as exc:
            _log_event("vendor.confirmation_payment_check_failed", int(show["id"]), {"error": str(exc)}, actor_type="public")
    return render_template("vendor_confirmation.html", show=show, registration=reg)


@app.route("/admin/shows/<int:show_id>/categories", methods=["GET", "POST"])
@require_admin
def admin_show_categories(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    if request.method == "POST":
        rows = _category_rows_from_request()
        if not rows:
            flash("Enter at least one category or upload a category CSV.", "error")
        else:
            count = save_show_categories(_db_path(), show_id, rows)
            flash(f"Saved {count} voting categories.", "ok")
        return redirect(url_for("admin_show_categories", show_id=show_id))
    categories = list_show_categories(_db_path(), show_id, active_only=False)
    return render_template("show_categories.html", show=dict(show), categories=categories, organizer=None, admin_mode=True)


@app.route("/admin/shows/<int:show_id>/vendors", methods=["GET", "POST"])
@require_admin
def admin_show_vendors(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    if request.method == "POST":
        errors: List[str] = []
        open_raw = request.form.get("vendor_open_at", "").strip()
        deadline_raw = request.form.get("vendor_deadline", "").strip()
        open_dt = _parse_optional_datetime(open_raw)
        deadline_dt = _parse_optional_datetime(deadline_raw)
        if open_raw and not open_dt:
            errors.append("Opening date must be a valid date and time.")
        if deadline_raw and not deadline_dt:
            errors.append("Closing date must be a valid date and time.")
        if open_dt and deadline_dt and deadline_dt <= open_dt:
            errors.append("Closing date must be after the opening date.")

        overall_raw = (request.form.get("vendor_overall_max") or "").strip()
        overall_max = None
        if overall_raw:
            if overall_raw.isdigit() and int(overall_raw) > 0:
                overall_max = int(overall_raw)
            else:
                errors.append("Overall vendor maximum must be a positive whole number.")

        reserved_raw = (request.form.get("vendor_reserved_sponsor_spaces") or "0").strip()
        reserved_spaces = 0
        if reserved_raw:
            if reserved_raw.isdigit():
                reserved_spaces = int(reserved_raw)
            else:
                errors.append("Sponsor-reserved spaces must be a non-negative whole number.")

        package_rows = []
        for idx in range(1, 9):
            capacity_raw = (request.form.get(f"package_{idx}_capacity") or "").strip()
            capacity = None
            if capacity_raw:
                if capacity_raw.isdigit():
                    capacity = int(capacity_raw)
                else:
                    errors.append(f"Package {idx} capacity must be a non-negative whole number.")
            package_reserved_raw = (request.form.get(f"package_{idx}_reserved_sponsor_spaces") or "0").strip()
            package_reserved = 0
            if package_reserved_raw:
                if package_reserved_raw.isdigit():
                    package_reserved = int(package_reserved_raw)
                else:
                    errors.append(f"Package {idx} reserved sponsor spaces must be a non-negative whole number.")
            package_rows.append(
                {
                    "id": request.form.get(f"package_{idx}_id", ""),
                    "name": request.form.get(f"package_{idx}_name", ""),
                    "description": request.form.get(f"package_{idx}_description", ""),
                    "price_cents": _parse_dollars_to_cents(request.form.get(f"package_{idx}_price", "0")),
                    "capacity": capacity,
                    "reserved_sponsor_spaces": package_reserved,
                    "is_food": request.form.get(f"package_{idx}_is_food") == "on",
                    "is_closed": request.form.get(f"package_{idx}_is_closed") == "on",
                    "sort_order": idx * 10,
                    "is_active": request.form.get(f"package_{idx}_is_active") == "on",
                }
            )
        if errors:
            for error in errors:
                flash(error, "error")
            return redirect(url_for("admin_show_vendors", show_id=show_id))

        save_vendor_settings(
            _db_path(),
            show_id,
            {
                "vendors_enabled": request.form.get("vendors_enabled") == "on",
                "vendor_public_status": request.form.get("vendor_public_status", "draft"),
                "vendor_headline": request.form.get("vendor_headline", ""),
                "vendor_instructions": request.form.get("vendor_instructions", ""),
                "vendor_agreement": request.form.get("vendor_agreement", ""),
                "vendor_policy_version": request.form.get("vendor_policy_version", ""),
                "vendor_open_at": open_raw,
                "vendor_deadline": deadline_raw,
                "vendor_overall_max": overall_max,
                "vendor_reserved_sponsor_spaces": reserved_spaces,
                "food_vendors_enabled": request.form.get("food_vendors_enabled") == "on",
            },
        )
        count = save_vendor_packages(_db_path(), show_id, package_rows)
        _log_event(
            "admin.vendor_settings_saved",
            show_id,
            {"package_count": count, "vendor_status": request.form.get("vendor_public_status", "draft")},
            actor_type="admin",
        )
        flash(f"Vendor setup saved with {count} booth/package options.", "ok")
        return redirect(url_for("admin_show_vendors", show_id=show_id))
    return render_template(
        "admin_show_vendors.html",
        show=dict(show),
        settings=get_vendor_settings(_db_path(), show_id),
        packages=package_availability(_db_path(), show_id),
        dashboard=vendor_dashboard(_db_path(), show_id),
    )


@app.get("/admin/shows/<int:show_id>/vendors/registrations")
@require_admin
def admin_vendor_registrations(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    cleanup_expired_vendor_holds(_db_path())
    rows = list_vendor_registrations(
        _db_path(),
        show_id,
        query=request.args.get("q", "").strip(),
        status=request.args.get("status", "").strip(),
        package_id=request.args.get("package_id", "").strip(),
    )
    return render_template(
        "admin_vendor_registrations.html",
        show=dict(show),
        rows=rows,
        dashboard=vendor_dashboard(_db_path(), show_id),
        packages=package_availability(_db_path(), show_id),
        filters=request.args,
    )


@app.get("/admin/shows/<int:show_id>/vendors/export.csv")
@require_admin
def admin_vendor_export_csv(show_id: int):
    _require_show_access(show_id)
    rows = list_vendor_registrations(_db_path(), show_id)
    return send_file(
        io.BytesIO(vendor_csv_bytes(rows)),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"show-{show_id}-vendors.csv",
    )


@app.get("/admin/shows/<int:show_id>/vendors/roster")
@require_admin
def admin_vendor_roster(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    rows = list_vendor_registrations(_db_path(), show_id)
    return render_template("admin_vendor_roster.html", show=dict(show), rows=rows)


@app.route("/admin/vendors/<int:registration_id>", methods=["GET", "POST"])
@require_admin
def admin_vendor_detail(registration_id: int):
    reg = get_vendor_registration(_db_path(), registration_id)
    if not reg:
        abort(404)
    _require_show_access(int(reg["show_id"]))
    show = get_show_by_id(int(reg["show_id"]))
    if request.method == "POST":
        update_vendor_admin(_db_path(), registration_id, request.form)
        flash("Vendor details updated.", "ok")
        return redirect(url_for("admin_vendor_detail", registration_id=registration_id))
    return render_template("admin_vendor_detail.html", show=dict(show), registration=reg)


@app.post("/admin/vendors/<int:registration_id>/check-in")
@require_admin
def admin_vendor_check_in(registration_id: int):
    reg = get_vendor_registration(_db_path(), registration_id)
    if not reg:
        abort(404)
    _require_show_access(int(reg["show_id"]))
    set_vendor_status(_db_path(), registration_id, "checked_in")
    flash("Vendor marked checked in.", "ok")
    return redirect(url_for("admin_vendor_detail", registration_id=registration_id))


@app.post("/admin/vendors/<int:registration_id>/cancel")
@require_admin
def admin_vendor_cancel(registration_id: int):
    reg = get_vendor_registration(_db_path(), registration_id)
    if not reg:
        abort(404)
    _require_show_access(int(reg["show_id"]))
    set_vendor_status(_db_path(), registration_id, "canceled", release_slot=request.form.get("release_slot") == "on")
    flash("Vendor canceled. No refund was issued automatically.", "ok")
    return redirect(url_for("admin_vendor_detail", registration_id=registration_id))


@app.get("/admin/shows/<int:show_id>/vendors/recommend")
@require_admin
def admin_vendor_recommendations(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    values = {k: request.args.get(k, "").strip() for k in ("vehicles", "spectators", "physical_spaces", "sponsor_spaces", "duration_hours", "food")}
    recommendations = None
    if values["physical_spaces"]:
        vehicles = int(values["vehicles"] or 0)
        spectators = int(values["spectators"] or 0)
        physical = max(0, int(values["physical_spaces"] or 0))
        sponsor_spaces = max(0, int(values["sponsor_spaces"] or 0))
        food_enabled = values["food"] != "no"
        sellable = max(0, physical - sponsor_spaces)
        base = min(sellable, max(3, round((vehicles / 25) + (spectators / 250))))
        food_count = min(3, max(0, round(spectators / 500))) if food_enabled else 0
        service = max(1, round(base * 0.35))
        product = max(1, base - service - food_count)
        recommendations = {
            "sellable": sellable,
            "rows": [
                {"name": "Product Vendor", "capacity": product, "price": "50.00", "description": "Retail, crafts, merchandise, and display vendors."},
                {"name": "Service Vendor", "capacity": service, "price": "50.00", "description": "Community, nonprofit, service, and information vendors."},
            ],
            "assumptions": f"Based on {vehicles} vehicles, {spectators} spectators, {physical} physical spaces, and {sponsor_spaces} sponsor-reserved spaces. Recommendations stay within the sellable capacity of {sellable}.",
        }
        if food_enabled:
            recommendations["rows"].append({"name": "Food Vendor", "capacity": food_count, "price": "75.00", "description": "Food truck, snack, drink, or food service vendor."})
    return render_template("admin_vendor_recommend.html", show=dict(show), values=values, recommendations=recommendations)


@app.route("/admin/shows/<int:show_id>/platform-pricing", methods=["GET", "POST"])
@require_admin
def admin_show_platform_pricing(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    if request.method == "POST":
        try:
            percent = float(request.form.get("platform_fee_percent", "10") or 10)
        except ValueError:
            percent = 10.0
        update_show_platform_pricing(
            _db_path(),
            show_id,
            platform_fee_percent=percent,
            platform_event_fee_cents=_parse_dollars_to_cents(request.form.get("platform_event_fee", "0")),
            platform_per_transaction_fee_cents=_parse_dollars_to_cents(request.form.get("platform_per_transaction_fee", "0")),
        )
        flash("Outside-show platform pricing updated.", "ok")
        return redirect(url_for("admin_show_platform_pricing", show_id=show_id))
    report = show_transaction_report(_db_path(), show_id)
    return render_template("admin_show_platform_pricing.html", show=dict(show), report=report)


@app.get("/admin")
def admin_page():
    show = _admin_current_show() if session.get("admin_authed") else get_active_show()
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

    visible_shows = _admin_visible_shows()
    return render_template(
        "admin.html",
        show=show,
        authed=True,
        next=next_url,
        registered_cars=registered_cars,
        visible_shows=visible_shows,
    )

@app.get("/admin/car-search")
@require_admin
def admin_car_search():
    show = _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))

    q = request.args.get("q", "").strip()
    results = search_show_cars_admin(int(show["id"]), q)

    return render_template(
        "admin_car_search.html",
        show=show,
        q=q,
        results=results,
    )

@app.get("/admin/registration/<int:show_car_id>/edit")
@require_admin
def admin_registration_edit(show_car_id: int):
    show = _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))

    car = get_show_car_admin_by_id(int(show["id"]), int(show_car_id))
    if not car:
        return "Registration not found.", 404

    slots = list_registration_slots(int(show["id"]), public_only=False)

    selected_slot_ids = []
    conn = _conn_direct()
    try:
        rows = conn.execute(
            """
            SELECT registration_slot_id
            FROM show_car_registration_slots
            WHERE show_id = ? AND show_car_id = ?
            ORDER BY registration_slot_id ASC
            """,
            (int(show["id"]), int(show_car_id)),
        ).fetchall()
        selected_slot_ids = [int(r["registration_slot_id"]) for r in rows]
    finally:
        conn.close()

    if not selected_slot_ids and car["registration_slot_id"]:
        selected_slot_ids = [int(car["registration_slot_id"])]

    return render_template(
        "admin_registration_edit.html",
        show=show,
        car=car,
        slots=slots,
        selected_slot_ids=selected_slot_ids,
    )


@app.post("/admin/registration/<int:show_car_id>/edit")
@require_admin
def admin_registration_edit_submit(show_car_id: int):
    show = get_active_show()
    if not show:
        return "No active show.", 500

    car = get_show_car_admin_by_id(int(show["id"]), int(show_car_id))
    if not car:
        return "Registration not found.", 404

    owner_name = request.form.get("owner_name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip().lower()
    year = request.form.get("year", "").strip()
    make = request.form.get("make", "").strip()
    model = request.form.get("model", "").strip()
    insurance_carrier = request.form.get("insurance_carrier", "").strip()
    registration_payment_status = request.form.get("registration_payment_status", "").strip()

    raw_slot_ids = request.form.getlist("registration_slot_ids")
    slot_ids = []
    for raw in raw_slot_ids:
        raw = str(raw or "").strip()
        if raw.isdigit():
            slot_id = int(raw)
            if slot_id not in slot_ids:
                slot_ids.append(slot_id)

    if not (owner_name and phone and email and year and make and model):
        slots = list_registration_slots(int(show["id"]), public_only=False)
        return render_template(
            "admin_registration_edit.html",
            show=show,
            car=car,
            slots=slots,
            selected_slot_ids=slot_ids,
            error="Owner name, phone, email, year, make, and model are required.",
        )

    try:
        update_show_car_admin_registration(
            show_id=int(show["id"]),
            show_car_id=int(show_car_id),
            owner_name=owner_name,
            phone=phone,
            email=email,
            year=year,
            make=make,
            model=model,
            insurance_carrier=insurance_carrier,
            registration_payment_status=registration_payment_status,
            registration_slot_ids=slot_ids,
        )
    except ValueError as e:
        slots = list_registration_slots(int(show["id"]), public_only=False)
        return render_template(
            "admin_registration_edit.html",
            show=show,
            car=car,
            slots=slots,
            selected_slot_ids=slot_ids,
            error=str(e),
        )

    _log_event(
        "admin.registration_edited",
        int(show["id"]),
        {
            "show_car_id": int(show_car_id),
            "car_number": int(car["car_number"]),
            "payment_status": registration_payment_status,
            "slot_ids": slot_ids,
        },
        actor_type="admin",
    )

    flash("Registration updated.", "ok")
    return redirect(url_for("admin_registration_edit", show_car_id=show_car_id))


@app.post("/admin/registration/<int:show_car_id>/remove")
@require_admin
def admin_registration_remove(show_car_id: int):
    show = get_active_show()
    if not show:
        return "No active show.", 500

    car = get_show_car_admin_by_id(int(show["id"]), int(show_car_id))
    if not car:
        return "Registration not found.", 404

    confirm = request.form.get("confirm_remove", "").strip().lower()
    if confirm != "yes":
        flash("Removal was not completed. Check the confirmation box first.", "error")
        return redirect(url_for("admin_registration_edit", show_car_id=show_car_id))

    remove_show_car_registration(
        show_id=int(show["id"]),
        show_car_id=int(show_car_id),
        removed_by="admin",
    )

    _log_event(
        "admin.registration_removed",
        int(show["id"]),
        {
            "show_car_id": int(show_car_id),
            "car_number": int(car["car_number"]),
            "previous_payment_status": car["registration_payment_status"],
            "previous_registration_state": car["registration_state"] if "registration_state" in car.keys() else "",
            "note": "Soft removed by admin. Spot opened for capacity counting.",
        },
        actor_type="admin",
    )

    flash("Registration removed. The spot is now open and this car no longer counts as coming.", "ok")
    return redirect(url_for("admin_registration_edit", show_car_id=show_car_id))


@app.get("/admin/debug-registration-slots")
@require_admin
def admin_debug_registration_slots():
    conn = _conn_direct()
    try:
        rows = conn.execute("""
            SELECT
                sc.car_number,
                p.name,
                p.phone,
                p.email,
                sc.year || ' ' || sc.make || ' ' || sc.model AS vehicle,
                GROUP_CONCAT(srs.slot_label, ', ') AS slots
            FROM show_cars sc
            LEFT JOIN people p ON p.id = sc.person_id
            LEFT JOIN show_car_registration_slots scrs ON scrs.show_car_id = sc.id
            LEFT JOIN show_registration_slots srs ON srs.id = scrs.registration_slot_id
            GROUP BY sc.id
            ORDER BY sc.car_number
        """).fetchall()
    finally:
        conn.close()

    html = "<h1>Registration Slots</h1><pre>"
    for r in rows:
        html += f"#{r['car_number']} | {r['name']} | {r['phone']} | {r['email']} | {r['vehicle']} | {r['slots']}\n"
    html += "</pre>"
    return html


@app.get("/admin/paper-ballots")
@require_admin
def admin_paper_ballots():
    show_id = request.args.get("show_id", "").strip()
    show = get_show_by_id(int(show_id)) if show_id.isdigit() else _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))
    classes = list_paper_ballot_classes(int(show["id"]))
    recent_ballots = list_recent_paper_ballots(int(show["id"]))
    return render_template(
        "admin_paper_ballot_entry.html",
        show=show,
        classes=classes,
        recent_ballots=recent_ballots,
        result=None,
        errors=[],
    )


@app.post("/admin/paper-ballots")
@require_admin
def admin_paper_ballots_submit():
    show_id = int(request.form.get("show_id", "0") or 0)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    _require_show_access(show_id)
    classes = list_paper_ballot_classes(show_id)
    selections: Dict[int, Dict[int, int]] = {}
    for c in classes:
        class_id = int(c["id"])
        selections[class_id] = {}
        for placement, field in [(1, "first"), (2, "second"), (3, "third")]:
            raw = request.form.get(f"class_{class_id}_{field}", "").strip()
            if raw:
                selections[class_id][placement] = raw
    result = create_paper_ballot_with_votes(
        show_id,
        selections,
        ballot_label=request.form.get("ballot_label", "").strip(),
        source="manual",
        entered_by=_current_admin_label(),
        notes=request.form.get("notes", "").strip(),
    )
    recent_ballots = list_recent_paper_ballots(show_id)
    return render_template(
        "admin_paper_ballot_entry.html",
        show=show,
        classes=classes,
        recent_ballots=recent_ballots,
        result=result if result.get("ok") else None,
        errors=result.get("errors", []),
    )


@app.get("/admin/paper-ballots/print")
@require_admin
def admin_paper_ballots_print():
    show_id = request.args.get("show_id", "").strip()
    show = get_show_by_id(int(show_id)) if show_id.isdigit() else _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))
    classes = list_paper_ballot_classes(int(show["id"]))
    return render_template("paper_ballot_print.html", show=show, classes=classes, hide_nav=True)


@app.get("/admin/paper-ballots/template.csv")
@require_admin
def admin_paper_ballots_template_csv():
    show_id = request.args.get("show_id", "").strip()
    show = get_show_by_id(int(show_id)) if show_id.isdigit() else _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))
    csv_text = build_paper_ballot_csv_template(int(show["id"]))
    return send_file(
        io.BytesIO(csv_text.encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"paper-ballot-template-{show['slug']}.csv",
    )


@app.get("/admin/paper-ballots/import")
@require_admin
def admin_paper_ballots_import_page():
    show_id = request.args.get("show_id", "").strip()
    show = get_show_by_id(int(show_id)) if show_id.isdigit() else _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))
    return render_template("admin_paper_ballot_import.html", show=show, result=None, errors=[])


@app.post("/admin/paper-ballots/import")
@require_admin
def admin_paper_ballots_import_submit():
    show_id = int(request.form.get("show_id", "0") or 0)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    _require_show_access(show_id)
    file_storage = request.files.get("csv_file")
    errors = []
    result = None
    if not file_storage or not file_storage.filename:
        errors = ["Please choose a CSV file."]
    else:
        raw = file_storage.read()
        csv_text = raw.decode("utf-8-sig", errors="replace")
        result = import_paper_ballot_csv(show_id, csv_text, entered_by=_current_admin_label())
        if not result.get("ok"):
            errors = result.get("errors", [])
    return render_template("admin_paper_ballot_import.html", show=show, result=result, errors=errors)


@app.get("/admin/command-center")
@require_admin
def admin_command_center():
    show = _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))

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
        new_contact_message_count=count_new_contact_messages(),
    )
    
@app.post("/admin/login")
@rate_limit("admin_login", 10, 900)
def admin_login():
    email = request.form.get("email", "").strip().lower()
    pw = request.form.get("password", "")
    next_url = request.form.get("next", "") or url_for("admin_page")
    show = get_active_show()

    # New user-based login. If email is supplied, authenticate against admin_users.
    if email:
        user = get_admin_user_by_email(email)
        if user and int(user["is_active"] or 0) == 1 and check_password_hash(user["password_hash"], pw or ""):
            session["admin_authed"] = True
            session["admin_user_id"] = int(user["id"])
            session["admin_email"] = user["email"]
            session["admin_name"] = user["name"]
            session["admin_role"] = (user["global_role"] or "show_owner").strip().lower()
            _log_event("admin.user_login_success", int(show["id"]) if show else None, {"email": email, "next": next_url}, actor_type="admin")
            return redirect(next_url)
        _log_event("admin.user_login_failed", int(show["id"]) if show else None, {"email": email, "next": next_url}, actor_type="admin")
        return render_template("admin.html", show=show, authed=False, login_error="Incorrect email or password.", next=next_url)

    # Legacy super-admin fallback. Keeps existing Railway ADMIN_PASSWORD/ADMIN_PASSWORD_HASH working.
    if _check_admin_password(pw):
        session["admin_authed"] = True
        session["admin_user_id"] = None
        session["admin_email"] = ""
        session["admin_name"] = "Legacy Super Admin"
        session["admin_role"] = "super_admin"
        _log_event("admin.legacy_login_success", int(show["id"]) if show else None, {"next": next_url}, actor_type="admin")
        return redirect(next_url)

    _log_event("admin.login_failed", int(show["id"]) if show else None, {"next": next_url}, actor_type="admin")
    return render_template("admin.html", show=show, authed=False, login_error="Incorrect password.", next=next_url)


@app.post("/admin/logout")
@require_admin
def admin_logout():
    show = get_active_show()
    for key in ["admin_authed", "admin_user_id", "admin_email", "admin_name", "admin_role"]:
        session.pop(key, None)
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

    show_type = (request.form.get("show_type") or "full").strip().lower().replace("-", "_")
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
        public_vote_disclosure=request.form.get("public_vote_disclosure", "").strip() or DEFAULT_PUBLIC_VOTE_DISCLOSURE,
        public_registration_disclosure=request.form.get("public_registration_disclosure", ""),
        public_donation_disclosure=request.form.get("public_donation_disclosure", ""),
        voting_mode=request.form.get("voting_mode", "fundraiser_unlimited").strip(),
        voting_method=request.form.get("voting_method", "qr_only").strip(),
        payment_mode=request.form.get("payment_mode", "stripe").strip(),
        charity_processor_label=request.form.get("charity_processor_label", "").strip(),
        external_payment_url=request.form.get("external_payment_url", "").strip(),
        allow_custom_votes=1 if request.form.get("allow_custom_votes") else 0,
        preset_vote_options=request.form.get("preset_vote_options", "1,5,10,20,25").strip(),
        max_votes_per_checkout=max(
            1,
            int(request.form.get("max_votes_per_checkout", "50") or "50")
        ) if (request.form.get("max_votes_per_checkout", "50") or "50").isdigit() else 50,
        registration_slot_selection_mode=request.form.get("registration_slot_selection_mode", "single").strip(),
    )
    _log_event("admin.show_settings_saved", int(show["id"]), {"show_type": show_type, "max_cars": max_cars}, actor_type="admin")
    flash("Show settings saved.", "ok")
    return redirect(url_for("admin_page"))


@app.get("/admin/shows")
@require_admin
def admin_shows():
    shows = _admin_visible_shows()

    def _status(row):
        return str(row["status"] or "draft").strip().lower()

    active_shows = [s for s in shows if int(s["is_active"] or 0) == 1 and _status(s) not in {"archived", "past"}]
    upcoming_shows = [s for s in shows if _status(s) == "upcoming" and int(s["is_active"] or 0) != 1]
    draft_shows = [s for s in shows if _status(s) in {"", "draft"}]
    archived_shows = [s for s in shows if _status(s) in {"archived", "past"}]

    return render_template(
        "admin_shows.html",
        shows=shows,
        active_shows=active_shows,
        upcoming_shows=upcoming_shows,
        draft_shows=draft_shows,
        archived_shows=archived_shows,
        show=_admin_current_show(),
        saved_exports=_list_saved_exports() if _admin_is_super() else [],
        pending_vote_reviews=list_pending_vote_reviews() if _admin_is_super() else [],
        can_create_shows=_admin_is_super(),
    )


@app.get("/admin/shows/<int:show_id>")
@require_admin
def admin_show_detail(show_id: int):
    _require_show_access(show_id)
    show_detail = get_show_by_id(show_id)
    if not show_detail:
        abort(404)
    slots_by_show = {int(show_id): list_registration_slots(int(show_id), public_only=False)}
    classes_by_show = {int(show_id): list_judging_classes(int(show_id), active_only=False)}
    return render_template(
        "admin_show_detail.html",
        show_detail=show_detail,
        show=show_detail,
        waiver_templates=list_waiver_templates(),
        slots_by_show=slots_by_show,
        classes_by_show=classes_by_show,
        show_voters=list_show_voters(int(show_id)),
    )


@app.get("/admin/shows/<int:show_id>/general-vote-qr.png")
@require_admin
def admin_show_general_vote_qr_png(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    import qrcode

    vote_url = _abs_url(url_for("car_number_vote_page", show_slug=show["slug"]))
    qr = qrcode.QRCode(version=1, box_size=12, border=3)
    qr.add_data(vote_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    mem = io.BytesIO()
    img.save(mem, format="PNG")
    mem.seek(0)
    return send_file(
        mem,
        mimetype="image/png",
        as_attachment=request.args.get("download", "").strip() == "1",
        download_name=f"{show['slug']}-scan-to-vote-enter-car-number.png",
    )


@app.post("/admin/shows/<int:show_id>/judge-code")
@require_admin
def admin_create_judge_code(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        abort(404)
    if not _show_participant_voting(show):
        flash("Judge access codes are only available when Voting Mode is set to Participant/Judge restricted voting.", "error")
        return redirect(url_for("admin_show_detail", show_id=show_id))
    display_name = request.form.get("display_name", "Judge").strip() or "Judge"
    email = request.form.get("email", "").strip().lower()
    phone = request.form.get("phone", "").strip()
    voter = create_judge_voter(int(show_id), display_name, email, phone)
    access_url = _abs_url(url_for("judge_vote_access", show_slug=show["slug"], voter_token=voter["voter_token"]))
    flash(f"Judge code created for {display_name}. Access link: {access_url}", "ok")
    return redirect(url_for("admin_show_detail", show_id=show_id))


def _read_uploaded_csv_rows(file_storage) -> list[dict[str, Any]]:
    if not file_storage or not file_storage.filename:
        raise ValueError("Please choose a CSV file to import.")
    raw = file_storage.read()
    text = raw.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("The CSV file does not have a header row.")
    rows = []
    for row in reader:
        clean = {str(k or "").strip(): (v or "").strip() for k, v in row.items()}
        if any(clean.values()):
            rows.append(clean)
    if not rows:
        raise ValueError("The CSV file did not contain any importable rows.")
    return rows


@app.get("/admin/shows/<int:show_id>/import")
@require_admin
def admin_show_import(show_id: int):
    _require_show_access(show_id)
    show_detail = get_show_by_id(show_id)
    if not show_detail:
        abort(404)
    return render_template("admin_show_import.html", show_detail=show_detail, show=show_detail)


@app.post("/admin/shows/<int:show_id>/import")
@require_admin
def admin_show_import_post(show_id: int):
    _require_show_access(show_id)
    show_detail = get_show_by_id(show_id)
    if not show_detail:
        abort(404)
    import_type = (request.form.get("import_type") or "").strip().lower()
    try:
        rows = _read_uploaded_csv_rows(request.files.get("import_file"))
        if import_type == "classes":
            result = import_judging_classes_for_show(show_id, rows)
            flash(f"Imported {result.get('created', 0)} judging classes.", "ok")
        elif import_type == "registrations":
            result = import_registered_cars_for_show(
                show_id,
                rows,
                assume_paid=request.form.get("assume_paid") == "on",
            )
            flash(
                f"Imported {result.get('created', 0)} registrations. "
                f"Created {result.get('classes_created', 0)} missing classes. "
                f"Skipped {result.get('skipped', 0)} rows.",
                "ok" if not result.get("skipped") else "error",
            )
        else:
            raise ValueError("Unknown import type.")
    except Exception as e:
        flash(f"Import failed: {e}", "error")
    return redirect(url_for("admin_show_import", show_id=show_id))


@app.post("/admin/shows/create")
@require_super_admin
def admin_shows_create():
    max_cars_raw = request.form.get("max_cars", "").strip()
    max_cars = int(max_cars_raw) if max_cars_raw.isdigit() and int(max_cars_raw) > 0 else None

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

    voting_mode = request.form.get("voting_mode", "fundraiser_unlimited").strip().lower()
    voting_method = request.form.get("voting_method", "both").strip().lower()
    payment_mode = request.form.get("payment_mode", "stripe").strip().lower()
    show_type = (request.form.get("show_type") or "full").strip().lower().replace("-", "_")
    if show_type == "cruise_in":
        voting_mode = "none" if voting_mode == "fundraiser_unlimited" else voting_mode
        voting_method = "disabled" if voting_method == "both" else voting_method
        payment_mode = "none" if payment_mode == "stripe" else payment_mode
    external_payment_url = request.form.get("external_payment_url", "").strip()
    charity_processor_label = request.form.get("charity_processor_label", "").strip()
    allow_custom_votes = 1 if request.form.get("allow_custom_votes") else 0
    preset_vote_options = request.form.get("preset_vote_options", "1,5,10,20,25").strip()
    max_votes_per_checkout_raw = request.form.get("max_votes_per_checkout", "50").strip()

    try:
        max_votes_per_checkout = max(1, int(max_votes_per_checkout_raw))
    except ValueError:
        max_votes_per_checkout = 50

    max_cars_raw = request.form.get("max_cars", "").strip()
    max_cars = int(max_cars_raw) if max_cars_raw.isdigit() and int(max_cars_raw) > 0 else None

    flyer_image_path = request.form.get("flyer_image_path", "").strip()
    flyer_file = request.files.get("flyer_image")
    if flyer_file and flyer_file.filename:
        try:
            flyer_image_path = _save_uploaded_flyer(flyer_file, slug)
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("admin_shows"))

    new_show_id = create_show_admin(
        slug=slug,
        flyer_image_path=flyer_image_path,
        title=title,
        show_type=show_type,
        max_cars=max_cars,
        date=request.form.get("date", "").strip(),
        time="",
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
        public_vote_disclosure=request.form.get("public_vote_disclosure", "").strip() or DEFAULT_PUBLIC_VOTE_DISCLOSURE,
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
        voting_method=voting_method,
        participant_voting_enabled=1 if request.form.get("participant_voting_enabled") == "on" or voting_mode in {"participant_restricted", "participant_only", "judge_only"} else 0,
        payment_mode=payment_mode,
        charity_processor_label=charity_processor_label,
        external_payment_url=external_payment_url,
        allow_custom_votes=allow_custom_votes,
        preset_vote_options=preset_vote_options,
        max_votes_per_checkout=max_votes_per_checkout,
        allow_sponsorships=1 if request.form.get("allow_sponsorships") == "on" else 0,
        registration_slot_selection_mode=request.form.get("registration_slot_selection_mode", "single").strip(),
        card_headline=request.form.get("card_headline", "").strip(),
        card_subheadline=request.form.get("card_subheadline", "").strip(),
        card_layout_mode=request.form.get("card_layout_mode", "auto").strip(),
    )
    save_registration_slots_for_show(new_show_id, _slot_payloads_from_request())
    save_judging_classes_for_show(new_show_id, _judging_class_payloads_from_request())

    flash("Show created.", "ok")
    return redirect(url_for("admin_shows"))


@app.post("/admin/shows/<int:show_id>/update")
@require_admin
def admin_shows_update(show_id: int):
    _require_show_access(show_id)
    try:
        sort_order = int(request.form.get("sort_order", "100") or "100")
    except ValueError:
        sort_order = 100

    try:
        waiver_template_id = int(request.form.get("waiver_template_id", "0") or "0") or None
    except ValueError:
        waiver_template_id = None

    voting_mode = request.form.get("voting_mode", "fundraiser_unlimited").strip().lower()
    voting_method = request.form.get("voting_method", "qr_only").strip().lower()
    payment_mode = request.form.get("payment_mode", "stripe").strip().lower()
    show_type = (request.form.get("show_type") or "full").strip().lower().replace("-", "_")
    if show_type == "cruise_in":
        voting_mode = "none" if voting_mode == "fundraiser_unlimited" else voting_mode
        voting_method = "disabled" if voting_method == "qr_only" else voting_method
        payment_mode = "none" if payment_mode == "stripe" else payment_mode
    external_payment_url = request.form.get("external_payment_url", "").strip()
    charity_processor_label = request.form.get("charity_processor_label", "").strip()
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
        show_type=show_type,
        max_cars=max_cars,
        flyer_image_path=flyer_image_path,
        date=request.form.get("date", "").strip(),
        time="",
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
        public_vote_disclosure=request.form.get("public_vote_disclosure", "").strip() or DEFAULT_PUBLIC_VOTE_DISCLOSURE,
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
        voting_method=voting_method,
        participant_voting_enabled=1 if request.form.get("participant_voting_enabled") == "on" or voting_mode in {"participant_restricted", "participant_only", "judge_only"} else 0,
        payment_mode=payment_mode,
        charity_processor_label=charity_processor_label,
        external_payment_url=external_payment_url,
        allow_custom_votes=allow_custom_votes,
        preset_vote_options=preset_vote_options,
        max_votes_per_checkout=max_votes_per_checkout,
        allow_sponsorships=1 if request.form.get("allow_sponsorships") == "on" else 0,
        registration_slot_selection_mode=request.form.get("registration_slot_selection_mode", "single").strip(),
        card_headline=request.form.get("card_headline", "").strip(),
        card_subheadline=request.form.get("card_subheadline", "").strip(),
        card_layout_mode=request.form.get("card_layout_mode", "auto").strip(),
    )
    save_registration_slots_for_show(show_id, _slot_payloads_from_request())
    save_judging_classes_for_show(show_id, _judging_class_payloads_from_request())

    flash("Show updated.", "ok")
    return redirect(url_for("admin_show_detail", show_id=show_id))


@app.post("/admin/shows/<int:show_id>/set-active")
@require_super_admin
def admin_shows_set_active(show_id: int):
    set_active_show(show_id)
    _log_event("admin.show_set_active", show_id, actor_type="admin")
    flash("Show set as active.", "ok")
    return redirect(url_for("admin_shows"))


@app.post("/admin/shows/<int:show_id>/set-upcoming")
@require_super_admin
def admin_shows_set_upcoming(show_id: int):
    set_upcoming_show(show_id)
    _log_event("admin.show_set_upcoming", show_id, actor_type="admin")
    flash("Show set as upcoming.", "ok")
    return redirect(url_for("admin_shows"))


@app.post("/admin/shows/<int:show_id>/set-past")
@require_super_admin
def admin_shows_set_past(show_id: int):
    set_past_show(show_id)
    try:
        _, filename, save_path = _save_snapshot_zip_for_show(show_id)
        _log_event("admin.show_set_past", show_id, {"auto_export_filename": filename, "saved_path": save_path}, actor_type="admin")
        flash(f"Show moved to past and export saved: {filename}", "ok")
    except Exception as e:
        _log_event("admin.show_set_past_export_failed", show_id, {"error": str(e)}, actor_type="admin")
        flash(f"Show moved to past, but automatic export failed: {e}", "error")
    return redirect(url_for("admin_shows"))


@app.post("/admin/shows/<int:show_id>/archive")
@require_super_admin
def admin_shows_archive(show_id: int):
    archive_show(show_id)
    try:
        _, filename, save_path = _save_snapshot_zip_for_show(show_id)
        _log_event("admin.show_archived", show_id, {"auto_export_filename": filename, "saved_path": save_path}, actor_type="admin")
        flash(f"Show archived and export saved: {filename}", "ok")
    except Exception as e:
        _log_event("admin.show_archive_export_failed", show_id, {"error": str(e)}, actor_type="admin")
        flash(f"Show archived, but automatic export failed: {e}", "error")
    return redirect(url_for("admin_shows"))


@app.get("/admin/waivers")
@require_admin
def admin_waivers():
    show = _admin_current_show()
    if show:
        _require_show_permission(int(show["id"]), {"show_owner"})
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
    _require_show_permission(int(show["id"]), {"show_owner", "registrar", "volunteer"})

    ids_raw = request.args.get("ids", "").strip()
    all_raw = request.args.get("all", "").strip()
    include_back = request.args.get("back", "").strip() == "1"
    print_mode = request.args.get("mode", "").strip().lower()

    cars = list_show_cars_public(int(show["id"]))
    if not cars:
        return "No cars to print.", 400

    if print_mode == "registered":
        cars = [
            r for r in cars
            if int(r["is_placeholder"] or 0) == 0
            and str(r["registration_payment_status"] or "").lower() == "paid"
        ]

    elif print_mode == "unused":
        cars = [
            r for r in cars
            if int(r["is_placeholder"] or 0) == 1
            and str(r["registration_state"] or "").lower() == "placeholder"
        ]

    if not cars:
        return "No cars match selected print mode.", 400

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

        if not selected:
            return "No selected cars match selected print mode.", 400

    title_sponsor, sponsors = get_show_sponsors(int(show["id"])) or (None, [])

    pdf_bytes = build_landscape_cards_pdf(
        show=dict(show),
        cars_rows=[dict(r) for r in selected],
        base_url=_abs_url(""),
        static_root=os.path.join(app.root_path, "static"),
        title_sponsor=title_sponsor,
        sponsors=sponsors,
        judging_classes=[dict(c) for c in list_judging_classes(int(show["id"]), active_only=True)],
        include_back=include_back,
        mirror_back_pages=False,
    )

    _log_event(
        "admin.print_cards_exported",
        int(show["id"]),
        {
            "count": len(selected),
            "include_back": include_back,
            "mode": print_mode or "all",
        },
        actor_type="admin",
    )

    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=f"{show['slug']}-voting-cards-landscape.pdf",
    )



@app.get("/admin/show-mode")
@require_admin
def admin_show_mode():
    """Fast staff dashboard for show-day check-in, placeholder claiming, and cash payment review."""
    show = get_active_show()
    if not show:
        return "No active show.", 500
    conn = _conn_direct()
    try:
        rows = conn.execute(
            """
            SELECT
                sc.*,
                p.name AS owner_name,
                p.phone AS owner_phone,
                p.email AS owner_email,
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
            WHERE sc.show_id = ?
              AND COALESCE(sc.registration_state, '') != 'removed'
              AND COALESCE(sc.registration_payment_status, '') NOT IN ('removed', 'canceled', 'refunded')
            ORDER BY sc.car_number ASC
            """,
            (int(show["id"]),),
        ).fetchall()
    finally:
        conn.close()
    return render_template("admin_show_mode.html", show=show, cars=rows)


@app.post("/admin/show-mode/mark-cash-paid/<int:show_car_id>")
@require_admin
def admin_show_mode_mark_cash_paid(show_car_id: int):
    show = get_active_show()
    if not show:
        return "No active show.", 500
    _require_show_permission(int(show["id"]), {"show_owner", "registrar"})
    conn = _conn_direct()
    try:
        conn.execute(
            """
            UPDATE show_cars
            SET registration_payment_status = 'paid_cash',
                registration_state = CASE
                    WHEN COALESCE(checked_in_at, '') != '' THEN 'checked-in'
                    ELSE 'claimed'
                END
            WHERE id = ? AND show_id = ?
            """,
            (int(show_car_id), int(show["id"])),
        )
        conn.execute(
            """
            UPDATE registration_intents
            SET payment_status = 'paid_cash', paid_at = COALESCE(paid_at, datetime('now'))
            WHERE finalized_show_car_id = ? AND show_id = ?
            """,
            (int(show_car_id), int(show["id"])),
        )
        conn.commit()
        _log_event("admin.cash_payment_marked_paid", int(show["id"]), {"show_car_id": int(show_car_id)}, actor_type="admin")
    finally:
        conn.close()
    flash("Cash payment marked paid.", "ok")
    return redirect(url_for("admin_show_mode"))


def _exports_dir() -> Path:
    p = Path("/data/exports") if os.path.isdir("/data") else Path(app.instance_path) / "exports"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _save_snapshot_zip_for_show(show_id: int) -> tuple[bytes, str, str]:
    zip_bytes, filename = build_snapshot_zip_bytes(int(show_id))
    save_path = _exports_dir() / filename
    save_path.write_bytes(zip_bytes)
    return zip_bytes, filename, str(save_path)


def _list_saved_exports() -> list[dict]:
    export_dir = _exports_dir()
    rows = []
    for p in sorted(export_dir.glob("*.zip"), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            rows.append({
                "filename": p.name,
                "size_bytes": p.stat().st_size,
                "modified_at": datetime.fromtimestamp(p.stat().st_mtime, LOCAL_TZ).strftime("%Y-%m-%d %I:%M %p"),
            })
        except Exception:
            pass
    return rows


@app.get("/admin/export-snapshot.zip")
@require_admin
def admin_export_snapshot_zip():
    """Legacy active-show export route. Kept for old bookmarks."""
    show = get_active_show()
    if not show:
        flash("No active show. Use Manage Shows → Download Data for any past show.", "error")
        return redirect(url_for("admin_shows"))
    zip_bytes, filename, save_path = _save_snapshot_zip_for_show(int(show["id"]))
    _log_event("admin.snapshot_exported", int(show["id"]), {"filename": filename, "saved_path": save_path}, actor_type="admin")
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip", as_attachment=True, download_name=filename)


@app.get("/admin/shows/<int:show_id>/export.zip")
@require_admin
def admin_export_show_zip(show_id: int):
    _require_show_access(show_id)
    show = get_show_by_id(show_id)
    if not show:
        return "Show not found.", 404
    zip_bytes, filename, save_path = _save_snapshot_zip_for_show(show_id)
    _log_event("admin.show_snapshot_exported", show_id, {"filename": filename, "saved_path": save_path}, actor_type="admin")
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip", as_attachment=True, download_name=filename)


@app.get("/admin/exports/<path:filename>")
@require_admin
def admin_download_saved_export(filename: str):
    safe_name = secure_filename(filename)
    if safe_name != filename or not safe_name.endswith(".zip"):
        return "Invalid export filename.", 400
    export_path = _exports_dir() / safe_name
    if not export_path.exists():
        return "Export not found.", 404
    return send_file(export_path, mimetype="application/zip", as_attachment=True, download_name=safe_name)


@app.post("/admin/shows/<int:show_id>/close-and-export")
@require_admin
def admin_show_close_and_export(show_id: int):
    show = get_show_by_id(show_id)
    if not show:
        return "Show not found.", 404
    set_show_voting_open(show_id, False)
    set_past_show(show_id)
    zip_bytes, filename, save_path = _save_snapshot_zip_for_show(show_id)
    _log_event("admin.show_closed_and_exported", show_id, {"filename": filename, "saved_path": save_path}, actor_type="admin")
    flash(f"Show closed and export saved: {filename}", "ok")
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip", as_attachment=True, download_name=filename)


@app.post("/admin/close-voting-and-export")
@require_admin
def admin_close_voting_and_export():
    """Legacy active-show close/export route. Saves the ZIP permanently too."""
    show = get_active_show()
    if not show:
        flash("No active show. Use Manage Shows → Close + Export for any show.", "error")
        return redirect(url_for("admin_shows"))
    set_show_voting_open(int(show["id"]), False)
    zip_bytes, filename, save_path = _save_snapshot_zip_for_show(int(show["id"]))
    _log_event("admin.voting_closed_and_exported", int(show["id"]), {"filename": filename, "saved_path": save_path}, actor_type="admin")
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip", as_attachment=True, download_name=filename)


@app.post("/admin/toggle-voting")
@require_admin
def admin_toggle_voting():
    show = get_active_show()
    if show:
        _require_show_permission(int(show["id"]), {"show_owner"})
        toggle_show_voting(int(show["id"]))
        _log_event("admin.voting_toggled", int(show["id"]), actor_type="admin")
    return redirect(url_for("admin_page"))


@app.post("/admin/open-voting")
@require_admin
def admin_open_voting():
    show = get_active_show()
    if show:
        _require_show_permission(int(show["id"]), {"show_owner"})
        set_show_voting_open(int(show["id"]), True)
        _log_event("admin.voting_opened", int(show["id"]), actor_type="admin")
    return redirect(url_for("admin_page"))


@app.post("/admin/close-voting")
@require_admin
def admin_close_voting():
    show = get_active_show()
    if show:
        _require_show_permission(int(show["id"]), {"show_owner"})
        set_show_voting_open(int(show["id"]), False)
        _log_event("admin.voting_closed", int(show["id"]), actor_type="admin")
    return redirect(url_for("admin_page"))


@app.post("/admin/reset-votes")
@require_admin
def admin_reset_votes():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    _require_show_permission(int(show["id"]), {"show_owner"})
    zip_bytes, filename = build_snapshot_zip_bytes(int(show["id"]))
    reset_votes_for_show(int(show["id"]))
    _log_event("admin.votes_reset", int(show["id"]), {"backup_filename": filename}, actor_type="admin")
    return send_file(io.BytesIO(zip_bytes), mimetype="application/zip", as_attachment=True, download_name=filename)


@app.get("/admin/leads")
@require_admin
def admin_leads():
    show_id_raw = request.args.get("show_id", "").strip()
    selected_show_id = int(show_id_raw) if show_id_raw.isdigit() else None

    shows = _admin_visible_shows()
    allowed_ids = [int(s["id"]) for s in shows]
    if selected_show_id and not _admin_can_access_show(selected_show_id):
        abort(403, "You do not have access to this show.")
    if not _admin_is_super() and not selected_show_id and allowed_ids:
        # Scoped users see leads for their first accessible show by default.
        selected_show_id = allowed_ids[0]
    leads = list_marketing_contacts(selected_show_id)

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
    if selected_show_id:
        _require_show_permission(selected_show_id, {"show_owner"})
    elif not _admin_is_super():
        abort(403, "Platform-wide contact export requires super admin access.")

    csv_bytes = export_marketing_contacts_csv(selected_show_id)
    filename = "consented-contacts.csv" if selected_show_id is None else f"consented-contacts-show-{selected_show_id}.csv"

    return send_file(
        io.BytesIO(csv_bytes),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )



def _sync_recent_paid_platform_vote_sessions(show: Any, lookback_hours: int = 12, limit: int = 100) -> int:
    """
    Emergency live-show safety net:
    Pull recent paid Stripe Checkout Sessions from the platform account and finalize any vote sessions.
    This helps when Stripe webhooks are delayed/misconfigured or the voter does not return to the success page.
    """
    if not show or not PLATFORM_STRIPE_SECRET_KEY:
        return 0

    synced = 0
    try:
        created_gte = int((datetime.now(timezone.utc) - timedelta(hours=lookback_hours)).timestamp())
        sessions = stripe.checkout.Session.list(
            limit=limit,
            created={"gte": created_gte},
        )

        for sess in sessions.auto_paging_iter():
            try:
                if getattr(sess, "payment_status", "") != "paid":
                    continue

                md = getattr(sess, "metadata", None) or {}
                if md.get("payment_item_type") != "vote":
                    continue

                if str(md.get("show_id", "")) != str(show["id"]):
                    continue

                finalize_vote_intent_paid(sess.id)
                synced += 1
            except Exception:
                continue
    except Exception:
        return synced

    return synced




@app.get("/admin/leaderboard")
@require_admin
def admin_leaderboard():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()

    # LIVE FIX 2026-04-25:
    # Before showing the leaderboard, catch up any paid platform Stripe vote sessions
    # that did not finalize through webhook/success return.
    _sync_recent_paid_platform_vote_sessions(show)

    return render_template(
        "leaderboard.html",
        show=show,
        by_category=leaderboard_by_category(int(show["id"]), start_date=start_date, end_date=end_date),
        overall=leaderboard_overall(int(show["id"]), start_date=start_date, end_date=end_date),
        start_date=start_date,
        end_date=end_date,
    )


@app.get("/admin/export-votes.csv")
@require_admin
def admin_export_votes():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    rows = export_votes_for_show(int(show["id"]), start_date=start_date, end_date=end_date)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "created_at",
        "category",
        "vote_qty",
        "amount_cents",
        "stripe_session_id",
        "entry_method",
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
            r["entry_method"] if "entry_method" in r.keys() else "car_qr",
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
    suffix = ""
    if start_date or end_date:
        suffix = f"_{start_date or 'start'}_to_{end_date or 'end'}".replace("/", "-")
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=f"votes_export{suffix}.csv")


@app.get("/admin/placeholders")
@require_admin
def admin_placeholders():
    show = _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))
    return render_template("admin_placeholders.html", show=show, cars=list_show_cars_public(int(show["id"])))


@app.post("/admin/placeholders/create")
@require_admin
def admin_placeholders_create():
    show = _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))
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



@app.post("/admin/placeholders/fill-to-max")
@require_admin
def admin_placeholders_fill_to_max():
    show = _admin_current_show()
    if not show:
        return "No accessible show.", 403
    _require_show_access(int(show["id"]))
    try:
        created = ensure_placeholder_cards_up_to_max(int(show["id"]))
        _log_event("admin.placeholders_filled_to_max", int(show["id"]), {"created": created}, actor_type="admin")
        flash(f"Created {created} open placeholder cards up to this show's Max Cars limit.", "ok")
    except ValueError as e:
        flash(str(e), "error")
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
                elif item_type == "vendor":
                    finalize_vendor_paid(
                        _db_path(),
                        session_id,
                        obj.get("payment_intent", "") or "",
                        int(obj.get("amount_total", 0) or 0),
                    )
                try:
                    _sync_actual_stripe_fee(session_id)
                except Exception as fee_error:
                    _log_event(
                        "stripe.actual_fee_sync_deferred",
                        int(metadata.get("show_id", "0") or 0) or None,
                        {"session_id": session_id, "error": str(fee_error)},
                    )

        if event_id:
            mark_webhook_event_processed(event_id, event_type)
        return jsonify({"ok": True})
    except Exception as e:
        return f"Webhook processing error: {e}", 500



@app.route("/admin/users", methods=["GET", "POST"])
@require_super_admin
def admin_users():
    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "create_user":
            name = request.form.get("name", "").strip()
            email = request.form.get("email", "").strip().lower()
            password = request.form.get("password", "")
            global_role = request.form.get("global_role", "show_owner").strip().lower()
            show_id_raw = request.form.get("show_id", "").strip()
            show_role = request.form.get("show_role", "show_owner").strip().lower()

            if not name or not email or not password:
                flash("Name, email, and password are required.", "error")
                return redirect(url_for("admin_users"))

            try:
                admin_user_id = create_admin_user(
                    name=name,
                    email=email,
                    password_hash=generate_password_hash(password),
                    global_role=global_role,
                    is_active=1,
                )
                if show_id_raw.isdigit() and global_role != "super_admin":
                    assign_admin_user_show_role(admin_user_id, int(show_id_raw), show_role)
                flash("Admin user created.", "ok")
            except Exception as e:
                flash(f"Could not create user: {e}", "error")

        elif action == "assign_role":
            admin_user_id_raw = request.form.get("admin_user_id", "").strip()
            show_id_raw = request.form.get("show_id", "").strip()
            role = request.form.get("role", "show_owner").strip().lower()
            if admin_user_id_raw.isdigit() and show_id_raw.isdigit():
                assign_admin_user_show_role(int(admin_user_id_raw), int(show_id_raw), role)
                flash("Show role assigned.", "ok")
            else:
                flash("Choose a user and a show.", "error")

        elif action == "deactivate_user":
            admin_user_id_raw = request.form.get("admin_user_id", "").strip()
            if admin_user_id_raw.isdigit():
                set_admin_user_active(int(admin_user_id_raw), 0)
                flash("Admin user deactivated.", "ok")

        elif action == "activate_user":
            admin_user_id_raw = request.form.get("admin_user_id", "").strip()
            if admin_user_id_raw.isdigit():
                set_admin_user_active(int(admin_user_id_raw), 1)
                flash("Admin user activated.", "ok")

        return redirect(url_for("admin_users"))

    users = list_admin_users()
    shows = list_shows_admin()
    roles = list_admin_user_show_roles()
    return render_template(
        "admin_users.html",
        show=_admin_current_show(),
        users=users,
        shows=shows,
        roles=roles,
    )


# ==========================================================
# Import Template Downloads - 0.9.2-beta v4
# ==========================================================

@app.get("/admin/shows/<int:show_id>/import/template/classes.csv")
@require_admin
def admin_download_judging_classes_template(show_id: int):
    _require_show_access(show_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["class_code", "class_name", "description", "sort_order", "award_places", "is_active"])
    writer.writerow(["PC", "People's Choice", "Overall favorite", "10", "3", "1"])
    writer.writerow(["BP", "Best Paint", "Best paint / finish", "20", "3", "1"])
    writer.writerow(["BI", "Best Interior", "Best interior", "30", "3", "1"])
    data = output.getvalue().encode("utf-8-sig")
    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name="judging_classes_template.csv",
    )


@app.get("/admin/shows/<int:show_id>/import/template/registrations.csv")
@require_admin
def admin_download_registration_template(show_id: int):
    _require_show_access(show_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "car_number",
        "owner_name",
        "owner_email",
        "owner_phone",
        "year",
        "make",
        "model",
        "class_code",
        "class_name",
        "registration_status",
        "custom_1",
        "custom_2",
    ])
    writer.writerow([
        "101",
        "Sample Owner",
        "owner@example.com",
        "555-555-5555",
        "1967",
        "Ford",
        "Mustang",
        "PC",
        "People's Choice",
        "paid",
        "",
        "",
    ])
    writer.writerow([
        "102",
        "Another Owner",
        "another@example.com",
        "555-555-1212",
        "1977",
        "MG",
        "MGB",
        "BP",
        "Best Paint",
        "paid",
        "",
        "",
    ])
    data = output.getvalue().encode("utf-8-sig")
    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name="accepted_registrations_template.csv",
    )


@app.get("/admin/shows/<int:show_id>/import/template/combined.csv")
@require_admin
def admin_download_combined_import_template(show_id: int):
    _require_show_access(show_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "record_type",
        "car_number",
        "owner_name",
        "owner_email",
        "owner_phone",
        "year",
        "make",
        "model",
        "class_code",
        "class_name",
        "description",
        "sort_order",
        "award_places",
        "is_active",
        "registration_status",
        "custom_1",
        "custom_2",
    ])
    writer.writerow(["class", "", "", "", "", "", "", "", "PC", "People's Choice", "Overall favorite", "10", "3", "1", "", "", ""])
    writer.writerow(["class", "", "", "", "", "", "", "", "BP", "Best Paint", "Best paint / finish", "20", "3", "1", "", "", ""])
    writer.writerow(["registration", "101", "Sample Owner", "owner@example.com", "555-555-5555", "1967", "Ford", "Mustang", "PC", "People's Choice", "", "", "", "", "paid", "", ""])
    writer.writerow(["registration", "102", "Another Owner", "another@example.com", "555-555-1212", "1977", "MG", "MGB", "BP", "Best Paint", "", "", "", "", "paid", "", ""])
    data = output.getvalue().encode("utf-8-sig")
    return send_file(
        io.BytesIO(data),
        mimetype="text/csv",
        as_attachment=True,
        download_name="combined_show_import_template.csv",
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
