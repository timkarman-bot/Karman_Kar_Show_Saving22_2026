#Here is the **complete replacement `app.py`** that matches the new `database.py` and the new templates we have been building toward.

#It is designed to compile with:

#* the **new `database.py`**
#* the new `admin.html`
#* the new `register.html`
#* the new `register_checkout.html`
#* the new `register_success.html`
#* the updated `vote_qty.html`

#It also supports:

#* **charity-owned Stripe Connect**
#* **webhook confirmation**
#* **registration checkout**
#* **voting checkout**
#* **attendance fee checkout**
#* **electronic waiver capture**
#* **admin pricing**
#* **charity connect / disconnect routes**

#A few honest notes before the file:

#1. This will compile as Python if your `database.py` matches the version I gave you.
#2. It assumes you will have an `attendee_fee.html` template after you rename/update that file.
#3. It keeps a few compatibility routes so older links do not instantly break.
#4. For Stripe Connect, set these environment variables:

#   * `PLATFORM_STRIPE_SECRET_KEY`
#   * `STRIPE_WEBHOOK_SECRET`
#   * `STRIPE_CLIENT_ID`
#   * `BASE_URL`
#   * `FLASK_SECRET`
#   * `ADMIN_PASSWORD`

python
# app.py
# Karman Kar Shows & Events — charity-owned Stripe Connect checkout + webhook confirmation
# 4-space indentation only (no tabs)

import os
import io
import csv
import secrets
from typing import Dict, Optional
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urlencode

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
from werkzeug.middleware.proxy_fix import ProxyFix
from functools import wraps

from database import (
    init_db,
    ensure_default_show,
    build_snapshot_zip_bytes,
    get_active_show,
    get_show_by_id,
    get_show_by_slug,
    toggle_show_voting,
    set_show_voting_open,
    update_show_admin_settings,
    set_show_charity_connect,
    clear_show_charity_connect,
    count_registered_cars,
    show_has_capacity,
    create_person,
    update_person,
    create_show_car,
    update_show_car_details,
    get_show_car_public_by_token,
    get_show_car_private_by_token,
    get_show_car_by_number,
    create_registration_intent,
    get_registration_intent_by_token,
    get_registration_intent_by_session,
    attach_stripe_session_to_registration_intent,
    finalize_registration_intent_paid,
    create_vote_intent,
    get_vote_intent_by_session,
    attach_stripe_session_to_vote_intent,
    finalize_vote_intent_paid,
    record_paid_votes,
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
    get_donation_by_id,
    get_donation_by_session,
    attach_stripe_session_to_donation,
    mark_donation_paid,
    waiver_mark_received,
    has_processed_webhook_event,
    mark_webhook_event_processed,
)

# ----------------------------
# BASIC CONFIG
# ----------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-me")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "change-me")
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

UPCOMING_EVENTS = [
    {
        "date": "May 23, 2026",
        "title": "Pop-Up Car Show (Certificates + People’s Choice)",
        "location": "Kansas City Metro (TBD)",
        "status": "Planning",
    },
    {
        "date": "June 20, 2026",
        "title": "Summer Cruise + Mini Show",
        "location": "Liberty, MO (TBD)",
        "status": "Planning",
    },
]

init_db()
ensure_default_show(DEFAULT_SHOW)

# ----------------------------
# HELPERS
# ----------------------------
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
    st = str(st).strip().lower()
    return st == "full"


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


def _format_cents(cents: int) -> str:
    return f"{(int(cents or 0) / 100):.2f}"


def _connected_account_id(show) -> Optional[str]:
    if not show:
        return None
    acct = (show["charity_stripe_account_id"] or "").strip() if "charity_stripe_account_id" in show.keys() else ""
    status = (show["charity_connect_status"] or "").strip() if "charity_connect_status" in show.keys() else ""
    if acct and status == "connected":
        return acct
    return None


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
        now_dt = datetime.now(ZoneInfo("America/Chicago"))
        if now_dt >= end_dt:
            set_show_voting_open(int(show["id"]), False)
    except Exception:
        return


@app.before_request
def before_request():
    _maybe_auto_close_voting()


# ----------------------------
# GLOBAL TEMPLATE VARS
# ----------------------------
@app.context_processor
def inject_globals():
    show = get_active_show()
    title_sponsor, sponsors = (None, [])
    registered_cars = 0

    if show:
        result = get_show_sponsors(int(show["id"])) or (None, [])
        title_sponsor, sponsors = result
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


# ----------------------------
# PUBLIC PAGES
# ----------------------------
@app.get("/")
def home():
    show = get_active_show()
    if not show:
        return "No active show configured.", 500
    return render_template("home.html", show=show)


@app.get("/instructions/<show_slug>")
def voting_instructions(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    return render_template("voting_instructions.html", show=show)


@app.get("/events")
def events():
    show = get_active_show()
    return render_template("events.html", show=show, events=UPCOMING_EVENTS)


@app.get("/show/<slug>")
def show_page(slug: str):
    show = get_show_by_slug(slug)
    if not show:
        return render_template("show.html", show={"title": "Show Not Found"}, not_found=True)

    cars = list_show_cars_public(int(show["id"]))
    return render_template("show.html", show=show, cars=cars, not_found=False)


# ----------------------------
# REGISTRATION
# ----------------------------
@app.get("/register")
def register_page():
    show = get_active_show()
    if not show:
        return "No active show configured.", 500

    if not prereg_allowed(show):
        return render_template("registration_closed.html", show=show), 403

    if not show_has_capacity(int(show["id"])):
        return render_template("registration_closed.html", show=show, error="This show is full."), 403

    return render_template("register.html", show=show)


@app.post("/register")
def register_submit():
    show = get_active_show()
    if not show:
        return "No active show configured.", 500

    if not prereg_allowed(show):
        return render_template("registration_closed.html", show=show), 403

    if not show_has_capacity(int(show["id"])):
        return render_template(
            "register.html",
            show=show,
            error="This show has reached its maximum number of cars.",
        )

    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip()
    opt_in_future = request.form.get("opt_in_future", "") == "on"
    sponsor_opt_in = request.form.get("sponsor_opt_in", "") == "on"

    car_number_raw = request.form.get("car_number", "").strip()
    year = request.form.get("year", "").strip()
    make = request.form.get("make", "").strip()
    model = request.form.get("model", "").strip()

    waiver_accepted = request.form.get("waiver_accepted", "") == "on"
    waiver_signed_name = request.form.get("waiver_signed_name", "").strip()

    if not (name and car_number_raw and year and make and model and waiver_signed_name):
        return render_template(
            "register.html",
            show=show,
            error="Please fill out all required fields.",
        )

    if (opt_in_future or sponsor_opt_in) and not phone:
        return render_template(
            "register.html",
            show=show,
            error="Phone number is required if you opt in to updates or sponsor information.",
        )

    if not waiver_accepted:
        return render_template(
            "register.html",
            show=show,
            error="You must accept the waiver to continue.",
        )

    try:
        car_number = int(car_number_raw)
        if car_number <= 0:
            raise ValueError()
    except ValueError:
        return render_template(
            "register.html",
            show=show,
            error="Car number must be a positive number.",
        )

    registration_fee_cents = int(show["registration_fee_cents"] or 0)
    waiver_text = (show["waiver_text"] or "").strip()
    waiver_version = (show["waiver_version"] or "").strip()

    try:
        registration_intent_id, intent_token = create_registration_intent(
            show_id=int(show["id"]),
            owner_name=name,
            phone=phone,
            email=email,
            opt_in_future=opt_in_future,
            sponsor_opt_in=sponsor_opt_in,
            car_number=car_number,
            year=year,
            make=make,
            model=model,
            waiver_accepted=waiver_accepted,
            waiver_signed_name=waiver_signed_name,
            waiver_text=waiver_text,
            waiver_version=waiver_version,
            amount_cents=registration_fee_cents,
        )
    except ValueError as e:
        return render_template("register.html", show=show, error=str(e))

    # Free registration path
    if registration_fee_cents <= 0:
        synthetic_session_id = f"free_reg_{intent_token}"
        attach_stripe_session_to_registration_intent(
            registration_intent_id=registration_intent_id,
            stripe_session_id=synthetic_session_id,
            stripe_payment_intent_id="",
        )
        result = finalize_registration_intent_paid(synthetic_session_id)
        car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
        return render_template("register_success.html", show=show, car=car)

    acct = _connected_account_id(show)
    if not acct:
        return render_template(
            "register.html",
            show=show,
            error="This show does not have a charity payment account connected yet. Please contact the organizer.",
        )

    _require_platform_stripe()

    success_url = (
        _abs_url(url_for("registration_success", show_slug=show["slug"], intent_token=intent_token))
        + "?session_id={CHECKOUT_SESSION_ID}"
    )
    cancel_url = _abs_url(url_for("register_page"))

    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "unit_amount": registration_fee_cents,
                    "product_data": {
                        "name": f"Registration – {show['title']}",
                    },
                },
                "quantity": 1,
            }
        ],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "payment_item_type": "registration",
            "show_id": str(show["id"]),
            "show_slug": show["slug"],
            "registration_intent_id": str(registration_intent_id),
            "intent_token": intent_token,
        },
        stripe_account=acct,
    )

    attach_stripe_session_to_registration_intent(
        registration_intent_id=registration_intent_id,
        stripe_session_id=session_obj.id,
        stripe_payment_intent_id="",
    )

    car_summary = {
        "year": year,
        "make": make,
        "model": model,
    }

    return render_template(
        "register_checkout.html",
        show=show,
        car=car_summary,
        car_number=car_number,
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

    # If already finalized, render success directly
    if ri["finalized_show_car_id"]:
        result = finalize_registration_intent_paid(ri["stripe_session_id"])
        car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
        return render_template("register_success.html", show=show, car=car)

    if not session_id:
        return render_template("payment_not_complete.html")

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

    result = finalize_registration_intent_paid(sess.id)
    car = get_show_car_public_by_token(int(show["id"]), result["car_token"])
    return render_template("register_success.html", show=show, car=car)


# ----------------------------
# PRINT / WINDSHIELD CARDS
# ----------------------------
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


# ----------------------------
# OWNER CHECK-IN
# ----------------------------
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
def checkin_submit(show_slug: str, car_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404

    car_private = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car_private:
        return "Car not found.", 404

    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip()
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

    update_show_car_details(
        show_car_id=int(car_private["id"]),
        year=year,
        make=make,
        model=model,
    )

    car_private2 = get_show_car_private_by_token(int(show["id"]), car_token)
    return render_template("checkin.html", show=show, car=car_private2, success="Check-in complete. You're all set!")


# ----------------------------
# WAIVER PRINT
# ----------------------------
@app.get("/waiver/<show_slug>/<car_token>")
def waiver_print(show_slug: str, car_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404

    car = get_show_car_private_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404

    return render_template("waiver_print.html", show=show, car=car)


# ----------------------------
# ATTENDEE CAPTURE + ATTENDANCE FEE
# ----------------------------
@app.get("/attend/<show_slug>")
def attendee_page(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    return render_template("attendee.html", show=show)


@app.post("/attend/<show_slug>")
def attendee_submit(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404

    first_name = request.form.get("first_name", "").strip()
    last_name = request.form.get("last_name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip()
    zip_code = request.form.get("zip", "").strip()

    sponsor_opt_in = request.form.get("sponsor_opt_in", "") == "on"
    updates_opt_in = request.form.get("updates_opt_in", "") == "on"

    if not (first_name and last_name):
        return render_template("attendee.html", show=show, error="First and last name are required.")

    if (sponsor_opt_in or updates_opt_in) and not phone:
        return render_template(
            "attendee.html",
            show=show,
            error="Phone number is required if you choose to receive updates or sponsor information.",
        )

    attendee_id = create_attendee(
        show_id=int(show["id"]),
        first_name=first_name,
        last_name=last_name,
        phone=phone,
        email=email,
        zip_code=zip_code,
        sponsor_opt_in=sponsor_opt_in,
        updates_opt_in=updates_opt_in,
        consent_text=ATTENDEE_CONSENT_TEXT,
        consent_version=ATTENDEE_CONSENT_VERSION,
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


# Compatibility old route
@app.get("/attend/<show_slug>/donate/<int:attendee_id>")
def attendee_donate_page(show_slug: str, attendee_id: int):
    return redirect(url_for("attendee_fee_page", show_slug=show_slug, attendee_id=attendee_id))


@app.post("/attend/create-fee-checkout")
@app.post("/attend/create-donation-checkout")
def create_attendee_fee_checkout():
    show_slug = request.form.get("show_slug", "").strip()
    attendee_id_raw = request.form.get("attendee_id", "").strip()
    amount_raw = request.form.get("amount_dollars", "").strip()

    show = get_show_by_slug(show_slug)
    if not show:
        return jsonify({"ok": False, "error": "Show not found."}), 404

    try:
        attendee_id = int(attendee_id_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid attendee."}), 400

    if amount_raw:
        amount_cents = _parse_dollars_to_cents(amount_raw, default_cents=int(show["attendee_fee_cents"] or 0))
    else:
        amount_cents = int(show["attendee_fee_cents"] or 0)

    if amount_cents <= 0:
        create_donation_row(int(show["id"]), attendee_id, 0, "skipped")
        return jsonify(
            {
                "ok": True,
                "skipped": True,
                "redirect_url": url_for("attendee_done", show_slug=show_slug),
            }
        )

    acct = _connected_account_id(show)
    if not acct:
        return jsonify({"ok": False, "error": "The charity payment account is not connected for this show."}), 400

    _require_platform_stripe()

    fee_row_id = create_donation_row(int(show["id"]), attendee_id, amount_cents, "pending")

    success_url = _abs_url(url_for("attendee_fee_success", show_slug=show_slug)) + "?session_id={CHECKOUT_SESSION_ID}"
    cancel_url = _abs_url(url_for("attendee_fee_page", show_slug=show_slug, attendee_id=attendee_id))

    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "unit_amount": amount_cents,
                    "product_data": {"name": f"Attendance Fee – {show['title']}"},
                },
                "quantity": 1,
            }
        ],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "payment_item_type": "attendance_fee",
            "show_id": str(show["id"]),
            "show_slug": show_slug,
            "donation_id": str(fee_row_id),
        },
        stripe_account=acct,
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

    mark_donation_paid(sess.id)
    return redirect(url_for("attendee_done", show_slug=show["slug"]))


@app.get("/attend/<show_slug>/done")
def attendee_done(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    return render_template("attendee_done.html", show=show)


# ----------------------------
# QR VOTING
# ----------------------------
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

    category_name = CATEGORY_SLUGS[category_slug]
    return render_template(
        "vote_qty.html",
        show=show,
        car=car,
        category_slug=category_slug,
        category_name=category_name,
        vote_price_cents=int(show["vote_price_cents"] or 100),
    )


@app.post("/create-checkout-session")
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

    acct = _connected_account_id(show)
    if not acct:
        return jsonify({"ok": False, "error": "The charity payment account is not connected for this show."}), 400

    if category_slug not in CATEGORY_SLUGS:
        return jsonify({"ok": False, "error": "Invalid category."}), 400

    car = get_show_car_public_by_token(int(show["id"]), car_token)
    if not car:
        return jsonify({"ok": False, "error": "Car not found."}), 404

    try:
        vote_qty = int(qty_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid vote quantity."}), 400

    if vote_qty < 1 or vote_qty > 50:
        return jsonify({"ok": False, "error": "Vote quantity must be between 1 and 50."}), 400

    vote_price_cents = int(show["vote_price_cents"] or 100)
    amount_cents = vote_qty * vote_price_cents

    _require_platform_stripe()

    vote_intent_id = create_vote_intent(
        show_id=int(show["id"]),
        show_car_id=int(car["id"]),
        category=CATEGORY_SLUGS[category_slug],
        vote_qty=vote_qty,
        amount_cents=amount_cents,
    )

    success_url = _abs_url(url_for("vote_success")) + "?session_id={CHECKOUT_SESSION_ID}&show_slug=" + show_slug
    cancel_url = _abs_url(
        url_for("vote_qty_page", show_slug=show_slug, car_token=car_token, category_slug=category_slug)
    )

    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "unit_amount": vote_price_cents,
                    "product_data": {
                        "name": f"Vote – {CATEGORY_SLUGS[category_slug]} (Car #{car['car_number']})",
                    },
                },
                "quantity": vote_qty,
            }
        ],
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
        stripe_account=acct,
    )

    attach_stripe_session_to_vote_intent(
        vote_intent_id=vote_intent_id,
        stripe_session_id=session_obj.id,
        stripe_payment_intent_id="",
    )

    return jsonify({"ok": True, "checkout_url": session_obj.url})


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
    return render_template("vote_success.html")


# ----------------------------
# ADMIN
# ----------------------------
@app.get("/admin")
def admin_page():
    show = get_active_show()
    next_url = request.args.get("next", "")
    if not session.get("admin_authed"):
        return render_template("admin.html", show=show, authed=False, next=next_url)
    return render_template("admin.html", show=show, authed=True, next=next_url)


@app.post("/admin/login")
def admin_login():
    pw = request.form.get("password", "")
    next_url = request.form.get("next", "") or url_for("admin_page")

    show = get_active_show()
    if pw == ADMIN_PASSWORD:
        session["admin_authed"] = True
        return redirect(next_url)

    return render_template(
        "admin.html",
        show=show,
        authed=False,
        login_error="Incorrect password.",
        next=next_url,
    )


@app.post("/admin/logout")
@require_admin
def admin_logout():
    session.pop("admin_authed", None)
    return redirect(url_for("admin_page"))


@app.get("/admin/stripe/connect")
@require_admin
def admin_connect_charity_stripe():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    _require_platform_stripe()
    auth_url = _build_connect_authorize_url(int(show["id"]), show["slug"])
    return redirect(auth_url)


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

        set_show_charity_connect(
            show_id=int(show_id),
            stripe_account_id=stripe_account_id,
            connect_status="connected",
            connect_email=connect_email,
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
    if ov_raw == "":
        ov = None
    else:
        try:
            ov = int(ov_raw)
        except ValueError:
            ov = None

    max_cars_raw = request.form.get("max_cars", "").strip()
    if max_cars_raw == "":
        max_cars = None
    else:
        try:
            max_cars = int(max_cars_raw)
        except ValueError:
            max_cars = None

    registration_fee_cents = _parse_dollars_to_cents(
        request.form.get("registration_fee_dollars", ""),
        default_cents=int(show["registration_fee_cents"] or 0),
    )
    attendee_fee_cents = _parse_dollars_to_cents(
        request.form.get("attendee_fee_dollars", ""),
        default_cents=int(show["attendee_fee_cents"] or 0),
    )
    vote_price_cents = _parse_dollars_to_cents(
        request.form.get("vote_price_dollars", ""),
        default_cents=int(show["vote_price_cents"] or 100),
    )
    if vote_price_cents <= 0:
        vote_price_cents = 100

    update_show_admin_settings(
        show_id=int(show["id"]),
        show_type=show_type,
        allow_prereg_override=ov,
        max_cars=max_cars,
        registration_fee_cents=registration_fee_cents,
        attendee_fee_cents=attendee_fee_cents,
        vote_price_cents=vote_price_cents,
        public_vote_disclosure=request.form.get("public_vote_disclosure", ""),
        public_registration_disclosure=request.form.get("public_registration_disclosure", ""),
        public_donation_disclosure=request.form.get("public_donation_disclosure", ""),
        waiver_text=request.form.get("waiver_text", ""),
        waiver_version=request.form.get("waiver_version", ""),
    )

    flash("Show settings saved.", "ok")
    return redirect(url_for("admin_page"))


@app.get("/admin/print-cards.pdf")
@require_admin
def admin_print_cards_pdf():
    from utils.print_cards import build_landscape_cards_pdf

    show = get_active_show()
    if not show:
        return "No active show.", 500

    ids_raw = request.args.get("ids", "").strip()
    all_raw = request.args.get("all", "").strip()
    back_raw = request.args.get("back", "").strip()

    include_back = back_raw == "1"

    cars = list_show_cars_public(int(show["id"]))
    if not cars:
        return "No cars to print.", 400

    selected = cars
    if all_raw != "1":
        want_ids = set()
        if ids_raw:
            for part in ids_raw.split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    want_ids.add(int(part))
                except ValueError:
                    pass

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
        mirror_back_pages=True,
    )

    fname = f"{show['slug']}-voting-cards-landscape.pdf"
    return send_file(
        io.BytesIO(pdf_bytes),
        mimetype="application/pdf",
        as_attachment=True,
        download_name=fname,
    )


@app.get("/admin/export-snapshot.zip")
@require_admin
def admin_export_snapshot_zip():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    zip_bytes, filename = build_snapshot_zip_bytes(int(show["id"]))
    return send_file(
        io.BytesIO(zip_bytes),
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


@app.post("/admin/close-voting-and-export")
@require_admin
def admin_close_voting_and_export():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    set_show_voting_open(int(show["id"]), False)

    zip_bytes, filename = build_snapshot_zip_bytes(int(show["id"]))
    return send_file(
        io.BytesIO(zip_bytes),
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


@app.post("/admin/toggle-voting")
@require_admin
def admin_toggle_voting():
    show = get_active_show()
    if show:
        toggle_show_voting(int(show["id"]))
    return redirect(url_for("admin_page"))


@app.post("/admin/open-voting")
@require_admin
def admin_open_voting():
    show = get_active_show()
    if show:
        set_show_voting_open(int(show["id"]), True)
    return redirect(url_for("admin_page"))


@app.post("/admin/close-voting")
@require_admin
def admin_close_voting():
    show = get_active_show()
    if show:
        set_show_voting_open(int(show["id"]), False)
    return redirect(url_for("admin_page"))


@app.post("/admin/reset-votes")
@require_admin
def admin_reset_votes():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    zip_bytes, filename = build_snapshot_zip_bytes(int(show["id"]))
    reset_votes_for_show(int(show["id"]))

    return send_file(
        io.BytesIO(zip_bytes),
        mimetype="application/zip",
        as_attachment=True,
        download_name=filename,
    )


@app.get("/admin/leaderboard")
@require_admin
def admin_leaderboard():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    by_cat = leaderboard_by_category(int(show["id"]))
    overall = leaderboard_overall(int(show["id"]))
    return render_template("leaderboard.html", show=show, by_category=by_cat, overall=overall)


@app.get("/admin/export-votes.csv")
@require_admin
def admin_export_votes():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    rows = export_votes_for_show(int(show["id"]))

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(
        [
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
        ]
    )
    for r in rows:
        w.writerow(
            [
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
            ]
        )

    mem = io.BytesIO(buf.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="votes_export.csv")


@app.get("/admin/placeholders")
@require_admin
def admin_placeholders():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    cars = list_show_cars_public(int(show["id"]))
    return render_template("admin_placeholders.html", show=show, cars=cars)


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
    flash(f"Created {created} placeholder cars.", "ok")
    return redirect(url_for("admin_placeholders"))


@app.post("/admin/waiver-received")
@require_admin
def admin_waiver_received():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    show_car_id_raw = request.form.get("show_car_id", "").strip()
    try:
        show_car_id = int(show_car_id_raw)
    except ValueError:
        return redirect(url_for("admin_placeholders"))

    waiver_mark_received(int(show["id"]), show_car_id, received_by="admin")
    flash("Waiver marked as received.", "ok")
    return redirect(url_for("admin_placeholders"))


@app.get("/admin/sponsors")
@require_admin
def admin_sponsors():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    result = get_show_sponsors(int(show["id"])) or (None, [])
    title_sponsor, sponsors = result
    return render_template("admin_sponsors.html", show=show, title_sponsor=title_sponsor, sponsors=sponsors)


@app.post("/admin/sponsors/add")
@require_admin
def admin_sponsors_add():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    name = request.form.get("name", "").strip()
    logo_path = request.form.get("logo_path", "").strip()
    website_url = request.form.get("website_url", "").strip()
    placement = request.form.get("placement", "standard").strip()
    sort_order_raw = request.form.get("sort_order", "100").strip()

    if not name:
        flash("Sponsor name is required.", "error")
        return redirect(url_for("admin_sponsors"))

    try:
        sort_order = int(sort_order_raw)
    except ValueError:
        sort_order = 100

    sponsor_id = upsert_sponsor(name=name, logo_path=logo_path, website_url=website_url)

    if placement == "title":
        set_title_sponsor(int(show["id"]), sponsor_id)
    else:
        attach_sponsor_to_show(int(show["id"]), sponsor_id, placement="standard", sort_order=sort_order)

    flash("Sponsor saved.", "ok")
    return redirect(url_for("admin_sponsors"))


@app.post("/admin/sponsors/remove")
@require_admin
def admin_sponsors_remove():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    sponsor_id_raw = request.form.get("sponsor_id", "").strip()
    try:
        sponsor_id = int(sponsor_id_raw)
    except ValueError:
        return redirect(url_for("admin_sponsors"))

    remove_sponsor_from_show(int(show["id"]), sponsor_id)
    flash("Sponsor removed from show.", "ok")
    return redirect(url_for("admin_sponsors"))


@app.get("/admin/debug/routes")
@require_admin
def admin_debug_routes():
    routes = []
    for rule in app.url_map.iter_rules():
        methods = sorted(m for m in rule.methods if m not in ("HEAD", "OPTIONS"))
        routes.append(
            {
                "rule": str(rule),
                "endpoint": rule.endpoint,
                "methods": methods,
            }
        )
    routes.sort(key=lambda r: r["rule"])
    return {"count": len(routes), "routes": routes}


# ----------------------------
# WEBHOOK
# ----------------------------
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
                elif item_type == "vote":
                    finalize_vote_intent_paid(session_id)
                elif item_type == "attendance_fee":
                    mark_donation_paid(session_id)

        if event_id:
            mark_webhook_event_processed(event_id, event_type)

        return jsonify({"ok": True})
    except Exception as e:
        return f"Webhook processing error: {e}", 500


# ----------------------------
# RUN
# ----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
```

## Important compile / integration notes

#This `app.py` is matched to the **new database file**, not your old one.

#A few things to check right after pasting:

### 1. Environment variables

#Set these in Railway:

#```bash
#PLATFORM_STRIPE_SECRET_KEY=sk_...
#STRIPE_WEBHOOK_SECRET=whsec_...
#STRIPE_CLIENT_ID=ca_...
#BASE_URL=https://your-app-url.up.railway.app
#FLASK_SECRET=change-me
#ADMIN_PASSWORD=change-me
#```

### 2. Templates expected by this file

#This file expects these to exist:

#* `admin.html`
#* `register.html`
#* `register_checkout.html`
#* `register_success.html`
#* `vote_qty.html`
#* `attendee_fee.html`
#* `attendee.html`
#* `attendee_done.html`
#* `payment_not_complete.html`

### 3. One likely runtime issue if not yet updated

#You renamed the attendance template direction, so if `templates/attendee_fee.html` does not exist yet, this route will fail at runtime:

#```python
#return render_template("attendee_fee.html", show=show, attendee_id=attendee_id)
#```

### 4. Backward compatibility included

#I kept these compatibility paths:

#* old donation route redirects to fee route
#* old `/attend/create-donation-checkout` still works
#* old `/donation-success` still works

### 5. Next best file to generate

#The next file that should be created or updated is:

#```text
#templates/attendee_fee.html
#```

#because this new `app.py` now expects that filename.
