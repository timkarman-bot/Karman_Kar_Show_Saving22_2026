# app.py
# Karman Kar Shows & Events — Car show registration + QR voting + admin controls
# 4-space indentation only (no tabs)

import os
import io
import csv
from typing import Dict
from datetime import datetime
from zoneinfo import ZoneInfo

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
    get_show_by_slug,
    toggle_show_voting,
    set_show_voting_open,
    # registration / checkin
    create_person,
    update_person,
    create_show_car,
    update_show_car_details,
    get_show_car_public_by_token,
    get_show_car_private_by_token,
    get_show_car_by_number,
    # voting
    record_paid_votes,
    reset_votes_for_show,
    export_votes_for_show,
    leaderboard_by_category,
    leaderboard_overall,
    # placeholder cars (pre-print)
    create_placeholder_cars,
    list_show_cars_public,
    # sponsors
    get_show_sponsors,
    upsert_sponsor,
    attach_sponsor_to_show,
    remove_sponsor_from_show,
    set_title_sponsor,
    # attendee capture + donation
    create_attendee,
    record_field_metric,
    create_donation_row,
    attach_stripe_session_to_donation,
    mark_donation_paid,
    # waiver tracking
    waiver_mark_received,
)

# ----------------------------
# BASIC CONFIG
# ----------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-me")

#--------------------------------------------
# Add a temporary route lister (fast proof)
#--------------------------------------------

 @app.get("/admin/debug/routes")
 @require_admin
 def admin_debug_routes():
    rules = []
   for r in app.url_map.iter_rules():
        rules.append({
            "rule": str(r),
            "endpoint": r.endpoint,
            "methods": sorted([m for m in r.methods if m not in ("HEAD","OPTIONS")]),
        })
    rules.sort(key=lambda x: x["rule"])
    return {"count": len(rules), "routes": rules}



# Railway / reverse-proxy friendly (fixes _external URLs and scheme)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "change-me")

stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
BASE_URL = os.getenv("BASE_URL", "")  # e.g. https://<yourapp>.up.railway.app

VOTE_PRICE_CENTS = 100  # $1 per vote

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
    "description": "A charity car show supporting veteran suicide awareness with judged certificates by branch favorites and People’s Choice.",
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

# ----------------------------
# INIT DB + ENSURE DEFAULT SHOW
# ----------------------------
init_db()
ensure_default_show(DEFAULT_SHOW)

# ----------------------------
# HELPERS
# ----------------------------
def _require_stripe() -> None:
    if not stripe.api_key:
        abort(500, "Stripe is not configured. Set STRIPE_SECRET_KEY in Railway variables.")


def _abs_url(path: str) -> str:
    """
    Stripe requires absolute URLs.
    Uses BASE_URL if set; else request.url_root.
    'path' must start with '/'.
    """
    if BASE_URL:
        return BASE_URL.rstrip("/") + path
    return request.url_root.rstrip("/") + path


def require_admin(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("admin_authed"):
            return redirect(url_for("admin_page", next=request.path))
        return view_func(*args, **kwargs)

    return wrapped


def _maybe_auto_close_voting() -> None:
    """
    If VOTING_END is set and we've passed it (America/Chicago), automatically close voting.
    Format: "YYYY-MM-DD HH:MM"
    """
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
    if show:
        result = get_show_sponsors(int(show["id"])) or (None, [])
        title_sponsor, sponsors = result

    return {
        "active_show": show,
        "CATEGORY_SLUGS": CATEGORY_SLUGS,
        "CATEGORY_NAMES": list(CATEGORY_SLUGS.values()),
        "title_sponsor": title_sponsor,
        "sponsors": sponsors,
        "is_admin": session.get("admin_authed", False),
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
# REGISTRATION (direct registration)
# ----------------------------
@app.get("/register")
def register_page():
    show = get_active_show()
    if not show:
        return "No active show configured.", 500
    return render_template("register.html", show=show)


@app.post("/register")
def register_submit():
    show = get_active_show()
    if not show:
        return "No active show configured.", 500

    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip()
    opt_in_future = request.form.get("opt_in_future", "") == "on"

    car_number_raw = request.form.get("car_number", "").strip()
    year = request.form.get("year", "").strip()
    make = request.form.get("make", "").strip()
    model = request.form.get("model", "").strip()

    if not (name and phone and email and car_number_raw and year and make and model):
        return render_template("register.html", show=show, error="Please fill out all required fields.")

    try:
        car_number = int(car_number_raw)
        if car_number <= 0:
            raise ValueError()
    except ValueError:
        return render_template("register.html", show=show, error="Car number must be a positive number.")

    person_id = create_person(name=name, phone=phone, email=email, opt_in_future=opt_in_future)

    try:
        _, car_token = create_show_car(
            show_id=int(show["id"]),
            person_id=person_id,
            car_number=car_number,
            year=year,
            make=make,
            model=model,
        )
    except ValueError as e:
        return render_template("register.html", show=show, error=str(e))

    return redirect(url_for("registration_complete", show_slug=show["slug"], car_token=car_token))


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


# ----------------------------
# OWNER CHECK-IN (preprinted workflow)
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
# WAIVER (PRINTABLE, PAPER-FIRST)
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
# ATTENDEE CAPTURE + OPTIONAL DONATION
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

    consent_text = (
        "By selecting these options, you agree Karman Kar Shows & Events may contact you about the event and, "
        "if selected, share sponsor offers. Msg/data rates may apply. Opt out anytime."
    )
    consent_version = "2026-02-24"

    attendee_id = create_attendee(
        show_id=int(show["id"]),
        first_name=first_name,
        last_name=last_name,
        phone=phone,
        email=email,
        zip_code=zip_code,
        sponsor_opt_in=sponsor_opt_in,
        updates_opt_in=updates_opt_in,
        consent_text=consent_text,
        consent_version=consent_version,
    )

    record_field_metric(int(show["id"]), "phone", bool(phone))
    record_field_metric(int(show["id"]), "email", bool(email))

    return redirect(url_for("attendee_donate_page", show_slug=show_slug, attendee_id=attendee_id))


@app.get("/attend/<show_slug>/donate/<int:attendee_id>")
def attendee_donate_page(show_slug: str, attendee_id: int):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    return render_template("attendee_donate.html", show=show, attendee_id=attendee_id)


@app.post("/attend/create-donation-checkout")
def create_donation_checkout():
    _require_stripe()

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

    try:
        dollars = float(amount_raw)
    except ValueError:
        dollars = 0.0

    amount_cents = int(round(max(dollars, 0.0) * 100))

    if amount_cents == 0:
        create_donation_row(int(show["id"]), attendee_id, 0, "skipped")
        return jsonify(
            {
                "ok": True,
                "skipped": True,
                "redirect_url": url_for("attendee_done", show_slug=show_slug),
            }
        )

    donation_id = create_donation_row(int(show["id"]), attendee_id, amount_cents, "pending")

    success_url = _abs_url(url_for("donation_success")) + "?session_id={CHECKOUT_SESSION_ID}"
    cancel_url = _abs_url(url_for("attendee_donate_page", show_slug=show_slug, attendee_id=attendee_id))

    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[
            {
                "price_data": {
                    "currency": "usd",
                    "unit_amount": amount_cents,
                    "product_data": {"name": f"Donation – {show['title']}"},
                },
                "quantity": 1,
            }
        ],
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "show_id": str(show["id"]),
            "donation_id": str(donation_id),
            "show_slug": show_slug,
        },
    )

    attach_stripe_session_to_donation(donation_id, session_obj.id)
    return jsonify({"ok": True, "checkout_url": session_obj.url})


@app.get("/donation-success")
def donation_success():
    _require_stripe()

    session_id = request.args.get("session_id", "").strip()
    if not session_id:
        return "Missing session_id.", 400

    sess = stripe.checkout.Session.retrieve(session_id)
    if sess.payment_status != "paid":
        return render_template("payment_not_complete.html")

    mark_donation_paid(sess.id)

    show_slug = (sess.metadata or {}).get("show_slug", "")
    if not show_slug:
        show = get_active_show()
        show_slug = show["slug"] if show else ""

    return redirect(url_for("attendee_done", show_slug=show_slug))


@app.get("/attend/<show_slug>/done")
def attendee_done(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    return render_template("attendee_done.html", show=show)


# ----------------------------
# QR VOTING (CATEGORY LOCKED)
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
        vote_price_cents=VOTE_PRICE_CENTS,
    )


@app.post("/create-checkout-session")
def create_checkout_session():
    _require_stripe()

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

    if vote_qty < 1 or vote_qty > 50:
        return jsonify({"ok": False, "error": "Vote quantity must be between 1 and 50."}), 400

    success_url = _abs_url(url_for("vote_success")) + "?session_id={CHECKOUT_SESSION_ID}"
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
                    "unit_amount": VOTE_PRICE_CENTS,
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
            "show_id": str(show["id"]),
            "show_car_id": str(car["id"]),
            "category": CATEGORY_SLUGS[category_slug],
            "vote_qty": str(vote_qty),
        },
    )

    return jsonify({"ok": True, "checkout_url": session_obj.url})


@app.get("/success")
def vote_success():
    _require_stripe()

    session_id = request.args.get("session_id", "").strip()
    if not session_id:
        return "Missing session_id.", 400

    sess = stripe.checkout.Session.retrieve(session_id)
    if sess.payment_status != "paid":
        return render_template("payment_not_complete.html")

    md = sess.metadata or {}
    show_id = int(md.get("show_id", "0") or "0")
    show_car_id = int(md.get("show_car_id", "0") or "0")
    category = md.get("category", "")
    vote_qty = int(md.get("vote_qty", "0") or "0")

    if not (show_id and show_car_id and category and vote_qty):
        return "Invalid metadata.", 500

    record_paid_votes(
        show_id=show_id,
        show_car_id=show_car_id,
        category=category,
        vote_qty=vote_qty,
        amount_cents=int(sess.amount_total or 0),
        stripe_session_id=sess.id,
    )

    return render_template("vote_success.html")


# ----------------------------
# ADMIN (RESTRICTED)
# ----------------------------
@app.get("/admin")
def admin_page():
    """
    Publicly reachable, but only shows the dashboard if logged in.
    Otherwise shows the login form.
    """
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


# ---- Snapshot export + close voting/export ----
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
    """
    One-click: closes voting and downloads a snapshot ZIP immediately.
    """
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


# ---- Voting controls ----
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


# ---- Reset votes (export snapshot FIRST) ----
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


# ---- Leaderboard / exports ----
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


# ---- Placeholder cars ----
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


# ---- Sponsors ----
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
    logo_path = request.form.get("logo_path", "").strip()  # e.g. img/sponsors/acme.png
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

#-----------------------------
# Temporary Debug route Start removed 2/25/2026
#-----------------------------

#@app.get("/admin/debug/db")
#@require_admin
#def admin_debug_db():
#    import os
#    from database import DB_PATH, get_active_show
#    show = get_active_show()
#    return {
#        "db_path": DB_PATH,
#        "db_exists": os.path.exists(DB_PATH),
#        "db_size_bytes": os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0,
#        "active_show_slug": show["slug"] if show else None,
#        "active_show_id": int(show["id"]) if show else None,
#    }




#---------------------------
# Temporary Debug route End
#---------------------------


#---------------------------
# Temporary Debug route End
#---------------------------
@app.get("/admin/debug/routes")
@require_admin
def admin_debug_routes():
    routes = []
    for rule in app.url_map.iter_rules():
        methods = sorted(m for m in rule.methods if m not in ("HEAD", "OPTIONS"))
        routes.append({
            "rule": str(rule),
            "endpoint": rule.endpoint,
            "methods": methods,
        })
    routes.sort(key=lambda r: r["rule"])
    return {"count": len(routes), "routes": routes}


# ----------------------------
# RUN
# ----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
