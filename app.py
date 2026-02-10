import os
import io
import csv
from typing import Dict

import stripe
from flask import (
    Flask, render_template, request, redirect, url_for,
    jsonify, session, send_file, abort
)
from werkzeug.middleware.proxy_fix import ProxyFix


from database import (
    init_db,
    ensure_default_show,
    get_active_show,
    get_show_by_slug,
    toggle_show_voting,
    set_show_voting_open,
    create_person,
    create_show_car,
    get_show_car_by_token,
    record_paid_votes,
    reset_votes_for_show,
    export_votes_for_show,
    leaderboard_by_category,
    leaderboard_overall,
)

# ----------------------------
# BASIC CONFIG
# ----------------------------
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-me")

# Railway / reverse-proxy friendly (fixes _external URLs)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)


ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "change-me")

stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
BASE_URL = os.getenv("BASE_URL", "")  # set in Railway: https://<yourapp>.up.railway.app

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
    "benefiting": "Saving 22 / 22 Survivor Awareness",
    "suggested_donation": "$35 suggested donation for show cars",
    "description": "A charity car show supporting veteran suicide awareness with judged certificates by branch favorites and People’s Choice.",
}

UPCOMING_EVENTS = [
    {"date": "May 23, 2026", "title": "Pop-Up Car Show (Certificates + People’s Choice)", "location": "Kansas City Metro (TBD)", "status": "Planning"},
    {"date": "June 20, 2026", "title": "Summer Cruise + Mini Show", "location": "Liberty, MO (TBD)", "status": "Planning"},
]

# ----------------------------
# INIT DB + ENSURE DEFAULT SHOW
# ----------------------------
init_db()
ensure_default_show(DEFAULT_SHOW)

# ----------------------------
# GLOBAL TEMPLATE VARS
# ----------------------------
@app.context_processor
def inject_globals():
    show = get_active_show()
    return {
        "active_show": show,
        "CATEGORY_SLUGS": CATEGORY_SLUGS,
        "CATEGORY_NAMES": list(CATEGORY_SLUGS.values()),
    }


def _require_stripe():
    if not stripe.api_key:
        abort(500, "Stripe is not configured. Set STRIPE_SECRET_KEY.")


def _abs_url(endpoint: str, **values) -> str:
    """
    Build a fully qualified URL.
    Prefers BASE_URL (Railway variable). Falls back to Flask _external URLs
    (ProxyFix makes these correct behind Railway).
    """
    path = url_for(endpoint, _external=False, **values)
    if BASE_URL:
        return BASE_URL.rstrip("/") + path
    return url_for(endpoint, _external=True, **values)


# ----------------------------
# PUBLIC PAGES
# ----------------------------
@app.get("/")
def home():
    show = get_active_show()
    if not show:
        return "No active show configured.", 500
    return render_template("home.html", show=show)


@app.get("/events")
def events():
    show = get_active_show()
    return render_template("events.html", show=show, events=UPCOMING_EVENTS)


@app.get("/show/<slug>")
def show_page(slug: str):
    show = get_show_by_slug(slug)
    if not show:
        return render_template("show.html", show={"title": "Show Not Found"}, not_found=True)
    return render_template("show.html", show=show, not_found=False)


# ----------------------------
# REGISTRATION
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
    opt_in_future = (request.form.get("opt_in_future", "") == "on")

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


@app.get("/r/<show_slug>/<car_token>")
def registration_complete(show_slug: str, car_token: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404

    car = get_show_car_by_token(int(show["id"]), car_token)
    if not car:
        return "Car not found.", 404

    return render_template("registration_complete.html", show=show, car=car)


# ----------------------------
# QR VOTING (CATEGORY LOCKED)
# /v/<show_slug>/<car_token>/<category_slug>
# ----------------------------
@app.get("/v/<show_slug>/<car_token>/<category_slug>")
def vote_qty_page(show_slug: str, car_token: str, category_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404

    if category_slug not in CATEGORY_SLUGS:
        return "Invalid category.", 404

    car = get_show_car_by_token(int(show["id"]), car_token)
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
        category_name=category_name
    )


@app.post("/create-chec@app.post("/create-checkout-session")
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

    car = get_show_car_by_token(int(show["id"]), car_token)
    if not car:
        return jsonify({"ok": False, "error": "Car not found."}), 404

    try:
        vote_qty = int(qty_raw)
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid vote quantity."}), 400

    if vote_qty < 1 or vote_qty > 50:
        return jsonify({"ok": False, "error": "Vote quantity must be between 1 and 50."}), 400

    success_url = _abs_url(url_for("vote_success")) + "?session_id={CHECKOUT_SESSION_ID}"
    cancel_url = _abs_url(url_for(
        "vote_qty_page",
        show_slug=show_slug,
        car_token=car_token,
        category_slug=category_slug
    ))

    session_obj = stripe.checkout.Session.create(
        mode="payment",
        payment_method_types=["card"],
        line_items=[{
            "price_data": {
                "currency": "usd",
                "unit_amount": VOTE_PRICE_CENTS,
                "product_data": {
                    "name": f"Vote - {CATEGORY_SLUGS[category_slug]} (Car #{car['car_number']})"
                },
            },
            "quantity": vote_qty,
        }],
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
    show = get_active_show()
    if not session.get("admin_authed"):
        return render_template("admin.html", show=show, authed=False)
    return render_template("admin.html", show=show, authed=True)


@app.post("/admin/login")
def admin_login():
    pw = request.form.get("password", "")
    show = get_active_show()
    if pw == ADMIN_PASSWORD:
        session["admin_authed"] = True
        return redirect(url_for("admin_page"))
    return render_template("admin.html", show=show, authed=False, login_error="Incorrect password.")


@app.post("/admin/logout")
def admin_logout():
    session.pop("admin_authed", None)
    return redirect(url_for("admin_page"))


@app.post("/admin/toggle-voting")
def admin_toggle_voting():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))
    show = get_active_show()
    if show:
        toggle_show_voting(int(show["id"]))
    return redirect(url_for("admin_page"))


@app.post("/admin/open-voting")
def admin_open_voting():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))
    show = get_active_show()
    if show:
        set_show_voting_open(int(show["id"]), True)
    return redirect(url_for("admin_page"))


@app.post("/admin/close-voting")
def admin_close_voting():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))
    show = get_active_show()
    if show:
        set_show_voting_open(int(show["id"]), False)
    return redirect(url_for("admin_page"))


@app.post("/admin/reset-votes")
def admin_reset_votes():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))
    show = get_active_show()
    if show:
        reset_votes_for_show(int(show["id"]))
    return redirect(url_for("admin_page"))


@app.get("/admin/leaderboard")
def admin_leaderboard():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))

    show = get_active_show()
    if not show:
        return "No active show.", 500

    by_cat = leaderboard_by_category(int(show["id"]))
    overall = leaderboard_overall(int(show["id"]))

    return render_template("leaderboard.html", show=show, by_category=by_cat, overall=overall)


@app.get("/admin/export-votes.csv")
def admin_export_votes():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))

    show = get_active_show()
    if not show:
        return "No active show.", 500

    rows = export_votes_for_show(int(show["id"]))

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([
        "created_at", "category", "vote_qty", "amount_cents", "stripe_session_id",
        "car_number", "year", "make", "model",
        "owner_name", "owner_phone", "owner_email", "opt_in_future"
    ])
    for r in rows:
        w.writerow([
            r["created_at"], r["category"], r["vote_qty"], r["amount_cents"], r["stripe_session_id"],
            r["car_number"], r["year"], r["make"], r["model"],
            r["owner_name"], r["owner_phone"], r["owner_email"], r["opt_in_future"]
        ])

    mem = io.BytesIO(buf.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="votes_export.csv")


# ----------------------------
# RUN (RAILWAY-READY)
# ----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
