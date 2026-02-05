import os
from datetime import datetime, timedelta

import stripe
from flask import Flask, render_template, request, redirect, url_for, jsonify, session

from database import init_db, record_vote, get_totals

# ===============================
# APP SETUP
# ===============================
app = Flask(__name__)

# Flask session secret (NOT Stripe)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-me")

# Admin password
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "")

# Stripe
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

# Config
MAX_CARS = int(os.getenv("MAX_CARS", "300"))

BRANCHES = [
    "Army",
    "Navy",
    "Air Force",
    "Marines",
    "Coast Guard",
    "Space Force",
    "People’s Choice",
]
PEOPLES_CHOICE = "People’s Choice"

# Voting lock controls (in-memory)
VOTING_MANUALLY_LOCKED = False
VOTING_END_DATETIME = None  # datetime(...)

# Admin session settings
ADMIN_SESSION_IDLE_MINUTES = 30

# ===============================
# INIT DB (once)
# ===============================
init_db()


# ===============================
# HELPERS
# ===============================
def voting_is_open():
    global VOTING_MANUALLY_LOCKED, VOTING_END_DATETIME
    if VOTING_MANUALLY_LOCKED:
        return False
    if VOTING_END_DATETIME and datetime.now() > VOTING_END_DATETIME:
        return False
    return True


def is_admin():
    if not session.get("admin"):
        return False

    last_seen = session.get("admin_last_seen")
    if not last_seen:
        return False

    try:
        last_seen_dt = datetime.fromisoformat(last_seen)
    except Exception:
        session.clear()
        return False

    if datetime.utcnow() - last_seen_dt > timedelta(minutes=ADMIN_SESSION_IDLE_MINUTES):
        session.clear()
        return False

    session["admin_last_seen"] = datetime.utcnow().isoformat()
    return True


def admin_required():
    if not is_admin():
        return redirect(url_for("admin_login"))
    return None


def truthy_checkbox(val: str) -> bool:
    return str(val).strip().lower() in {"1", "true", "yes", "on", "y"}


def requires_veteran_attestation(branch: str) -> bool:
    # Only People’s Choice is open to all
    return branch != PEOPLES_CHOICE


# ===============================
# PUBLIC ROUTES
# ===============================
@app.route("/")
def home():
    return render_template("home.html")


@app.route("/vote/<int:car_id>")
def vote(car_id):
    if car_id < 1 or car_id > MAX_CARS:
        return "Invalid car number", 404

    if not voting_is_open():
        return render_template("voting_closed.html")

    return render_template("vote.html", car_id=car_id, branches=BRANCHES, error=None)


@app.route("/create-checkout-session", methods=["POST"])
def create_checkout_session():
    if not voting_is_open():
        return render_template("voting_closed.html"), 403

    if not stripe.api_key:
        return "Stripe not configured. Missing STRIPE_SECRET_KEY.", 500

    # Read form fields
    try:
        car_id = int(request.form.get("car_id", "0"))
        branch = request.form.get("branch", "")
        amount = int(request.form.get("amount", "0"))      # dollars per vote
        quantity = int(request.form.get("quantity", "0"))  # number of votes
    except ValueError:
        return "Invalid form values", 400

    is_veteran = truthy_checkbox(request.form.get("is_veteran", ""))
    served_branch_confirm = truthy_checkbox(request.form.get("served_branch_confirm", ""))

    # Validate
    if not (1 <= car_id <= MAX_CARS):
        return "Invalid car id", 400
    if branch not in BRANCHES:
        return "Invalid category", 400

    # Enforce $1 only
    if amount != 1:
        return "Invalid amount", 400

    if quantity < 1 or quantity > 50:
        return "Invalid vote quantity", 400

    # Enforce veteran checkboxes for branch categories (honor system, but required acknowledgment)
    if requires_veteran_attestation(branch):
        if not is_veteran or not served_branch_confirm:
            return render_template(
                "vote.html",
                car_id=car_id,
                branches=BRANCHES,
                error="Branch voting requires veteran confirmation. Please check both boxes, or select People’s Choice.",
            ), 400

    # Stripe Checkout Session
    try:
        session_obj = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="payment",
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "product_data": {"name": f"Car {car_id} – {branch} Vote"},
                    "unit_amount": 100,  # $1 in cents
                },
                "quantity": quantity,
            }],
            metadata={
                "car_id": str(car_id),
                "branch": branch,
                "votes": str(quantity),
                "vote_amount": "1",
                "veteran_attested": "yes" if (is_veteran and served_branch_confirm) else "no",
            },
            success_url=url_for("success", _external=True),
            cancel_url=url_for("vote", car_id=car_id, _external=True),
        )
        return redirect(session_obj.url)
    except Exception as e:
        return f"Stripe error: {e}", 500


@app.route("/success")
def success():
    return render_template("success.html")


@app.route("/health")
def health():
    return jsonify(status="ok")


# ===============================
# ADMIN AUTH ROUTES
# ===============================
@app.route("/admin-login", methods=["GET", "POST"])
def admin_login():
    if not ADMIN_PASSWORD:
        return "Admin login is not configured. Set ADMIN_PASSWORD in Railway Variables.", 500

    error = None
    if request.method == "POST":
        password = request.form.get("password", "")
        if password == ADMIN_PASSWORD:
            session["admin"] = True
            session["admin_last_seen"] = datetime.utcnow().isoformat()
            return redirect(url_for("admin"))
        error = "Invalid password."

    return render_template("admin_login.html", error=error)


@app.route("/admin-logout")
def admin_logout():
    session.clear()
    return redirect(url_for("home"))


# ===============================
# ADMIN ROUTES (PROTECTED)
# ===============================
@app.route("/admin")
def admin():
    resp = admin_required()
    if resp:
        return resp

    return render_template(
        "admin.html",
        totals=get_totals(),
        max_cars=MAX_CARS,
        voting_open=voting_is_open(),
        voting_end=VOTING_END_DATETIME,
        voting_locked=VOTING_MANUALLY_LOCKED,
        idle_minutes=ADMIN_SESSION_IDLE_MINUTES,
    )


@app.route("/leaderboard")
def leaderboard():
    resp = admin_required()
    if resp:
        return resp

    return render_template("leaderboard.html", totals=get_totals())


@app.route("/admin/lock", methods=["POST"])
def lock_voting():
    resp = admin_required()
    if resp:
        return resp

    global VOTING_MANUALLY_LOCKED
    VOTING_MANUALLY_LOCKED = True
    return redirect(url_for("admin"))


@app.route("/admin/unlock", methods=["POST"])
def unlock_voting():
    resp = admin_required()
    if resp:
        return resp

    global VOTING_MANUALLY_LOCKED
    VOTING_MANUALLY_LOCKED = False
    return redirect(url_for("admin"))


@app.route("/admin/set-end-time", methods=["POST"])
def set_end_time():
    resp = admin_required()
    if resp:
        return resp

    global VOTING_END_DATETIME
    end_time_str = request.form.get("end_time", "").strip()
    if not end_time_str:
        return redirect(url_for("admin"))

    try:
        VOTING_END_DATETIME = datetime.fromisoformat(end_time_str)
    except Exception:
        return "Invalid datetime format", 400

    return redirect(url_for("admin"))


# ===============================
# STRIPE WEBHOOK (RECORD VOTES)
# ===============================
@app.route("/webhook", methods=["POST"])
def webhook():
    if not STRIPE_WEBHOOK_SECRET:
        return "Webhook not configured. Set STRIPE_WEBHOOK_SECRET.", 501

    payload = request.data
    sig_header = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except Exception:
        return "Invalid webhook signature", 400

    if event["type"] == "checkout.session.completed":
        session_obj = event["data"]["object"]
        md = session_obj.get("metadata", {})

        try:
            car_id = int(md.get("car_id", "0"))
            branch = md.get("branch", "")
            votes = int(md.get("votes", "0"))
            vote_amount = int(md.get("vote_amount", "0"))
            stripe_session_id = session_obj.get("id", "")
        except Exception:
            return "", 200

        # Enforce $1 vote_amount only
        if (1 <= car_id <= MAX_CARS) and (branch in BRANCHES) and (votes > 0) and (vote_amount == 1):
            record_vote(car_id, branch, vote_amount, votes, stripe_session_id)

    return "", 200


# ===============================
# RUN LOCAL
# ===============================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
