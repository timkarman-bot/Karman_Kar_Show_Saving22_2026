import os
from flask import Flask, render_template, request, redirect, url_for, jsonify
import stripe
from datetime import datetime

app = Flask(__name__)

# ===============================
# STRIPE CONFIG
# ===============================
stripe.api_key = os.getenv("STRIPE_SECRET_KEY")

# ===============================
# GLOBAL CONFIG
# ===============================
MAX_CARS = 300

BRANCHES = [
    "Army",
    "Navy",
    "Air Force",
    "Marines",
    "Coast Guard",
    "Space Force",
    "People’s Choice"
]

# Voting control (can later be stored in DB)
VOTING_END_DATETIME = None  # example: datetime(2026, 4, 26, 15, 0)
VOTING_MANUALLY_LOCKED = False


# ===============================
# HELPERS
# ===============================
def voting_is_open():
    if VOTING_MANUALLY_LOCKED:
        return False
    if VOTING_END_DATETIME and datetime.now() > VOTING_END_DATETIME:
        return False
    return True


# ===============================
# ROUTES
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

    return render_template(
        "vote.html",
        car_id=car_id,
        branches=BRANCHES
    )


@app.route("/create-checkout-session", methods=["POST"])
def create_checkout_session():
    if not voting_is_open():
        return "Voting is closed", 403

    car_id = request.form.get("car_id")
    branch = request.form.get("branch")
    amount = int(request.form.get("amount"))  # dollars
    quantity = int(request.form.get("quantity"))

    unit_amount_cents = amount * 100

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="payment",
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "product_data": {
                        "name": f"Car {car_id} – {branch}"
                    },
                    "unit_amount": unit_amount_cents,
                },
                "quantity": quantity,
            }],
            metadata={
                "car_id": car_id,
                "branch": branch,
                "votes": quantity
            },
            success_url=url_for("success", _external=True),
            cancel_url=url_for("vote", car_id=car_id, _external=True),
        )

        return redirect(session.url)

    except Exception as e:
        return str(e), 500


@app.route("/success")
def success():
    return render_template("success.html")


# ===============================
# ADMIN (FOUNDATION)
# ===============================

@app.route("/admin")
def admin():
    return render_template(
        "admin.html",
        voting_locked=not voting_is_open(),
        voting_end=VOTING_END_DATETIME
    )


@app.route("/admin/lock", methods=["POST"])
def lock_voting():
    global VOTING_MANUALLY_LOCKED
    VOTING_MANUALLY_LOCKED = True
    return redirect(url_for("admin"))


@app.route("/admin/unlock", methods=["POST"])
def unlock_voting():
    global VOTING_MANUALLY_LOCKED
    VOTING_MANUALLY_LOCKED = False
    return redirect(url_for("admin"))


@app.route("/admin/set-end-time", methods=["POST"])
def set_end_time():
    global VOTING_END_DATETIME
    end_time_str = request.form.get("end_time")

    if end_time_str:
        VOTING_END_DATETIME = datetime.fromisoformat(end_time_str)

    return redirect(url_for("admin"))


# ===============================
# HEALTH CHECK (Railway-friendly)
# ===============================
@app.route("/health")
def health():
    return jsonify(status="ok")


# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
