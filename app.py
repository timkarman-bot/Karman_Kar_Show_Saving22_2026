import os
from flask import Flask, render_template, request, redirect, url_for, jsonify, session
from database import init_db, record_vote, get_totals

init_db()

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-me")

ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "change-me")

# ====== Simple show config ======
CURRENT_SHOW = {
    "slug": "karman-charity-show",
    "title": "Karman Charity Car Show",
    "date": "Saturday, April 26, 2026",
    "time": "Cars arrive at 10:00 AM",
    "location_name": "Children’s Mercy Park",
    "address": "1 Sporting Way, Kansas City, KS 66111",
    "benefiting": "Saving 22 / 22 Survivor Awareness",
    "suggested_donation": "$35 suggested donation for show cars",
    "description": "A charity car show supporting veteran suicide awareness with judged certificates by branch favorites and People’s Choice."
}

UPCOMING_EVENTS = [
    {
        "date": "May 23, 2026",
        "title": "Pop-Up Car Show (Certificates + People’s Choice)",
        "location": "Kansas City Metro (TBD)",
        "status": "Planning"
    },
    {
        "date": "June 20, 2026",
        "title": "Summer Cruise + Mini Show",
        "location": "Liberty, MO (TBD)",
        "status": "Planning"
    },
]

# Voting lock controls (in-memory; resets on restart)
VOTING_MANUALLY_LOCKED = False
app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev-secret-change-me")


# ===============================
# GLOBAL TEMPLATE VARIABLES
# ===============================

@app.context_processor
def inject_show():
    return {"show": CURRENT_SHOW}


# ===============================
# Website Pages
# ===============================

@app.get("/")
def home():
    return render_template("home.html", show=CURRENT_SHOW)

@app.get("/events")
def events():
    return render_template("events.html", show=CURRENT_SHOW, events=UPCOMING_EVENTS)

@app.get("/show/<slug>")
def show_page(slug):
    if slug != CURRENT_SHOW["slug"]:
        return render_template("show.html", show={**CURRENT_SHOW, "title": "Show Not Found"}, not_found=True)
    return render_template("show.html", show=CURRENT_SHOW, not_found=False)

@app.get("/show/<slug>/vote")
def vote_page(slug):
    if slug != CURRENT_SHOW["slug"]:
        return redirect(url_for("show_page", slug=slug))
    global VOTING_MANUALLY_LOCKED
    return render_template("vote.html", show=CURRENT_SHOW, voting_locked=VOTING_MANUALLY_LOCKED)

@app.get("/show/<slug>/leaderboard")
def leaderboard_page(slug):
    if slug != CURRENT_SHOW["slug"]:
        return redirect(url_for("show_page", slug=slug))
    totals = get_totals()
    # sort high-to-low
    totals_sorted = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
    return render_template("leaderboard.html", show=CURRENT_SHOW, totals=totals_sorted)


# ===============================
# Voting + Admin Endpoints
# ===============================

@app.post("/api/vote")
def api_vote():
    global VOTING_MANUALLY_LOCKED
    if VOTING_MANUALLY_LOCKED:
        return jsonify({"ok": False, "error": "Voting is currently locked."}), 403

    car_number = request.form.get("car_number", "").strip()
    category = request.form.get("category", "").strip()

    if not car_number or not category:
        return jsonify({"ok": False, "error": "Missing car number or category."}), 400

    # You likely already record votes by category/car.
    # Adjust record_vote signature if yours differs.
    record_vote(category=category, car_number=car_number)

    return jsonify({"ok": True})

@app.get("/admin")
def admin_page():
    if not session.get("admin_authed"):
        return render_template("admin.html", show=CURRENT_SHOW, authed=False)
    totals = get_totals()
    totals_sorted = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)
    return render_template("admin.html", show=CURRENT_SHOW, authed=True, totals=totals_sorted, voting_locked=VOTING_MANUALLY_LOCKED)

@app.post("/admin/login")
def admin_login():
    pw = request.form.get("password", "")
    if pw == ADMIN_PASSWORD:
        session["admin_authed"] = True
        return redirect(url_for("admin_page"))
    return render_template("admin.html", show=CURRENT_SHOW, authed=False, login_error="Incorrect password.")

@app.post("/admin/logout")
def admin_logout():
    session.pop("admin_authed", None)
    return redirect(url_for("admin_page"))

@app.post("/admin/toggle-voting")
def admin_toggle_voting():
    global VOTING_MANUALLY_LOCKED
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))
    VOTING_MANUALLY_LOCKED = not VOTING_MANUALLY_LOCKED
    return redirect(url_for("admin_page"))

@app.post("/admin/reset-votes")
def admin_reset_votes():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))
    reset_votes()
    return redirect(url_for("admin_page"))

@app.get("/admin/export-votes.csv")
def admin_export_votes():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))

    rows = export_votes_rows()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["category", "car_number", "created_at"])
    for r in rows:
        w.writerow([r["category"], r["car_number"], r["created_at"]])

    mem = io.BytesIO(buf.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name="votes_export.csv")

@app.get("/admin/cars")
def admin_cars():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))
    cars = list_cars(active_only=False)
    return render_template("admin_cars.html", show=CURRENT_SHOW, cars=cars)

@app.post("/admin/cars/toggle")
def admin_cars_toggle():
    if not session.get("admin_authed"):
        return redirect(url_for("admin_page"))
    car_number = request.form.get("car_number", "").strip()
    desired = request.form.get("is_active", "1") == "1"
    if car_number:
        set_car_active(car_number, desired)
    return redirect(url_for("admin_cars"))


if __name__ == "__main__":
    app.run()
