from flask import Blueprint, render_template, request, redirect, url_for, flash
from sponsorship_system import init_sponsorship_tables, list_sponsorship_packages, save_sponsorship_package
from database import get_show_by_slug, get_active_show, get_show_sponsors

sponsorship_bp = Blueprint("sponsorship", __name__)

@sponsorship_bp.before_app_request
def _init_tables() -> None:
    init_sponsorship_tables()

@sponsorship_bp.get("/sponsorship/<show_slug>")
def public_sponsorship_page(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    title_sponsor, sponsors = get_show_sponsors(int(show["id"])) or (None, [])
    sponsorship_packages = list_sponsorship_packages(int(show["id"]))
    return render_template("sponsorship.html", show=show, title_sponsor=title_sponsor, sponsors=sponsors, sponsorship_packages=sponsorship_packages)

def register_admin_routes(app, require_admin, parse_dollars_to_cents):
    @app.get("/admin/sponsors")
    @require_admin
    def admin_sponsors():
        show = get_active_show()
        if not show:
            return "No active show.", 500
        title_sponsor, sponsors = get_show_sponsors(int(show["id"])) or (None, [])
        sponsorship_packages = list_sponsorship_packages(int(show["id"]))
        return render_template("admin_sponsors.html", show=show, title_sponsor=title_sponsor, sponsors=sponsors, sponsorship_packages=sponsorship_packages)

    @app.post("/admin/sponsorship-packages/save")
    @require_admin
    def admin_save_package():
        show = get_active_show()
        if not show:
            return "No active show.", 500
        package_id_raw = request.form.get("package_id", "").strip()
        package_id = int(package_id_raw) if package_id_raw.isdigit() else None
        try:
            agreed_percent = float((request.form.get("agreed_percent", "0") or "0").strip())
        except Exception:
            agreed_percent = 0.0
        save_sponsorship_package(
            show_id=int(show["id"]),
            package_id=package_id,
            package_name=request.form.get("package_name", "").strip(),
            description=request.form.get("description", "").strip(),
            price_cents=parse_dollars_to_cents(request.form.get("price_dollars", "0")),
            quantity_total=max(0, int(request.form.get("quantity_total", "1") or "1")),
            quantity_sold=max(0, int(request.form.get("quantity_sold", "0") or "0")),
            credit_person_name=request.form.get("credit_person_name", "").strip(),
            organizer_name=request.form.get("organizer_name", "").strip(),
            agreed_percent=agreed_percent,
            internal_notes=request.form.get("internal_notes", "").strip(),
        )
        flash("Sponsorship package saved.", "ok")
        return redirect(url_for("admin_sponsors"))
