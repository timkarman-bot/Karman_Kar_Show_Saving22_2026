from functools import wraps

from flask import Blueprint, render_template, request, redirect, url_for, flash, session

from sponsorship_system import (
    init_sponsorship_tables,
    list_sponsorship_packages,
    save_sponsorship_package,
)
from database import (
    get_show_by_slug,
    get_active_show,
    get_show_sponsors,
    upsert_sponsor,
    attach_sponsor_to_show,
    remove_sponsor_from_show,
)

sponsorship_bp = Blueprint("sponsorship", __name__)


def require_admin_bp(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if not session.get("admin_authed"):
            return redirect(url_for("admin_page", next=request.path))
        return view_func(*args, **kwargs)
    return wrapped


def parse_dollars_to_cents(value: str, default_cents: int = 0) -> int:
    try:
        return max(0, int(round(float((value or "").strip()) * 100)))
    except Exception:
        return default_cents


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

    return render_template(
        "sponsorship.html",
        show=show,
        title_sponsor=title_sponsor,
        sponsors=sponsors,
        sponsorship_packages=sponsorship_packages,
    )


@sponsorship_bp.get("/admin/sponsors")
@require_admin_bp
def admin_sponsors():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    title_sponsor, sponsors = get_show_sponsors(int(show["id"])) or (None, [])
    sponsorship_packages = list_sponsorship_packages(int(show["id"]))

    return render_template(
        "admin_sponsors.html",
        show=show,
        title_sponsor=title_sponsor,
        sponsors=sponsors,
        sponsorship_packages=sponsorship_packages,
    )


@sponsorship_bp.post("/admin/sponsors/add")
@require_admin_bp
def admin_sponsors_add():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    name = request.form.get("name", "").strip()
    logo_path = request.form.get("logo_path", "").strip()
    website_url = request.form.get("website_url", "").strip()
    placement = request.form.get("placement", "standard").strip().lower()
    sort_order_raw = request.form.get("sort_order", "100").strip()

    if not name:
        flash("Sponsor name is required.", "error")
        return redirect(url_for("sponsorship.admin_sponsors"))

    try:
        sort_order = int(sort_order_raw)
    except ValueError:
        sort_order = 100

    allowed_placements = {"presenting", "title", "gold", "silver", "standard"}
    if placement not in allowed_placements:
        placement = "standard"

    sponsor_id = upsert_sponsor(
        name=name,
        logo_path=logo_path,
        website_url=website_url,
    )

    attach_sponsor_to_show(
        int(show["id"]),
        sponsor_id,
        placement=placement,
        sort_order=sort_order,
    )

    flash("Sponsor saved.", "ok")
    return redirect(url_for("sponsorship.admin_sponsors"))


@sponsorship_bp.post("/admin/sponsors/remove")
@require_admin_bp
def admin_sponsors_remove():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    sponsor_id_raw = request.form.get("sponsor_id", "").strip()
    if not sponsor_id_raw.isdigit():
        return redirect(url_for("sponsorship.admin_sponsors"))

    sponsor_id = int(sponsor_id_raw)
    remove_sponsor_from_show(int(show["id"]), sponsor_id)

    flash("Sponsor removed from show.", "ok")
    return redirect(url_for("sponsorship.admin_sponsors"))


@sponsorship_bp.post("/admin/sponsorship-packages/save")
@require_admin_bp
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
    return redirect(url_for("sponsorship.admin_sponsors"))
