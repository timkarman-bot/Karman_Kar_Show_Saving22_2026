from datetime import datetime
from functools import wraps
from pathlib import Path

from flask import Blueprint, current_app, flash, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

from database import (
    attach_sponsor_to_show,
    get_active_show,
    get_show_by_slug,
    get_show_sponsors,
    remove_sponsor_from_show,
    upsert_sponsor,
)

from sponsorship_system import (
    get_salesperson,
    init_sponsorship_tables,
    list_salespeople,
    list_sponsorship_catalog,
    list_sponsorship_sales,
    save_catalog_item,
    save_salesperson,
    save_sponsorship_sale,
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


def _logo_upload_dir() -> Path:
    base = Path(current_app.static_folder) / "img" / "sponsors" / "uploads"
    base.mkdir(parents=True, exist_ok=True)
    return base


def _save_logo(file_storage):
    if not file_storage or not getattr(file_storage, "filename", ""):
        return ""
    filename = secure_filename(file_storage.filename)
    if not filename:
        return ""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in {"png", "jpg", "jpeg", "webp", "svg"}:
        return ""
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    final_name = f"sponsor-{stamp}-{filename}"
    save_path = _logo_upload_dir() / final_name
    file_storage.save(save_path)
    return f"img/sponsors/uploads/{final_name}"


@sponsorship_bp.before_app_request
def _init_tables() -> None:
    init_sponsorship_tables()


@sponsorship_bp.get("/sponsorship/<show_slug>")
def public_sponsorship_page(show_slug: str):
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404
    title_sponsor, sponsors = get_show_sponsors(int(show["id"])) or (None, [])
    return render_template(
        "sponsorship.html",
        show=show,
        title_sponsor=title_sponsor,
        sponsors=sponsors,
        sponsorship_catalog=list_sponsorship_catalog(int(show["id"]), public_only=True),
        salespeople=list_salespeople(active_only=True),
    )


@sponsorship_bp.get("/admin/sponsors")
@require_admin_bp
def admin_sponsors():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    title_sponsor, sponsors = get_show_sponsors(int(show["id"])) or (None, [])
    return render_template(
        "admin_sponsors.html",
        show=show,
        title_sponsor=title_sponsor,
        sponsors=sponsors,
        sponsorship_catalog=list_sponsorship_catalog(int(show["id"]), public_only=False),
        sponsorship_sales=list_sponsorship_sales(int(show["id"])),
        salespeople=list_salespeople(active_only=False),
    )


@sponsorship_bp.post("/admin/salespeople/save")
@require_admin_bp
def admin_save_salesperson():
    raw = request.form.get("salesperson_id", "").strip()
    salesperson_id = int(raw) if raw.isdigit() else None
    save_salesperson(
        salesperson_id=salesperson_id,
        name=request.form.get("name", "").strip(),
        default_commission_percent=float((request.form.get("default_commission_percent", "0") or "0").strip()),
        is_active=1 if request.form.get("is_active", "1") in {"1", "on"} else 0,
        notes=request.form.get("notes", "").strip(),
    )
    flash("Salesperson saved.", "ok")
    return redirect(url_for("sponsorship.admin_sponsors"))


@sponsorship_bp.post("/admin/catalog/save")
@require_admin_bp
def admin_save_catalog():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    raw = request.form.get("catalog_id", "").strip()
    catalog_id = int(raw) if raw.isdigit() else None
    save_catalog_item(
        catalog_id=catalog_id,
        show_id=int(show["id"]),
        package_name=request.form.get("package_name", "").strip(),
        description=request.form.get("description", "").strip(),
        price_cents=parse_dollars_to_cents(request.form.get("price_dollars", "0")),
        total_available=max(0, int(request.form.get("total_available", "1") or "1")),
        sort_order=max(0, int(request.form.get("sort_order", "100") or "100")),
        is_active=1 if request.form.get("is_active", "1") in {"1", "on"} else 0,
        is_public=1 if request.form.get("is_public", "1") in {"1", "on"} else 0,
    )
    flash("Sponsorship type saved.", "ok")
    return redirect(url_for("sponsorship.admin_sponsors"))


@sponsorship_bp.post("/admin/sponsorship-sales/save")
@require_admin_bp
def admin_save_sponsorship_sale():
    show = get_active_show()
    if not show:
        return "No active show.", 500

    sale_raw = request.form.get("sale_id", "").strip()
    sale_id = int(sale_raw) if sale_raw.isdigit() else None
    cat_raw = request.form.get("catalog_id", "").strip()
    catalog_id = int(cat_raw) if cat_raw.isdigit() else None
    sp_raw = request.form.get("salesperson_id", "").strip()
    salesperson_id = int(sp_raw) if sp_raw.isdigit() else None

    salesperson = get_salesperson(salesperson_id) if salesperson_id else None
    commission_percent = float((salesperson or {}).get("default_commission_percent") or 0)
    logo_path = _save_logo(request.files.get("logo_file")) or request.form.get("existing_logo_path", "").strip()

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
        payment_method_type=request.form.get("payment_method_type", "manual").strip(),
        payment_status=request.form.get("payment_status", "pending").strip(),
        status=request.form.get("status", "open").strip(),
        notes=request.form.get("notes", "").strip(),
    )

    if request.form.get("attach_to_show") == "1" and request.form.get("payment_status", "").strip() in {"paid", "manual_paid"}:
        sponsor_name = request.form.get("sponsor_business_name", "").strip()
        if sponsor_name:
            sponsor_id = upsert_sponsor(
                name=sponsor_name,
                logo_path=logo_path,
                website_url=request.form.get("website_url", "").strip(),
            )
            attach_sponsor_to_show(
                int(show["id"]),
                sponsor_id,
                placement=request.form.get("placement", "standard").strip(),
                sort_order=100,
            )

    flash("Sponsorship sale saved.", "ok")
    return redirect(url_for("sponsorship.admin_sponsors"))


@sponsorship_bp.post("/admin/sponsors/add")
@require_admin_bp
def admin_sponsors_add():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    logo_path = _save_logo(request.files.get("logo_file")) or request.form.get("logo_path", "").strip()
    sponsor_id = upsert_sponsor(
        name=request.form.get("name", "").strip(),
        logo_path=logo_path,
        website_url=request.form.get("website_url", "").strip(),
    )
    attach_sponsor_to_show(
        int(show["id"]),
        sponsor_id,
        placement=request.form.get("placement", "standard").strip().lower(),
        sort_order=max(0, int(request.form.get("sort_order", "100") or "100")),
    )
    flash("Sponsor saved.", "ok")
    return redirect(url_for("sponsorship.admin_sponsors"))


@sponsorship_bp.post("/admin/sponsors/remove")
@require_admin_bp
def admin_sponsors_remove():
    show = get_active_show()
    if not show:
        return "No active show.", 500
    raw = request.form.get("sponsor_id", "").strip()
    if raw.isdigit():
        remove_sponsor_from_show(int(show["id"]), int(raw))
    flash("Sponsor removed from show.", "ok")
    return redirect(url_for("sponsorship.admin_sponsors"))
