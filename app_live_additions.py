import smtplib
from email.message import EmailMessage

from sponsorship_system import (
    get_catalog_item,
    get_salesperson,
    get_sponsorship_sale,
    get_sponsorship_sale_by_checkout_session,
    mark_sponsorship_sale_paid_by_checkout_session,
    save_sponsorship_sale,
)

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
    stamp = datetime.now(ZoneInfo("America/Chicago")).strftime("%Y%m%d-%H%M%S")
    final_name = f"sponsor-{stamp}-{filename}"
    out_path = upload_dir / final_name
    file_storage.save(out_path)
    return f"img/sponsors/uploads/{final_name}"

def _send_system_email(*, subject: str, body: str, reply_to: str = "") -> bool:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    smtp_port = int(os.getenv("SMTP_PORT", "587").strip() or "587")
    smtp_username = os.getenv("SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    smtp_use_tls = os.getenv("SMTP_USE_TLS", "1").strip() != "0"
    smtp_from = os.getenv("SMTP_FROM_EMAIL", "info@karmankarshowsandevents.com").strip()
    target = "info@karmankarshowsandevents.com"

    if not smtp_host or not smtp_username or not smtp_password:
        app.logger.warning("SMTP not configured; email not sent. Subject=%s", subject)
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = target
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            if smtp_use_tls:
                server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
        return True
    except Exception:
        app.logger.exception("Failed to send system email.")
        return False

@app.get("/contact")
def contact_page():
    return render_template("contact.html")

@app.post("/contact")
@rate_limit("contact_submit", 10, 300)
def contact_submit():
    name = request.form.get("name", "").strip()
    email = request.form.get("email", "").strip()
    subject = request.form.get("subject", "").strip()
    body = request.form.get("body", "").strip()

    if not (name and email and subject and body):
        flash("Please complete all contact form fields.", "error")
        return redirect(url_for("contact_page"))

    sent = _send_system_email(
        subject=f"Customer Service: {subject}",
        body=f"From: {name}\nEmail: {email}\n\n{body}",
        reply_to=email,
    )

    if sent:
        flash("Your message has been sent.", "ok")
    else:
        flash("Your message was saved, but email delivery is not configured yet.", "error")
    return redirect(url_for("contact_page"))

@app.post("/sponsorship/submit")
@rate_limit("sponsorship_submit", 20, 300)
def sponsorship_public_submit():
    show_slug = request.form.get("show_slug", "").strip()
    show = get_show_by_slug(show_slug)
    if not show:
        return "Show not found.", 404

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
    if payment_method_choice not in {"card", "check", "invoice"}:
        payment_method_choice = "card"

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
        notes=request.form.get("notes", "").strip(),
    )

    if payment_method_choice == "card":
        acct = _connected_account_id(show)
        if not acct:
            flash("This show does not have a charity payment account connected yet.", "error")
            return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))

        _require_platform_stripe()
        success_url = _abs_url(url_for("sponsorship_checkout_success", sale_id=sale_id)) + "?session_id={CHECKOUT_SESSION_ID}"
        cancel_url = _abs_url(url_for("sponsorship.public_sponsorship_page", show_slug=show_slug))

        session_obj = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "unit_amount": int(catalog["price_cents"] or 0),
                    "product_data": {"name": f"Sponsorship – {catalog['package_name']} ({show['title']})"},
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
            stripe_account=acct,
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
            notes=request.form.get("notes", "").strip(),
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
        f"Amount: ${float(int(catalog['price_cents'] or 0) / 100):.2f}\n"
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

    md_show = get_active_show()
    acct = _connected_account_id(md_show)
    if not acct:
        return render_template("payment_not_complete.html")

    _require_platform_stripe()
    try:
        sess = stripe.checkout.Session.retrieve(session_id, stripe_account=acct)
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

    acct = _connected_account_id(show)
    if not acct:
        return render_template("payment_not_complete.html")

    receipt_url = ""
    try:
        if getattr(sess, "payment_intent", None):
            pi = stripe.PaymentIntent.retrieve(sess.payment_intent, stripe_account=acct)
            if getattr(pi, "latest_charge", None):
                ch = stripe.Charge.retrieve(pi.latest_charge, stripe_account=acct)
                receipt_url = getattr(ch, "receipt_url", "") or ""
    except Exception:
        receipt_url = ""

    mark_sponsorship_sale_paid_by_checkout_session(sess.id, receipt_url=receipt_url)
    sale = get_sponsorship_sale_by_checkout_session(sess.id) or sale
    sponsor_name = (sale.get("sponsor_business_name") or "").strip()
    if sponsor_name:
        sponsor_id = upsert_sponsor(name=sponsor_name, logo_path=(sale.get("logo_path") or "").strip(), website_url=(sale.get("website_url") or "").strip())
        attach_sponsor_to_show(int(show["id"]), sponsor_id, placement=(sale.get("placement") or "standard").strip(), sort_order=100)

    flash("Payment received. Stripe will send your receipt automatically.", "ok")
    return redirect(url_for("sponsorship.public_sponsorship_page", show_slug=show["slug"]))
