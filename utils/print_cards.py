# utils/print_cards.py
# Generates landscape voting cards PDF (and optional mirrored back pages for duplex)

from __future__ import annotations

import io
from typing import Any, List, Optional, Tuple

import qrcode
from PIL import Image

CATEGORY_SLUGS: List[Tuple[str, str]] = [
    ("army", "Army"),
    ("navy", "Navy"),
    ("air-force", "Air Force"),
    ("marines", "Marines"),
    ("coast-guard", "Coast Guard"),
    ("space-force", "Space Force"),
    ("peoples-choice", "People’s Choice"),
    ("", ""),  # spare
]


def safe_open_rgba(path: str) -> Optional[Image.Image]:
    try:
        if not path:
            return None
        return Image.open(path).convert("RGBA")
    except Exception:
        return None


def make_qr(url: str, box_size: int = 7, border: int = 2) -> Image.Image:
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=box_size,
        border=border,
    )
    qr.add_data(url)
    qr.make(fit=True)
    return qr.make_image(fill_color="black", back_color="white").convert("RGB")


def draw_image_contain(c: Any, img: Image.Image, x: float, y: float, w: float, h: float) -> None:
    """
    Draw PIL image into a ReportLab canvas, contained within (x, y, w, h).
    Imports reportlab lazily so importing this module won't crash the app
    if reportlab isn't installed yet.
    """
    from reportlab.lib.utils import ImageReader

    iw, ih = img.size
    if iw <= 0 or ih <= 0:
        return

    scale = min(w / iw, h / ih)
    nw, nh = iw * scale, ih * scale
    dx, dy = x + (w - nw) / 2, y + (h - nh) / 2

    rgba = img if img.mode == "RGBA" else img.convert("RGBA")
    bg = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
    bg.alpha_composite(rgba)
    final = bg.convert("RGB")

    c.drawImage(ImageReader(final), dx, dy, width=nw, height=nh, mask="auto")


def build_landscape_cards_pdf(
    *,
    show: dict,
    cars_rows: List[dict],
    base_url: str,
    static_root: str,
    title_sponsor: Optional[dict],
    sponsors: List[dict],
    include_back: bool = False,
    mirror_back_pages: bool = True,
) -> bytes:
    """
    Landscape 8.5x11 per car.

    Front:
    - Voting QR codes
    - Title sponsor and sponsor strip
    - Basic owner/vehicle write-in area

    Back (optional duplex):
    - Owner registration / placeholder claim QR
    - For placeholder/day-of cards this points to /claim/<show_slug>/<car_token>

    NOTE:
    ReportLab imports are inside this function so the app can boot even if
    reportlab isn't installed yet.
    """
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib.units import inch
    from reportlab.pdfgen import canvas as rl_canvas

    page_w, page_h = landscape(letter)
    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=landscape(letter))

    def static_fs(rel_path: str) -> str:
        return f"{static_root.rstrip('/')}/{rel_path}"

    def sponsor_logo_img(s: Optional[dict]) -> Optional[Image.Image]:
        if not s:
            return None
        lp = (s.get("logo_path") or "").strip()
        if not lp:
            return None
        return safe_open_rgba(static_fs(lp))

    brand_logo = safe_open_rgba(static_fs("img/karmankarshows-logo.png"))

    title_logo = sponsor_logo_img(title_sponsor)
    std_logos: List[Image.Image] = []
    for s in sponsors or []:
        img = sponsor_logo_img(s)
        if img:
            std_logos.append(img)

    margin = 0.85 * inch

    for r in cars_rows:
        car_number = int(r["car_number"])
        car_token = str(r["car_token"])

        # ----------------------------
        # FRONT PAGE
        # ----------------------------
        header_logo_w = 2.2 * inch
        header_logo_h = 0.75 * inch
        header_y = page_h - margin - header_logo_h

        if brand_logo:
            draw_image_contain(c, brand_logo, margin, header_y, header_logo_w, header_logo_h)

        title_x = margin + (2.35 * inch if brand_logo else 0)

        c.setFont("Helvetica-Bold", 28)
        c.drawString(title_x, page_h - margin - 40, f"VOTE FOR CAR #{car_number}")

        c.setFont("Helvetica", 12)
        c.drawString(title_x, page_h - margin - 62, str(show.get("title") or ""))

        c.setFont("Helvetica-Bold", 14)
        c.drawString(margin, page_h - margin - 105, "VOTING RULES")

        c.setFont("Helvetica", 12)
        rules = [
            "• All votes are paid votes. Scan the code and choose quantity.",
            "• Branch Awards: Only veterans, active military, or in memory of a veteran should vote by branch.",
            "• People’s Choice: Everyone can vote.",
        ]
        y = page_h - margin - 125
        for line in rules:
            c.drawString(margin, y, line)
            y -= 16

        sponsor_y = page_h - margin - 1.95 * inch
        sponsor_h = 1.05 * inch
        sponsor_w = page_w - 2 * margin
        left_w = sponsor_w * 0.55
        right_w = sponsor_w - left_w

        if title_logo:
            draw_image_contain(c, title_logo, margin, sponsor_y, left_w, sponsor_h)

        cols, rows = 3, 2
        cell_w = right_w / cols
        cell_h = sponsor_h / rows
        for i, img in enumerate(std_logos[: cols * rows]):
            col = i % cols
            row = i // cols
            x0 = margin + left_w + col * cell_w
            y0 = sponsor_y + (rows - 1 - row) * cell_h
            draw_image_contain(c, img, x0, y0, cell_w, cell_h)

        grid_x = margin
        grid_y = margin + 1.35 * inch
        grid_w = page_w - 2 * margin
        grid_h = 3.55 * inch

        c.setFont("Helvetica-Bold", 14)
        c.drawString(grid_x, grid_y + grid_h + 10, "SCAN TO VOTE")

        cols, rows = 4, 2
        pad = 10
        cell_w = (grid_w - 2 * pad) / cols
        cell_h = (grid_h - 2 * pad) / rows

        for i in range(cols * rows):
            col = i % cols
            row = i // cols
            x0 = grid_x + pad + col * cell_w
            y0 = grid_y + pad + (rows - 1 - row) * cell_h

            slug, label = CATEGORY_SLUGS[i]
            if slug:
                vote_url = f"{base_url.rstrip('/')}/v/{show['slug']}/{car_token}/{slug}"
                qr_img = make_qr(vote_url, box_size=7, border=2)
                box = min(cell_w, cell_h) - 28
                draw_image_contain(c, qr_img, x0 + 6, y0 + 18, box, box)

            c.setFont("Helvetica", 11)
            c.drawCentredString(x0 + cell_w / 2, y0 + 4, label)

        c.setFont("Helvetica-Bold", 13)
        c.drawString(margin, margin + 0.80 * inch, "OWNER / VEHICLE INFO (Write in)")
        c.setFont("Helvetica", 12)
        c.drawString(margin, margin + 0.55 * inch, "Owner name: _____________________________")
        c.drawString(
            margin,
            margin + 0.30 * inch,
            "Year: ________   Make: ________________________   Model: ________________________",
        )

        c.showPage()

        # ----------------------------
        # BACK PAGE
        # ----------------------------
        if include_back:
            if mirror_back_pages:
                c.saveState()
                c.translate(page_w, 0)
                c.scale(-1, 1)

            c.setFont("Helvetica-Bold", 24)
            c.drawString(margin, page_h - margin - 40, f"OWNER REGISTRATION — CAR #{car_number}")

            c.setFont("Helvetica", 12)
            c.drawString(
                margin,
                page_h - margin - 62,
                "Scan to claim this car number, enter your info, sign the waiver, and complete registration.",
            )

            claim_url = f"{base_url.rstrip('/')}/claim/{show['slug']}/{car_token}"
            qr_back = make_qr(claim_url, box_size=12, border=2)

            qr_size = 4.25 * inch
            qx = (page_w - qr_size) / 2
            qy = (page_h - qr_size) / 2 - 0.10 * inch
            draw_image_contain(c, qr_back, qx, qy, qr_size, qr_size)

            info_y = qy - 18
            c.setFont("Helvetica-Bold", 11)
            c.drawCentredString(page_w / 2, info_y, "SCAN TO REGISTER THIS CAR")

            c.setFont("Helvetica", 10)
            c.drawCentredString(
                page_w / 2,
                info_y - 15,
                "Includes vehicle info, contact info, future event opt-in, sponsor opt-in, and electronic waiver signature.",
            )

            c.setFont("Helvetica-Oblique", 9)
            c.drawCentredString(page_w / 2, info_y - 31, claim_url)

            footer_y = margin + 0.35 * inch
            c.setFont("Helvetica", 10)
            c.drawString(
                margin,
                footer_y + 18,
                "By opting in, you agree Karman Kar Shows & Events may contact you about this event and future events.",
            )
            c.drawString(
                margin,
                footer_y + 4,
                "If selected, sponsor information may also be sent. Msg/data rates may apply. Opt out anytime.",
            )

            if mirror_back_pages:
                c.restoreState()

            c.showPage()

    c.save()
    buf.seek(0)
    return buf.getvalue()