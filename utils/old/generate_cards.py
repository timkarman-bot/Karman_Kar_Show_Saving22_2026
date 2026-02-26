#!/usr/bin/env python3
"""
Generate 8.5x11 printable voting sheets for car show voting.

What this creates
- A single multi-page PDF: output/car_voting_sheets.pdf
- Optional per-car PNG previews (off by default): output/previews/car_001.png ...

What each page includes
- Your Karman Kar Shows & Events logo
- Big "Car #X" header
- QR code to the car’s unique voting page: {BASE_URL}/vote/<car_id>
- Clear voting instructions + rules
- A Title Sponsor area:
  - either a placeholder box (default)
  - or an optional sponsor logo (per event or per car)

Sponsor support
- Use a single event sponsor logo: --event-sponsor-logo path/to/logo.png
- OR map specific sponsor logos per car via CSV: --sponsor-csv sponsor_map.csv
  CSV format (header required):
      car_id,sponsor_logo_path,sponsor_name
      1,assets/sponsors/acme.png,ACME Plumbing
      2,assets/sponsors/other.png,Other Sponsor

Run examples
1) Basic (logo + placeholder sponsor box):
   python utils/generate_cards.py --base-url https://www.karmankarshowsandevents.com --logo static/KarmanKarShowsLogo.png

2) With one event-level title sponsor logo:
   python utils/generate_cards.py --base-url https://www.karmankarshowsandevents.com --logo static/KarmanKarShowsLogo.png --event-sponsor-logo assets/sponsors/title.png --event-sponsor-name "Title Sponsor"

3) With per-car sponsor logos via CSV:
   python utils/generate_cards.py --base-url https://www.karmankarshowsandevents.com --logo static/KarmanKarShowsLogo.png --sponsor-csv sponsor_map.csv

Dependencies
- reportlab
- qrcode
- Pillow
Install locally:
  pip install reportlab qrcode[pil] pillow
"""

import argparse
import csv
import os
from dataclasses import dataclass
from typing import Dict, Optional

import qrcode
from PIL import Image
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


BRANCH_RULES_TEXT = [
    "How voting works:",
    "• Branch Awards (Veterans Only): Veterans vote in the branch they served.",
    "  (Army, Navy, Air Force, Marines, Coast Guard, Space Force)",
    "• People’s Choice: Open to everyone.",
    "• $1 per vote • Vote as many times as you want.",
    "• A car can only win once in each category.",
]

PEOPLES_CHOICE_LABEL = "People’s Choice"


@dataclass
class SponsorInfo:
    sponsor_logo_path: Optional[str] = None
    sponsor_name: Optional[str] = None


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def load_sponsor_map(csv_path: str) -> Dict[int, SponsorInfo]:
    sponsor_map: Dict[int, SponsorInfo] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"car_id", "sponsor_logo_path", "sponsor_name"}
        if not required.issubset(set(reader.fieldnames or [])):
            raise ValueError(
                f"sponsor CSV must have headers: {', '.join(sorted(required))}"
            )
        for row in reader:
            try:
                car_id = int(str(row["car_id"]).strip())
            except Exception:
                continue
            sponsor_map[car_id] = SponsorInfo(
                sponsor_logo_path=str(row.get("sponsor_logo_path", "")).strip() or None,
                sponsor_name=str(row.get("sponsor_name", "")).strip() or None,
            )
    return sponsor_map


def make_qr_image(url: str, box_size: int = 10, border: int = 2) -> Image.Image:
    qr = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=box_size,
        border=border,
    )
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    return img.convert("RGB")


def safe_open_image(path: str) -> Optional[Image.Image]:
    if not path:
        return None
    if not os.path.exists(path):
        return None
    try:
        return Image.open(path).convert("RGBA")
    except Exception:
        return None


def draw_image_contain(
    c: canvas.Canvas,
    pil_img: Image.Image,
    x: float,
    y: float,
    w: float,
    h: float,
) -> None:
    """
    Draw PIL image into (x,y,w,h) with 'contain' behavior (no cropping).
    """
    img_w, img_h = pil_img.size
    if img_w == 0 or img_h == 0:
        return

    scale = min(w / img_w, h / img_h)
    new_w = img_w * scale
    new_h = img_h * scale

    # Center
    dx = x + (w - new_w) / 2
    dy = y + (h - new_h) / 2

    # Convert to ImageReader
    rgba = pil_img
    if rgba.mode != "RGBA":
        rgba = rgba.convert("RGBA")

    # reportlab doesn't like alpha sometimes; flatten on white
    bg = Image.new("RGBA", rgba.size, (255, 255, 255, 255))
    bg.alpha_composite(rgba)
    final = bg.convert("RGB")

    c.drawImage(ImageReader(final), dx, dy, width=new_w, height=new_h, mask="auto")


def draw_placeholder_box(
    c: canvas.Canvas,
    x: float,
    y: float,
    w: float,
    h: float,
    title: str,
    subtitle: str = "",
) -> None:
    c.setLineWidth(1)
    c.rect(x, y, w, h, stroke=1, fill=0)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(x + 10, y + h - 18, title)
    if subtitle:
        c.setFont("Helvetica", 10)
        c.drawString(x + 10, y + h - 34, subtitle)


def generate_pdf(
    output_pdf: str,
    base_url: str,
    logo_path: str,
    cars: int,
    sponsor_map: Dict[int, SponsorInfo],
    event_sponsor: SponsorInfo,
    include_png_previews: bool = False,
    preview_dir: str = "output/previews",
) -> None:
    ensure_dir(os.path.dirname(output_pdf) or ".")
    if include_png_previews:
        ensure_dir(preview_dir)

    page_w, page_h = letter  # 8.5 x 11

    # Layout constants
    margin = 0.6 * inch
    header_h = 1.25 * inch
    footer_h = 1.1 * inch

    qr_size = 3.5 * inch  # QR box
    sponsor_box_h = 1.6 * inch

    c = canvas.Canvas(output_pdf, pagesize=letter)

    # Load primary logo once
    logo_img = safe_open_image(logo_path)
    if logo_img is None:
        raise FileNotFoundError(f"Logo not found or unreadable: {logo_path}")

    for car_id in range(1, cars + 1):
        # Background
        c.setFont("Helvetica", 11)

        # HEADER: Logo + Title
        header_y = page_h - margin - header_h
        logo_box_w = 2.3 * inch
        logo_box_h = 1.0 * inch
        draw_image_contain(
            c,
            logo_img,
            margin,
            header_y + (header_h - logo_box_h) / 2,
            logo_box_w,
            logo_box_h,
        )

        c.setFont("Helvetica-Bold", 26)
        c.drawString(margin + logo_box_w + 20, page_h - margin - 40, f"VOTE FOR CAR #{car_id}")

        c.setFont("Helvetica", 12)
        c.drawString(margin + logo_box_w + 20, page_h - margin - 62, "Scan the QR code to vote ($1 per vote)")

        # QR URL
        vote_url = f"{base_url.rstrip('/')}/vote/{car_id}"
        qr_img = make_qr_image(vote_url, box_size=10, border=2)

        # Main content area
        content_top = header_y - 0.2 * inch
        content_bottom = margin + footer_h + sponsor_box_h + 0.25 * inch

        # Place QR left; instructions right
        qr_x = margin
        qr_y = content_top - qr_size
        draw_image_contain(c, qr_img, qr_x, qr_y, qr_size, qr_size)

        # QR caption + fallback URL
        c.setFont("Helvetica", 10)
        c.drawString(qr_x, qr_y - 14, "If you can’t scan, visit:")
        c.setFont("Helvetica-Bold", 10)
        c.drawString(qr_x, qr_y - 28, vote_url)

        # Instructions box on the right
        instr_x = margin + qr_size + 0.4 * inch
        instr_w = page_w - margin - instr_x
        instr_y = qr_y
        instr_h = qr_size

        c.setLineWidth(1)
        c.rect(instr_x, instr_y, instr_w, instr_h, stroke=1, fill=0)

        c.setFont("Helvetica-Bold", 14)
        c.drawString(instr_x + 12, instr_y + instr_h - 22, "Voting Instructions")

        c.setFont("Helvetica", 11)
        y_cursor = instr_y + instr_h - 42
        for line in BRANCH_RULES_TEXT:
            c.drawString(instr_x + 12, y_cursor, line)
            y_cursor -= 16

        c.setFont("Helvetica-Oblique", 10)
        c.drawString(
            instr_x + 12,
            instr_y + 16,
            "Branch voting uses honor system (veteran confirmation required on screen).",
        )

        # SPONSOR BOX area (bottom)
        sponsor_y = margin + footer_h + 0.15 * inch
        sponsor_x = margin
        sponsor_w = page_w - 2 * margin

        # Choose sponsor: per-car CSV overrides event sponsor
        s = sponsor_map.get(car_id) or event_sponsor
        sponsor_logo = safe_open_image(s.sponsor_logo_path) if s and s.sponsor_logo_path else None
        sponsor_name = (s.sponsor_name or "").strip() if s else ""

        if sponsor_logo is None:
            draw_placeholder_box(
                c,
                sponsor_x,
                sponsor_y,
                sponsor_w,
                sponsor_box_h,
                title="TITLE SPONSOR",
                subtitle="(Sponsor logo placed here)",
            )
            c.setFont("Helvetica", 10)
            c.drawString(
                sponsor_x + 10,
                sponsor_y + 12,
                "Optional: Provide sponsor logo(s) later and regenerate sheets.",
            )
        else:
            # Draw sponsor frame + logo + name
            c.setLineWidth(1)
            c.rect(sponsor_x, sponsor_y, sponsor_w, sponsor_box_h, stroke=1, fill=0)
            c.setFont("Helvetica-Bold", 12)
            c.drawString(sponsor_x + 10, sponsor_y + sponsor_box_h - 18, "TITLE SPONSOR")

            # logo area
            logo_area_x = sponsor_x + 10
            logo_area_y = sponsor_y + 10
            logo_area_w = sponsor_w * 0.55
            logo_area_h = sponsor_box_h - 34
            draw_image_contain(c, sponsor_logo, logo_area_x, logo_area_y, logo_area_w, logo_area_h)

            # sponsor name area
            c.setFont("Helvetica-Bold", 16)
            name_x = sponsor_x + (sponsor_w * 0.6)
            name_y = sponsor_y + sponsor_box_h / 2
            if sponsor_name:
                c.drawString(name_x, name_y, sponsor_name)
            else:
                c.setFont("Helvetica", 12)
                c.drawString(name_x, name_y, "(Sponsor Name)")

        # FOOTER
        footer_y = margin
        c.setFont("Helvetica", 10)
        c.drawString(margin, footer_y + 22, "Karman Kar Shows & Events")
        c.drawRightString(page_w - margin, footer_y + 22, "Thank you for supporting our causes.")
        c.setFont("Helvetica-Oblique", 9)
        c.drawString(margin, footer_y + 8, "Payments processed securely via Stripe Checkout.")

        # Optional PNG preview export
        if include_png_previews:
            # Render page to a raster image by re-drawing into a PIL canvas is non-trivial without extra deps.
            # Keep previews OFF by default; if you need PNGs, say so and I’ll provide a version using pdf2image.
            pass

        c.showPage()

    c.save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True, help="Base URL (e.g., https://www.karmankarshowsandevents.com)")
    parser.add_argument("--logo", required=True, help="Path to Karman Kar Shows logo (e.g., static/KarmanKarShowsLogo.png)")
    parser.add_argument("--cars", type=int, default=300, help="Number of cars (default: 300)")
    parser.add_argument("--out", default="output/car_voting_sheets.pdf", help="Output PDF path")
    parser.add_argument("--event-sponsor-logo", default="", help="Optional event sponsor logo path")
    parser.add_argument("--event-sponsor-name", default="", help="Optional event sponsor name")
    parser.add_argument("--sponsor-csv", default="", help="Optional CSV for per-car sponsor logos/names")

    args = parser.parse_args()

    sponsor_map: Dict[int, SponsorInfo] = {}
    if args.sponsor_csv:
        sponsor_map = load_sponsor_map(args.sponsor_csv)

    event_sponsor = SponsorInfo(
        sponsor_logo_path=args.event_sponsor_logo or None,
        sponsor_name=args.event_sponsor_name or None,
    )

    generate_pdf(
        output_pdf=args.out,
        base_url=args.base_url,
        logo_path=args.logo,
        cars=args.cars,
        sponsor_map=sponsor_map,
        event_sponsor=event_sponsor,
    )

    print(f"✅ Generated: {args.out}")
    if args.sponsor_csv:
        print(f"ℹ️ Used per-car sponsor mapping from: {args.sponsor_csv}")
    elif args.event_sponsor_logo:
        print(f"ℹ️ Used event sponsor logo: {args.event_sponsor_logo}")


if __name__ == "__main__":
    main()
