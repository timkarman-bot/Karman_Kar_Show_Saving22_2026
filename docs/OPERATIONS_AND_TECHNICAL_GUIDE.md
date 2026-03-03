# Karman Kar Shows & Events
# Operations & Technical Guide

---

# 1. System Overview

Karman Kar Shows & Events is a multi-show Flask application designed for live event scalability.

It supports:

- Pre-printed QR windshield voting sheets (PDF generation)
- Staff QR check-in workflow
- $1-per-vote Stripe payments
- Admin-only leaderboard
- CSV vote exports
- Sponsor management
- Attendee opt-in capture
- Paper waiver tracking
- Snapshot ZIP disaster recovery exports
- Multi-event support via `shows` table
- Railway persistent volume storage

The system is designed to:

- Prevent unpaid votes
- Prevent duplicate Stripe votes
- Protect personal data
- Maintain audit trails
- Support high-volume live events
- Scale to multiple shows cleanly

---

# 2. Architecture

## Backend

- Python 3.11
- Flask 2.3.x
- SQLite (WAL mode enabled)
- Stripe Checkout Sessions
- Gunicorn (Railway deployment)
- ReportLab (PDF windshield card generation)

## Database Location

Railway production:

/data/app.db

Local fallback:

app.db

---

# 3. Core URL Map

| Function | Route |
|----------|-------|
| Home | `/` |
| Show page | `/show/<slug>` |
| Register car | `/register` |
| Registration complete | `/r/<show_slug>/<car_token>` |
| Staff check-in | `/checkin/<show_slug>/<car_token>` |
| Vote page | `/v/<show_slug>/<car_token>/<category_slug>` |
| Admin panel | `/admin` |
| Leaderboard | `/admin/leaderboard` |
| Export votes CSV | `/admin/export-votes.csv` |
| Export snapshot ZIP | `/admin/export-snapshot.zip` |
| Placeholder generator | `/admin/placeholders` |
| Print voting cards PDF | `/admin/print-cards.pdf` |

---

# 4. Registration Logic

## Pre-registration availability

Determined by:

prereg_allowed(show)

Logic:

1. If allow_prereg_override exists:
   - 1 → allow
   - 0 → block
2. Otherwise:
   - show_type == "full" → allow
   - show_type == "popup" → block

---

## Registration Validation Rules

Required:
- name
- car_number
- year
- make
- model

Conditional:
- If opt_in_future == True → phone is required
- Email is optional

---

# 5. Windshield Voting Sheet

Generated via:

/admin/print-cards.pdf

Uses:

utils/print_cards.py

Features:

- Landscape 8.5x11
- Category QR codes
- Sponsor strip
- Title sponsor section
- Optional mirrored back page
- Lazy ReportLab import to prevent boot crash

---

# 6. Day-of Show Workflow

## Staff Flow

1. Assign pre-printed card
2. Scan staff QR
3. Enter:
   - Owner name
   - Phone (required)
   - Email (optional)
   - Year / Make / Model
   - Marketing opt-in
4. Save

Database updates:
- people
- show_cars

---

## Voter Flow

1. Scan QR
2. Select vote quantity
3. Stripe Checkout
4. Redirect to /success
5. Backend verifies Stripe session
6. record_paid_votes() executes

Votes are stored only after payment confirmation.

---

# 7. Stripe Payment Flow

1. Backend creates checkout session
2. Stripe processes payment
3. Stripe redirects to /success?session_id=...
4. Backend verifies payment_status == "paid"
5. Vote is recorded
6. stripe_session_id must be unique

Duplicate session IDs are ignored safely.

---

# 8. Opt-In Compliance

## Car Owners

Field:
opt_in_future INTEGER

- 1 = agreed to receive event updates
- 0 = no marketing contact

Phone required only if opted in.

## Attendees

Fields:
- sponsor_opt_in
- updates_opt_in
- consent_text
- consent_version

Consent text stored for audit trail.

Policy:
- No data selling.
- Sponsors may contact only if sponsor_opt_in = 1.

---

# 9. Waiver Tracking

Fields in show_cars:

- waiver_received
- waiver_received_at
- waiver_received_by

Marked via:
waiver_mark_received()

Provides timestamped verification.

---

# 10. Snapshot & Disaster Recovery

Admin → Export Snapshot ZIP

Contains:
- show.csv
- cars.csv
- people.csv
- votes.csv

If corruption occurs:
1. Replace /data/app.db
2. Restart Railway

---

# 11. Railway Deployment

Required Variables:
- ADMIN_PASSWORD
- FLASK_SECRET
- STRIPE_SECRET_KEY
- BASE_URL

Start Command:

gunicorn app:app --workers 1 --threads 4 --bind 0.0.0.0:$PORT

---

# 12. System Design Principles

- Payment before vote
- No duplicate Stripe votes
- Minimal required data collection
- Paper waiver compliance
- Audit trail preservation
- Multi-event scalability
- Railway-safe persistence
- Exportable records
