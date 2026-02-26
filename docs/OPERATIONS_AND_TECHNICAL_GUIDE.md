# Karman Kar Shows & Events

# Operations & Technical Guide

---

# 1. System Overview

Karman Kar Shows & Events is a multi-show Flask application that supports:

* Pre-printed QR windshield cards
* Staff check-in
* $1 per vote Stripe voting
* Admin-only leaderboards
* CSV exports
* Sponsor management
* Attendee opt-in capture
* Paper waiver tracking
* Snapshot ZIP exports
* Multi-event support via `shows` table

The system is designed to:

* Prevent unpaid votes
* Protect personal data
* Maintain clean audit trails
* Support high-volume live events
* Persist data safely using Railway volume storage

---

# 2. System Architecture

### Backend

* Python 3.11
* Flask 2.3.x
* SQLite (WAL mode)
* Stripe Checkout Sessions
* Gunicorn (Railway deployment)

### Database

SQLite file stored at:

```
/data/app.db   (Railway volume)
```

If no volume exists:

```
app.db (local fallback)
```

### Data Protection Strategy

* No vote recorded until Stripe confirms payment
* Stripe session ID must be unique
* Admin routes separated
* Opt-in stored explicitly
* Waiver tracking timestamped

---

# 3. Core URL Map

| Function              | Route                                        |
| --------------------- | -------------------------------------------- |
| Home                  | `/`                                          |
| Show page             | `/show/<slug>`                               |
| Register car          | `/register`                                  |
| Windshield print page | `/r/<show_slug>/<car_token>`                 |
| Staff check-in        | `/checkin/<show_slug>/<car_token>`           |
| Vote page             | `/v/<show_slug>/<car_token>/<category_slug>` |
| Admin panel           | `/admin`                                     |
| Leaderboard           | `/admin/leaderboard`                         |
| Export CSV            | `/admin/export-votes.csv`                    |
| Export Snapshot ZIP   | `/admin/export-snapshot.zip`                 |
| Placeholder generator | `/admin/placeholders`                        |

---

# 4. Pre-Show Workflow

### 1️⃣ Deploy

* Confirm Railway variables
* Confirm DB volume mounted
* Confirm `/admin` loads

### 2️⃣ Create Placeholder Cars

Admin → Placeholder Cars

Example:

* Start: 1
* Count: 300

This:

* Creates placeholder people rows
* Creates car tokens
* Locks in car numbers

### 3️⃣ Print Windshield Sheets

Visit:

```
/r/<show_slug>/<car_token>
```

Print:

* Car number
* Vote QR
* Staff check-in QR

Bring to show.

---

# 5. Day-Of Show Workflow

## Staff Flow

1. Assign pre-printed card
2. Scan STAFF QR
3. Fill in:

   * Owner name
   * Phone
   * Email
   * Year / Make / Model
   * Marketing opt-in
4. Save

Database updates:

* `people`
* `show_cars`
* Waiver fields (if marked)

---

## Voter Flow

1. Scan category QR
2. Select vote quantity
3. Stripe checkout opens
4. On success → `/success`
5. Vote recorded only after Stripe success

Votes table stores:

* show_id
* show_car_id
* category
* vote_qty
* amount_cents
* stripe_session_id (unique)

No payment → no vote.

---

# 6. End-Of-Show Workflow

1. Admin → Close Voting
2. Admin → View Leaderboard
3. Admin → Export Votes CSV
4. Optional: Export Snapshot ZIP

Snapshot ZIP contains:

* show.csv
* cars.csv
* people.csv
* votes.csv

This is your disaster recovery archive.

---

# 7. Opt-In Compliance System

We collect marketing preferences in two places:

## Car Owner (people table)

```
opt_in_future INTEGER
```

Meaning:

* 1 = agreed to receive event updates
* 0 = no marketing contact

## Attendees

```
sponsor_opt_in INTEGER
updates_opt_in INTEGER
consent_text TEXT
consent_version TEXT
```

Policy:

* We do not sell data.
* Sponsors may contact only if sponsor_opt_in = 1.
* Consent text stored for audit trail.

---

# 8. Waiver Tracking

Paper-first approach.

Fields in `show_cars`:

```
waiver_received INTEGER
waiver_received_at TEXT
waiver_received_by TEXT
```

Marked via:

```
waiver_mark_received()
```

This allows:

* Staff accountability
* Legal record
* Timestamped verification

---

# 9. Database Structure Overview

## shows

Stores each event.

Key fields:

* slug (unique identifier)
* is_active (only one active at a time)
* voting_open

---

## people

Car owners.

---

## show_cars

Cars tied to a specific show.
Includes waiver tracking.

---

## votes

Payment-verified votes.

---

## sponsors

Master sponsor list.

---

## show_sponsors

Sponsors attached to a show.
Supports:

* Title sponsor
* Sort order

---

## attendees

General attendees/donors.

---

## donations

Stripe donation tracking.

---

## field_metrics

Tracks:

* Phone provided?
* Email provided?

Used to measure data completeness.

---

# 10. Stripe Payment Flow

1. User selects vote qty
2. Backend creates Stripe checkout session
3. Redirect to Stripe
4. Stripe redirects to:

```
/success?session_id=...
```

5. Backend verifies session
6. record_paid_votes() called
7. Vote saved

Duplicate session IDs are ignored safely.

---

# 11. Railway Deployment Reference

## Required Variables

* ADMIN_PASSWORD
* FLASK_SECRET
* STRIPE_SECRET_KEY
* BASE_URL

## Optional

* STRIPE_WEBHOOK_SECRET
* DB_PATH

## Start Command

```
gunicorn app:app --workers 1 --threads 4 --bind 0.0.0.0:$PORT
```

---

# 12. Troubleshooting Matrix

## 502 Bad Gateway

App crashed.
Check Railway logs.

---

## NameError: conn not defined

Database init corrupted.
Fix indentation.
Ensure:

```
conn = _conn()
cur = conn.cursor()
```

---

## Voting says closed

Admin → Toggle voting.

---

## Stripe doesn’t open

Check:

* STRIPE_SECRET_KEY
* BASE_URL
* No trailing slash

---

## Votes not recording

Confirm:

* Stripe success route firing
* Unique stripe_session_id
* record_paid_votes() being called

---

## Check-in saves but looks blank

Confirm:

* update_person()
* update_show_car_details()
* Refresh browser

---

## Waiver not marking

Confirm:

* waiver_mark_received() exists
* Route calling it
* DB columns exist

---

## Templates show raw `{% %}`

You opened file directly.
Use Flask route URL.

---

## IndentationError

Use 4 spaces.
No tabs.

---

# 13. Disaster Recovery Procedure

If corruption or crash:

1. Restore snapshot ZIP
2. Replace `/data/app.db`
3. Restart Railway

---

# 14. Recommended Admin Testing Checklist

Before every live show:

* [ ] Voting open
* [ ] Placeholder cars created
* [ ] Print sheets working
* [ ] Stripe test payment works
* [ ] Leaderboard loads
* [ ] CSV export works
* [ ] Waiver marking works
* [ ] Sponsor display correct
* [ ] Opt-in saving correctly
* [ ] Snapshot export working

---

# 15. System Design Principles

* Payment before vote
* No duplicate votes
* No silent data selling
* Paper waiver compliance
* Staff accountability
* Multi-event scalable
* Exportable for audit
* Railway-safe persistence

---

If you'd like, next I can:

* Convert this into a printable PDF manual
* Create a Staff Quick Reference Sheet (1 page)
* Create an Admin Quick Control Sheet
* Create a Data & Legal Compliance Addendum
* Or create a technical architecture diagram

Tell me which direction you want next.
