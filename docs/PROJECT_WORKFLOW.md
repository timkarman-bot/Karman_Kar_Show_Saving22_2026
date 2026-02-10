# Karman Kar Shows & Events — Workflow & Operations

This system supports:
- Pre-printed windshield QR cards
- Staff check-in (capture owner + car details)
- $1-per-vote Stripe voting (no vote recorded unless payment succeeds)
- Admin-only leaderboards + CSV export
- Multi-show support via `shows` table

## Key URLs
- Home: `/`
- Current show: `/show/<slug>`
- Car registration (optional): `/register`
- Windshield sheet (print): `/r/<show_slug>/<car_token>`
- Staff check-in: `/checkin/<show_slug>/<car_token>`
- Vote (category locked): `/v/<show_slug>/<car_token>/<category_slug>`
- Admin: `/admin`
- Admin leaderboard: `/admin/leaderboard`
- Admin CSV export: `/admin/export-votes.csv`
- Admin placeholder cars: `/admin/placeholders`

## Before the show (recommended workflow)
1. Deploy to Railway with variables set (see README).
2. Admin → Placeholder cars (pre-print) → create a range (e.g., 1–300).
3. Print windshield sheets by opening `/r/<show_slug>/<car_token>` for each placeholder car.
4. Bring printed sheets/cards to the show.

## Day of show (staff workflow)
1. Assign a pre-printed card (car number) to the car.
2. Scan the STAFF ONLY CHECK-IN QR on the card.
3. Fill in owner + car details and save.
4. Voters scan category QR codes, select quantity, pay by Stripe.

## End of show (admin workflow)
1. Close voting in Admin.
2. View leaderboard in Admin.
3. Export votes CSV for records and winner contact info.
