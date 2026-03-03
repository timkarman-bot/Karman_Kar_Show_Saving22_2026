# Karman Kar Shows & Events
# Admin Control Sheet

---

# 🔑 ADMIN LOGIN

Route:
/admin

Use ADMIN_PASSWORD from Railway variables.

---

# 🚦 VOTING CONTROLS

Open Voting:
/admin → Open Voting

Close Voting:
/admin → Close Voting

Always confirm status before event starts.

---

# 🖨 PRINT WINDHSHEILD CARDS

Route:
/admin/print-cards.pdf

Options:
- Print all cars
- Print selected IDs
- Front only (default)
- Back pages (optional mirrored duplex)

---

# 📊 LEADERBOARD

Route:
/admin/leaderboard

Shows:
- Votes by category
- Overall totals

Visible to admin only.

---

# 📤 EXPORT DATA

Votes CSV:
/admin/export-votes.csv

Snapshot ZIP:
/admin/export-snapshot.zip

Snapshot includes:
- show.csv
- cars.csv
- people.csv
- votes.csv

Use snapshot for disaster recovery.

---

# 🔁 RESET VOTES

Admin → Reset Votes

IMPORTANT:
System automatically exports snapshot first.

---

# 🏷 SPONSORS

Route:
/admin/sponsors

Add:
- Sponsor name
- logo_path (static/img/sponsors/filename.png)
- Website
- Placement (title or standard)

Title sponsor displays prominently on print sheets.

---

# 🚗 PLACEHOLDER CARS

Route:
/admin/placeholders

Used before event to pre-generate car numbers and tokens.

Example:
Start: 1
Count: 300

---

# ⚠️ LIVE EVENT CHECKLIST

Before gates open:

- [ ] Voting open
- [ ] Stripe test payment works
- [ ] Leaderboard loads
- [ ] Print sheets tested
- [ ] Placeholder cars created
- [ ] Snapshot export tested
- [ ] Sponsor logos rendering
- [ ] Internet connection stable