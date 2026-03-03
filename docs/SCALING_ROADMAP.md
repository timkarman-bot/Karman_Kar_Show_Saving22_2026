# Karman Kar Shows & Events
# Scaling Roadmap

---

# 🎯 Current Version

Supports:
- Military + People’s Choice voting
- $1 per vote Stripe payments
- Single active show
- PDF voting sheets
- Sponsor management
- Snapshot exports

---

# 🚀 Next Phase: Configurable Voting Modes

Add show-level setting:

voting_mode:

Options:
- donation_per_vote (current)
- one_person_one_vote
- judge_panel
- competitive_live_leaderboard

Allows different show formats without rewriting core app.

---

# 📂 Custom Category System

Add table:
show_categories

Allows:
- Custom awards
- Car class categories
- Imported CSV-based categories

Removes hardcoded CATEGORY_SLUGS dependency.

---

# 📊 CSV Car Class Import

Admin uploads CSV:
Example:
Mustang 1960-1970
Cutlass 1969-1975

System auto-generates class awards.

Future:
Auto-assign class by make/model/year.

---

# 🌎 Multi-Show Improvements

Add:
- Public leaderboard toggle
- Event archive page
- Past winners history page
- Sponsor performance analytics

---

# 🔐 Data Enhancements

Future:
- Stripe webhook verification
- Email receipt confirmations
- SMS voting receipts
- Admin audit logs

---

# 🏗 Structural Refactor (Optional)

Add folders:
- services/
- models/
- routes/
- config/

Separate business logic from app.py.

---

# 📈 Long-Term Vision

Transform from:
"Single event voting app"

Into:
"Fully configurable fundraising event platform"