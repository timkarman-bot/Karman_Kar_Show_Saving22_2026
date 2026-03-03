# Troubleshooting Guide (Railway + Flask)

---

## 502 Bad Gateway

App crashed on startup.

Check Railway logs for:
- SyntaxError
- IndentationError
- ModuleNotFoundError
- TemplateNotFound

---

## ModuleNotFoundError: reportlab

Ensure:
- reportlab==4.0.0,<5 is in requirements.txt
- Railway is using requirements.txt
- pyproject.toml is not overriding dependencies

---

## Templates show raw {% %}

You opened the HTML file directly.

Use the Railway route URL instead.

---

## BuildError: Could not build url for endpoint

Template is calling incorrect route name.

Confirm url_for('route_name') matches function name in app.py.

---

## Stripe Checkout Not Opening

Check Railway variables:
- STRIPE_SECRET_KEY
- BASE_URL (no trailing slash)

---

## Votes Not Recording

Confirm:
- /success route firing
- stripe_session_id unique
- record_paid_votes() executing

---

## Voting Says Closed

Admin → Open Voting

Or confirm VOTING_END environment variable.

---

## Check-in Saves but Looks Blank

Confirm:
- update_person()
- update_show_car_details()
- Browser refresh

---

## IndentationError

Python requires:
- 4 spaces
- No tabs

---

## Print Cards PDF Not Generating

Check:
- utils/print_cards.py exists
- reportlab installed
- Lazy import inside route

---

# Live Show Quick Check

Before event:

- Voting open
- Stripe test payment works
- Print sheets load
- Placeholder cars created
- Leaderboard loads
- Snapshot export works
- Sponsor logos rendering
