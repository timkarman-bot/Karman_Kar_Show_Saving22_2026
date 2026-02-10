# Troubleshooting Guide (Railway + Flask)

## 1) Railway shows 502 Bad Gateway
**Meaning:** the app crashed on startup.
**Fix:**
- Open Railway → Deployments/Logs
- Look for: `SyntaxError`, `IndentationError`, missing import, missing template.

## 2) Templates show raw `{% ... %}` on screen
**Meaning:** you opened the HTML file directly.
**Fix:** always visit the Flask routes (e.g., `/register`) from the Railway URL.

## 3) BuildError: Could not build url for endpoint ...
**Meaning:** template calls `url_for()` with an endpoint that doesn't exist.
**Fix:**
- Confirm the route function name in `app.py` matches what the template uses.
  Example: template uses `url_for('register_page')` so app.py must have `def register_page():`

## 4) Stripe checkout doesn't open
**Check Railway variables:**
- `STRIPE_SECRET_KEY` must be set
- `BASE_URL` should be your public Railway URL (no trailing slash)

## 5) Votes not recording
- Votes only record after Stripe payment succeeds.
- Check `/success?session_id=...` logs.
- Confirm `stripe_session_id` is unique; duplicates are ignored safely.

## 6) Voting says closed
- Admin → Open voting (or Toggle voting).

## 7) Check-in page saves but fields still look blank
- Refresh. If still blank:
  - Confirm `update_person()` and `update_show_car_details()` exist in database.py
  - Confirm check-in POST route calls them

## 8) IndentationError on Railway
- Windows editors can introduce tabs.
- Fix: ensure Python uses 4 spaces (no tabs).
