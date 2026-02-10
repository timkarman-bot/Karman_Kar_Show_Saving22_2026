# Karman Kar Shows & Events — Charity Show Voting

## Environment variables (Railway)
Required:
- ADMIN_PASSWORD
- FLASK_SECRET
- STRIPE_SECRET_KEY
- BASE_URL  (example: https://yourapp.up.railway.app)

Optional:
- STRIPE_WEBHOOK_SECRET
- DB_PATH (defaults to app.db)

## Run locally
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
set FLASK_SECRET=dev
set ADMIN_PASSWORD=devpass
set STRIPE_SECRET_KEY=sk_test_...
set BASE_URL=http://127.0.0.1:5000
python app.py
```

## Main workflows
See:
- docs/PROJECT_WORKFLOW.md
- docs/TROUBLESHOOTING.md
