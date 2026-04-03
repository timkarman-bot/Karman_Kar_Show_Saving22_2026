LIVE SPONSOR SYSTEM BUNDLE

Included replacement files:
- sponsorship_system.py
- sponsorship_blueprint.py
- templates/sponsorship.html
- templates/admin_sponsors.html
- templates/base.html
- templates/contact.html
- templates/privacy.html

Included app add-on file:
- app_live_additions.py

WHAT THIS DOES
- adds mailing address fields to sponsorship records
- makes public sponsor form safe for live use
- removes public ability to claim payment status
- supports:
  - pay now by card
  - pay by check / salesperson collected
  - request invoice
- Stripe sends receipts automatically for card payments
- sends notification emails for invoice/check requests to info@karmankarshowsandevents.com
- adds Contact page + contact email form
- updates company name to Karman Kar Shows & Events, LLC

SMTP ENV VARS NEEDED
- SMTP_HOST
- SMTP_PORT
- SMTP_USERNAME
- SMTP_PASSWORD
- SMTP_USE_TLS
- SMTP_FROM_EMAIL
