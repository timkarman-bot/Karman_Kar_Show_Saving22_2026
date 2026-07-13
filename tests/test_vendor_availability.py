import sqlite3
import tempfile
import unittest
from pathlib import Path

import database
from vendor_system import (
    create_vendor_hold,
    init_vendor_tables,
    save_vendor_packages,
    save_vendor_settings,
    vendor_registration_open,
)


def _vendor_db(tmp_path: Path) -> str:
    db_path = str(tmp_path / "vendor.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE shows (id INTEGER PRIMARY KEY, title TEXT)")
    conn.execute("INSERT INTO shows (id, title) VALUES (1, 'Vendor Test Show')")
    conn.commit()
    conn.close()
    init_vendor_tables(db_path)
    return db_path


def _configure_vendor_show(db_path: str, **overrides):
    settings = {
        "vendors_enabled": True,
        "vendor_public_status": "open",
        "vendor_headline": "Vendor Registration",
        "vendor_instructions": "Setup starts at 8 AM.",
        "vendor_agreement": "Vendor rules apply. Vendor payments are non-refundable once confirmed.",
        "vendor_policy_version": "vendor-policy-test",
        "vendor_open_at": "",
        "vendor_deadline": "",
        "vendor_overall_max": 3,
        "vendor_reserved_sponsor_spaces": 0,
        "food_vendors_enabled": True,
    }
    settings.update(overrides)
    save_vendor_settings(db_path, 1, settings)
    save_vendor_packages(
        db_path,
        1,
        [
            {
                "name": "10x10 Booth",
                "price_cents": 7500,
                "capacity": 2,
                "reserved_sponsor_spaces": 0,
                "is_active": True,
                "is_closed": False,
                "is_food": False,
                "sort_order": 10,
            }
        ],
    )


class VendorAvailabilityTests(unittest.TestCase):
    def test_enabled_and_configured_vendor_registration_is_available(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = _vendor_db(Path(tmp))
            _configure_vendor_show(db_path)

            availability = vendor_registration_open(db_path, 1)

            self.assertTrue(availability["open"])
            self.assertEqual(availability["status_label"], "Open")
            self.assertEqual(availability["setup_warnings"], [])

    def test_disabled_or_incomplete_vendor_registration_is_unavailable(self):
        cases = [
            ({"vendors_enabled": False}, None),
            ({"vendor_public_status": "closed"}, None),
            ({"vendor_overall_max": None}, "Set an overall vendor maximum greater than zero."),
            ({"vendor_agreement": ""}, "Add the vendor rules and no-refund policy."),
            ({"vendor_agreement": "Vendor rules apply."}, "Add the vendor rules and no-refund policy."),
        ]
        for settings_override, expected_warning in cases:
            with self.subTest(settings_override=settings_override):
                with tempfile.TemporaryDirectory() as tmp:
                    db_path = _vendor_db(Path(tmp))
                    _configure_vendor_show(db_path, **settings_override)

                    availability = vendor_registration_open(db_path, 1)

                    self.assertFalse(availability["open"])
                    if expected_warning:
                        self.assertIn(expected_warning, availability["setup_warnings"])

    def test_no_active_categories_blocks_vendor_registration(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = _vendor_db(Path(tmp))
            _configure_vendor_show(db_path)
            save_vendor_packages(
                db_path,
                1,
                [
                    {
                        "id": 1,
                        "name": "10x10 Booth",
                        "price_cents": 7500,
                        "capacity": 2,
                        "is_active": False,
                        "is_closed": True,
                    }
                ],
            )

            availability = vendor_registration_open(db_path, 1)

            self.assertFalse(availability["open"])
            self.assertIn("Add at least one active vendor category.", availability["setup_warnings"])

    def test_missing_or_zero_category_capacity_blocks_vendor_registration(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = _vendor_db(Path(tmp))
            _configure_vendor_show(db_path)
            save_vendor_packages(
                db_path,
                1,
                [{"id": 1, "name": "10x10 Booth", "price_cents": 7500, "capacity": 0, "is_active": True}],
            )

            availability = vendor_registration_open(db_path, 1)

            self.assertFalse(availability["open"])
            self.assertIn(
                "Set a maximum greater than zero for at least one active vendor category.",
                availability["setup_warnings"],
            )

    def test_future_opening_date_blocks_get_status_and_direct_hold_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = _vendor_db(Path(tmp))
            _configure_vendor_show(db_path, vendor_open_at="2099-08-08T08:00")

            availability = vendor_registration_open(db_path, 1)

            self.assertFalse(availability["open"])
            self.assertEqual(
                availability["public_reason"],
                "Vendor registration opens August 8, 2099 at 8:00 AM.",
            )
            with self.assertRaises(ValueError):
                create_vendor_hold(db_path, 1, 1, {"business_name": "Test"}, "policy", "v1")

    def test_closing_deadline_and_manual_closure_block_registration(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = _vendor_db(Path(tmp))
            _configure_vendor_show(db_path, vendor_deadline="2000-01-01T12:00")
            self.assertEqual(vendor_registration_open(db_path, 1)["status_label"], "Closed by deadline")

            _configure_vendor_show(db_path, vendor_public_status="closed")
            self.assertEqual(vendor_registration_open(db_path, 1)["status_label"], "Closed manually")

    def test_capacity_full_blocks_registration(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = _vendor_db(Path(tmp))
            _configure_vendor_show(db_path, vendor_overall_max=1)
            create_vendor_hold(
                db_path,
                1,
                1,
                {
                    "business_name": "Full Test",
                    "contact_name": "Test User",
                    "email": "test@example.com",
                    "rules_accepted": True,
                    "refund_accepted": True,
                },
                "Vendor payments are non-refundable.",
                "v1",
            )

            availability = vendor_registration_open(db_path, 1)

            self.assertFalse(availability["open"])
            self.assertEqual(availability["status_label"], "Full")


def _create_active_show(db_path: Path) -> int:
    database.DB_PATH = str(db_path)
    database.init_db()
    show_id = database.create_show_admin(
        slug="active-test",
        flyer_image_path="",
        title="Active Test",
        date="2026-09-27",
        time="9:00 AM",
        location_name="Castle",
        address="123 Main",
        benefiting="Charity",
        suggested_donation="",
        description="Original",
        status="active",
        short_details="",
        qr_message="",
        cta_label="",
        cta_url="",
        show_on_site=1,
        sort_order=10,
    )
    database.set_active_show(show_id)
    return show_id


class ActiveShowEditingTests(unittest.TestCase):
    def test_active_show_edit_preserves_related_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "active-edit.db"
            show_id = _create_active_show(db_path)
            init_vendor_tables(str(db_path))
            save_vendor_settings(str(db_path), show_id, {"vendors_enabled": True, "vendor_public_status": "open"})
            save_vendor_packages(str(db_path), show_id, [{"name": "10x10", "capacity": 1, "is_active": True}])

            conn = sqlite3.connect(db_path)
            person_id = conn.execute(
                "INSERT INTO people (name, phone, email) VALUES ('Owner', '555', 'owner@example.com')"
            ).lastrowid
            car_id = conn.execute(
                """
                INSERT INTO show_cars (
                    show_id, person_id, car_number, car_token, year, make, model, registration_payment_status
                ) VALUES (?, ?, 1, 'token', '1965', 'VW', 'Beetle', 'paid')
                """,
                (show_id, person_id),
            ).lastrowid
            conn.execute(
                """
                INSERT INTO registration_intents (
                    show_id, intent_token, owner_name, phone, email, car_number, year, make, model,
                    waiver_signed_name, amount_cents, payment_status, finalized_show_car_id
                ) VALUES (?, 'intent', 'Owner', '555', 'owner@example.com', 1, '1965', 'VW',
                    'Beetle', 'Owner', 3500, 'paid', ?)
                """,
                (show_id, car_id),
            )
            conn.execute(
                """
                INSERT INTO vendor_registrations (
                    show_id, package_id, hold_token, confirmation_number, status, business_name,
                    contact_name, email, amount_cents
                ) VALUES (?, 1, 'hold', 'V-1', 'confirmed', 'Vendor', 'Contact',
                    'vendor@example.com', 7500)
                """,
                (show_id,),
            )
            conn.commit()
            conn.close()

            database.update_show_admin_record(
                show_id,
                slug="active-test",
                title="Active Test Updated",
                flyer_image_path="",
                date="2026-09-27",
                time="10:00 AM",
                location_name="Castle Grounds",
                address="123 Main",
                benefiting="Charity",
                suggested_donation="",
                description="Updated",
                status="active",
                short_details="",
                qr_message="",
                cta_label="",
                cta_url="",
                show_on_site=1,
                sort_order=10,
            )

            conn = sqlite3.connect(db_path)
            self.assertEqual(
                conn.execute("SELECT title FROM shows WHERE id = ?", (show_id,)).fetchone()[0],
                "Active Test Updated",
            )
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM show_cars WHERE show_id = ?", (show_id,)).fetchone()[0], 1)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM registration_intents WHERE show_id = ?", (show_id,)).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM vendor_registrations WHERE show_id = ?", (show_id,)).fetchone()[0],
                1,
            )
            conn.close()


if __name__ == "__main__":
    unittest.main()
