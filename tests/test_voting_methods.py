import os
import tempfile
import unittest


class VotingMethodTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        cls.db_path = os.path.join(cls.temp_dir.name, "voting-methods.db")
        os.environ["DB_PATH"] = cls.db_path
        os.environ["APP_ENV"] = "testing"
        os.environ["FLASK_SECRET"] = "test-secret"
        os.environ["ADMIN_PASSWORD"] = "test-password"

        import database
        database.DB_PATH = cls.db_path
        database.init_db()

        import sponsorship_system
        sponsorship_system.DB_PATH = cls.db_path

        import app
        cls.database = database
        cls.app_module = app
        cls.app = app.app
        cls.app.config.update(TESTING=True)
        cls.counter = 0

    @classmethod
    def tearDownClass(cls):
        cls.temp_dir.cleanup()

    def _show(self, voting_method="both"):
        type(self).counter += 1
        show_id = self.database.create_show_admin(
            slug=f"voting-method-{type(self).counter}",
            flyer_image_path="",
            title="Voting Method Test",
            date="2026-07-01",
            time="",
            location_name="Test Lot",
            address="",
            benefiting="Test Charity",
            suggested_donation="",
            description="",
            status="active",
            short_details="",
            qr_message="",
            cta_label="",
            cta_url="",
            show_on_site=1,
            sort_order=1,
            voting_method=voting_method,
        )
        self.database.set_show_voting_open(show_id, True)
        return self.database.get_show_by_id(show_id)

    def _car(self, show, number=7, checked_in=True, status="paid"):
        conn = self.database._conn()
        cur = conn.cursor()
        cur.execute("INSERT INTO people (name, phone, email) VALUES (?, ?, ?)", ("Owner", "", ""))
        person_id = int(cur.lastrowid)
        token = f"token-{show['id']}-{number}-{checked_in}-{status}"
        cur.execute(
            """
            INSERT INTO show_cars (
                show_id, person_id, car_number, car_token, year, make, model,
                registration_payment_status, registration_state, checked_in_at
            )
            VALUES (?, ?, ?, ?, '1967', 'Ford', 'Mustang', ?, 'claimed', ?)
            """,
            (
                int(show["id"]),
                person_id,
                int(number),
                token,
                status,
                "2026-07-01 10:00:00" if checked_in else None,
            ),
        )
        car_id = int(cur.lastrowid)
        conn.commit()
        conn.close()
        return car_id, token

    def test_car_number_voting_accepts_leading_zeros(self):
        show = self._show("both")
        self._car(show, 7)

        client = self.app.test_client()
        response = client.post(
            f"/vote/{show['slug']}",
            data={"car_number": "007", "category_slug": "peoples-choice"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Confirm Car", response.data)
        self.assertIn(b"1967", response.data)
        self.assertNotIn(b"Owner:", response.data)

    def test_voting_method_enforcement(self):
        qr_show = self._show("qr_only")
        self._car(qr_show, 7)
        response = self.app.test_client().get(f"/vote/{qr_show['slug']}")
        self.assertEqual(response.status_code, 403)

        number_show = self._show("number_only")
        _, token = self._car(number_show, 8)
        response = self.app.test_client().get(
            f"/v/{number_show['slug']}/{token}/peoples-choice"
        )
        self.assertEqual(response.status_code, 403)

        disabled_show = self._show("disabled")
        _, token = self._car(disabled_show, 9)
        response = self.app.test_client().get(
            f"/v/{disabled_show['slug']}/{token}/peoples-choice"
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Voting is closed", response.data)

    def test_car_number_lookup_rejects_unchecked_or_canceled_cars(self):
        show = self._show("both")
        self._car(show, 10, checked_in=False)
        self._car(show, 11, checked_in=True, status="canceled")

        unchecked = self.database.find_vote_car_by_number(int(show["id"]), "10")
        canceled = self.database.find_vote_car_by_number(int(show["id"]), "11")

        self.assertEqual(unchecked["status"], "not_found")
        self.assertEqual(canceled["status"], "not_found")

    def test_entry_method_is_preserved_when_vote_is_finalized(self):
        show = self._show("both")
        car_id, _ = self._car(show, 12)

        vote_intent_id = self.database.create_vote_intent(
            int(show["id"]),
            car_id,
            "People's Choice",
            2,
            200,
            entry_method="car_number",
        )
        conn = self.database._conn()
        conn.execute(
            "UPDATE vote_intents SET stripe_session_id = ? WHERE id = ?",
            ("sess_entry_method", vote_intent_id),
        )
        conn.commit()
        conn.close()

        self.database.finalize_vote_intent_paid("sess_entry_method")
        self.database.finalize_vote_intent_paid("sess_entry_method")

        rows = self.database.export_votes_for_show(int(show["id"]))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["entry_method"], "car_number")
        self.assertEqual(rows[0]["vote_qty"], 2)


if __name__ == "__main__":
    unittest.main()
