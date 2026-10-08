"""E2E: danger notifications and push-token registration through the REST gateway."""

import secrets
import unittest

from e2e_support import USERS, api, require_stack


NOTIFICATIONS = "/api/v1/notifications"
MEDIA, BNPB = USERS["MEDIA"], USERS["BNPB_INTERNAL"]


def setUpModule():
    require_stack()


class Notifications(unittest.TestCase):
    def test_summary_list_for_every_role(self):
        for role, user in USERS.items():
            with self.subTest(role=role):
                status, _, body = api("GET", NOTIFICATIONS, user=user)
                self.assertEqual(status, 200)
                self.assertIsInstance(body["items"], list)
                self.assertIsInstance(body["partial"], bool)

    def test_full_view_requires_full_scope(self):
        status, _, _ = api("GET", NOTIFICATIONS, user=MEDIA, query={"view": "full"})
        self.assertEqual(status, 403)
        for user in (USERS["FIELD_TEAM"], BNPB):
            with self.subTest(user=user):
                status, _, _ = api("GET", NOTIFICATIONS, user=user, query={"view": "full"})
                self.assertEqual(status, 200)

    def test_get_single_notification(self):
        status, _, body = api("GET", NOTIFICATIONS, user=BNPB, query={"page_size": 1})
        self.assertEqual(status, 200)
        if not body["items"]:
            self.skipTest("no SIAGA/AWAS hazard has produced a notification yet")
        notification_id = body["items"][0]["notification_id"]
        status, _, body = api("GET", f"{NOTIFICATIONS}/{notification_id}", user=BNPB)
        self.assertEqual(status, 200)
        self.assertEqual(body["item"]["notification_id"], notification_id)
        self.assertTrue(body["item"]["message"])

    def test_unknown_notification_is_404(self):
        status, _, _ = api("GET", f"{NOTIFICATIONS}/does-not-exist-e2e", user=BNPB)
        self.assertEqual(status, 404)

    def test_push_token_register_and_unregister(self):
        body = {"fcm_token": f"e2e-{secrets.token_hex(16)}"}
        status, _, _ = api("POST", f"{NOTIFICATIONS}/push/register", user=MEDIA, body=body)
        self.assertEqual(status, 204)
        status, _, _ = api("POST", f"{NOTIFICATIONS}/push/unregister", user=MEDIA, body=body)
        self.assertEqual(status, 204)


if __name__ == "__main__":
    unittest.main()
