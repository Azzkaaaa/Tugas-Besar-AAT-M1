"""E2E: webhook subscriptions and signed delivery to a listener run by the test."""

import hashlib
import hmac
import json
import secrets
import unittest

from e2e_support import TIMEOUT, USERS, WebhookListener, api, require_stack, wait_for


WEBHOOKS = "/api/v1/webhooks"
MEDIA, FIELD = USERS["MEDIA"], USERS["FIELD_TEAM"]


def setUpModule():
    require_stack()


def signature(secret, body):
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookPermissions(unittest.TestCase):
    def test_media_can_read_but_not_write(self):
        status, _, body = api("GET", WEBHOOKS, user=MEDIA)
        self.assertEqual(status, 200)
        self.assertIsInstance(body["items"], list)
        status, _, _ = api("POST", WEBHOOKS, user=MEDIA, body={"url": "http://example.invalid/hook"})
        self.assertEqual(status, 403)


class WebhookLifecycle(unittest.TestCase):
    def setUp(self):
        self.listener = WebhookListener()
        self.addCleanup(self.listener.close)
        self.secret = secrets.token_urlsafe(24)
        status, _, body = api("POST", WEBHOOKS, user=FIELD, body={
            "url": self.listener.url, "secret": self.secret, "description": "e2e"})
        self.assertEqual(status, 201)
        self.webhook = body
        self.addCleanup(api, "DELETE", f"{WEBHOOKS}/{body['webhook_id']}", user=FIELD)

    def test_created_subscription_is_visible_to_owner(self):
        webhook_id = self.webhook["webhook_id"]
        self.assertEqual(self.webhook["url"], self.listener.url)
        self.assertEqual(self.webhook["view"], "summary")
        self.assertTrue(self.webhook["is_active"])
        self.assertNotIn("secret", self.webhook)
        status, _, body = api("GET", f"{WEBHOOKS}/{webhook_id}", user=FIELD)
        self.assertEqual(status, 200)
        self.assertEqual(body["webhook_id"], webhook_id)
        status, _, body = api("GET", WEBHOOKS, user=FIELD, query={"page_size": 100})
        self.assertEqual(status, 200)
        self.assertIn(webhook_id, [item["webhook_id"] for item in body["items"]])

    def test_ping_is_signed(self):
        status, _, body = api("POST", f"{WEBHOOKS}/{self.webhook['webhook_id']}/test", user=FIELD)
        self.assertEqual(status, 200)
        self.assertTrue(body["success"], body.get("message"))
        headers, raw = wait_for(lambda: self.listener.deliveries(), timeout=30, interval=1, message="ping delivery")[0]
        self.assertEqual(headers.get("x-bnpb-signature"), signature(self.secret, raw))
        json.loads(raw)

    def test_hazard_events_are_delivered_and_logged(self):
        webhook_id = self.webhook["webhook_id"]

        def hazard_delivery():
            for headers, raw in self.listener.deliveries():
                payload = json.loads(raw)
                if payload.get("hazard_id") and payload.get("type") != "ping":
                    self.assertEqual(headers.get("x-bnpb-signature"), signature(self.secret, raw))
                    return payload
            return None

        wait_for(hazard_delivery, timeout=TIMEOUT, interval=2, message="signed hazard delivery")

        def logged():
            status, _, body = api("GET", f"{WEBHOOKS}/{webhook_id}/deliveries", user=FIELD)
            return status == 200 and [item for item in body["items"] if item["status"] == "SUCCESS"]

        delivery = wait_for(logged, timeout=30, message="SUCCESS delivery log")[0]
        self.assertEqual(delivery["webhook_id"], webhook_id)
        self.assertTrue(200 <= delivery["status_code"] < 300)

    def test_delete(self):
        webhook_id = self.webhook["webhook_id"]
        status, _, _ = api("DELETE", f"{WEBHOOKS}/{webhook_id}", user=FIELD)
        self.assertEqual(status, 204)
        status, _, _ = api("GET", f"{WEBHOOKS}/{webhook_id}", user=FIELD)
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
