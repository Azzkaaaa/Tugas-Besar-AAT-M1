"""E2E: Envoy authentication for non-browser clients (service users, bearer tokens)."""

import unittest

from e2e_support import USERS, api, require_stack, token


STATUSES = "/api/v1/hazards/sources/statuses"


def setUpModule():
    require_stack()


class BearerAuthentication(unittest.TestCase):
    def test_every_service_user_reaches_the_api(self):
        for role, user in USERS.items():
            with self.subTest(role=role):
                status, _, body = api("GET", STATUSES, user=user)
                self.assertEqual(status, 200)
                self.assertIsInstance(body.get("freshness"), list)

    def test_missing_credentials_rejected(self):
        status, _, _ = api("GET", STATUSES)
        self.assertEqual(status, 401)

    def test_invalid_bearer_rejected(self):
        status, _, _ = api("GET", STATUSES, headers={"Authorization": "Bearer not-a-real-token"})
        self.assertEqual(status, 401)

    def test_bearer_with_auth_cookie_is_ambiguous(self):
        status, _, _ = api("GET", STATUSES, headers={
            "Authorization": f"Bearer {token(USERS['BNPB_INTERNAL'])}", "Cookie": "bnpb_access=anything"})
        self.assertEqual(status, 400)

    def test_bearer_mutation_needs_no_csrf(self):
        # CSRF/Origin checks belong to the cookie path; an invalid body must reach the gateway (400), not 403.
        status, _, _ = api("POST", "/api/v1/webhooks", user=USERS["BNPB_INTERNAL"], body={})
        self.assertEqual(status, 400)


if __name__ == "__main__":
    unittest.main()
