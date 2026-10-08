"""E2E: hazards ingested from the BMKG/PVMBG mocks, read through the REST gateway."""

import unittest

from e2e_support import FULL_ONLY, USERS, api, require_stack, wait_for


HAZARDS = "/api/v1/hazards"
MEDIA, FIELD, BNPB = USERS["MEDIA"], USERS["FIELD_TEAM"], USERS["BNPB_INTERNAL"]


def setUpModule():
    require_stack()


def items(user, **query):
    status, _, body = api("GET", HAZARDS, user=user, query=query)
    return body["items"] if status == 200 else []


class Hazards(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # The aggregator polls both mocks; wait until each source has produced data.
        for source in ("BMKG", "PVMBG"):
            wait_for(lambda: items(BNPB, source=source), message=f"{source} hazards")

    def test_summary_view_for_every_role(self):
        for role, user in USERS.items():
            with self.subTest(role=role):
                status, _, body = api("GET", HAZARDS, user=user)
                self.assertEqual(status, 200)
                self.assertTrue(body["items"])
                self.assertIsInstance(body["partial"], bool)
                self.assertTrue(body["freshness"])
                for item in body["items"]:
                    self.assertFalse(FULL_ONLY & item.keys(), "summary view leaked full-only fields")

    def test_full_view_requires_full_scope(self):
        status, _, _ = api("GET", HAZARDS, user=MEDIA, query={"view": "full"})
        self.assertEqual(status, 403)
        for user in (FIELD, BNPB):
            with self.subTest(user=user):
                status, _, body = api("GET", HAZARDS, user=user, query={"view": "full"})
                self.assertEqual(status, 200)
                for item in body["items"]:
                    self.assertIn("source_ref_id", item)
                    self.assertIn("latitude", item)
                    self.assertIn("longitude", item)

    def test_get_single_hazard(self):
        hazard_id = items(BNPB)[0]["hazard_id"]
        status, _, body = api("GET", f"{HAZARDS}/{hazard_id}", user=BNPB, query={"view": "full"})
        self.assertEqual(status, 200)
        self.assertEqual(body["item"]["hazard_id"], hazard_id)
        self.assertEqual(len(body["freshness"]), 1)
        status, _, _ = api("GET", f"{HAZARDS}/{hazard_id}", user=MEDIA, query={"view": "full"})
        self.assertEqual(status, 403)

    def test_unknown_hazard_is_404(self):
        status, _, _ = api("GET", f"{HAZARDS}/does-not-exist-e2e", user=BNPB)
        self.assertEqual(status, 404)

    def test_type_filter_implies_source(self):
        for hazard_type, source in (("SEISMIC", "BMKG"), ("VOLCANIC", "PVMBG")):
            with self.subTest(hazard_type=hazard_type):
                found = items(BNPB, hazard_type=hazard_type)
                self.assertTrue(found)
                self.assertEqual({item["source"] for item in found}, {source})
                self.assertEqual({item["hazard_type"] for item in found}, {hazard_type})

    def test_conflicting_source_and_type_is_400(self):
        status, _, _ = api("GET", HAZARDS, user=BNPB, query={"source": "PVMBG", "hazard_type": "SEISMIC"})
        self.assertEqual(status, 400)

    def test_pagination(self):
        status, _, first = api("GET", HAZARDS, user=BNPB, query={"page_size": 1})
        self.assertEqual(status, 200)
        self.assertEqual(len(first["items"]), 1)
        self.assertIn("next_page_token", first)
        status, _, second = api("GET", HAZARDS, user=BNPB,
                                query={"page_size": 1, "page_token": first["next_page_token"]})
        self.assertEqual(status, 200)
        self.assertNotEqual(second["items"][0]["hazard_id"], first["items"][0]["hazard_id"])

    def test_source_statuses(self):
        status, _, body = api("GET", f"{HAZARDS}/sources/statuses", user=MEDIA)
        self.assertEqual(status, 200)
        self.assertEqual({entry["source"] for entry in body["freshness"]}, {"BMKG", "PVMBG"})


if __name__ == "__main__":
    unittest.main()
