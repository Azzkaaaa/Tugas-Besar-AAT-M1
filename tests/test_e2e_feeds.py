"""E2E: live SSE feeds from event-live-feed, through Envoy and the REST gateway."""

import json
import re
import time
import unittest

from e2e_support import TIMEOUT, USERS, SseReader, api, require_stack


BNPB = USERS["BNPB_INTERNAL"]


def setUpModule():
    require_stack()


class Feeds(unittest.TestCase):
    def open(self, path, **query):
        reader = SseReader(path, user=BNPB, query=query)
        self.addCleanup(reader.close)
        self.assertEqual(reader.status, 200)
        self.assertTrue(reader.headers.get("content-type", "").startswith("text/event-stream"))
        return reader

    def next_upsert(self, reader):
        # Streams start at the live tail; the mocks generate new records periodically.
        deadline = time.monotonic() + TIMEOUT
        while True:
            event = reader.next_event(deadline)
            if event.get("event") == "hazard.upsert":
                return event

    def test_events_feed_delivers_upserts(self):
        event = self.next_upsert(self.open("/api/v1/feeds/events"))
        self.assertTrue(event.get("id"), "hazard.upsert must carry a resumable id")
        data = json.loads(event["data"])
        self.assertTrue(data["event_id"])
        self.assertRegex(data["sequence"], re.compile(r"^[1-9][0-9]{0,19}$"))
        self.assertTrue(data["hazard"]["hazard_id"])
        self.assertTrue(data["published_at"])

    def test_seismic_feed_only_seismic(self):
        data = json.loads(self.next_upsert(self.open("/api/v1/feeds/seismic"))["data"])
        self.assertEqual(data["hazard"]["hazard_type"], "SEISMIC")

    def test_volcanic_feed_only_volcanic(self):
        data = json.loads(self.next_upsert(self.open("/api/v1/feeds/volcanic"))["data"])
        self.assertEqual(data["hazard"]["hazard_type"], "VOLCANIC")

    def test_full_view_feed_requires_full_scope(self):
        status, _, _ = api("GET", "/api/v1/feeds/events", user=USERS["MEDIA"], query={"view": "full"})
        self.assertEqual(status, 403)


if __name__ == "__main__":
    unittest.main()
