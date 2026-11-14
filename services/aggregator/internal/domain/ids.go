package domain

import (
	"strconv"

	"github.com/google/uuid"
)

// IDs are derived, never random, so every writer (any replica, a retry, two leaders during an
// overlap) computes the same ones. They are UUIDv5 name-based UUIDs: SHA-1 here is the standard
// name-to-UUID mapping, not a security measure, so anyone holding a source ID can compute the
// hazard ID. Never change the namespaces or the name formats: that would re-key every hazard and
// event.
var (
	hazardIDNamespace = uuid.MustParse("9c2e7a41-3b6d-5f08-8e1a-6d4b2c9f7e35")
	eventIDNamespace  = uuid.MustParse("4b0f7d3e-5a8c-4e7b-9a61-2f3c8d1e6b90")
)

// HazardID is the canonical ID of the upstream record (source, sourceRefID).
func HazardID(source, sourceRefID string) string {
	return "hz-" + uuid.NewSHA1(hazardIDNamespace, []byte(source+":"+sourceRefID)).String()
}

// EventID is the publication event_id of one committed revision of a hazard: a revision
// published twice carries one event_id, which consumers deduplicate, while every new revision
// gets a new one.
func EventID(hazardID string, revision int) string {
	return "pub-" + uuid.NewSHA1(eventIDNamespace, []byte(hazardID+":"+strconv.Itoa(revision))).String()
}
