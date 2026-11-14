package domain

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Pinned values, computed independently of google/uuid: changing a namespace or a name format
// would re-key every hazard and event.
const (
	pinnedHazardID = "hz-112d668a-fafe-5415-b0f1-aa516f4b48aa"
	pinnedEventID  = "pub-4a214e3b-bfc0-5b2f-b40e-b6e1f9fcf948"
)

func TestHazardID(t *testing.T) {
	a := HazardID(SourceBMKG, "eq-1")
	if a != HazardID(SourceBMKG, "eq-1") {
		t.Fatal("same record gave different hazard IDs")
	}
	if a == HazardID(SourcePVMBG, "eq-1") || a == HazardID(SourceBMKG, "eq-2") {
		t.Fatal("different records share a hazard ID")
	}
	id, err := uuid.Parse(strings.TrimPrefix(a, "hz-"))
	if !strings.HasPrefix(a, "hz-") || err != nil || id.Version() != 5 {
		t.Fatalf("hazard ID %q is not hz-<uuidv5>", a)
	}
	if got := HazardID(SourceBMKG, "eq-pinned"); got != pinnedHazardID {
		t.Fatalf("HazardID changed: %s (was %s)", got, pinnedHazardID)
	}
}

func TestEventID(t *testing.T) {
	a := EventID("hz-1", 1)
	if a != EventID("hz-1", 1) {
		t.Fatal("same revision gave different event IDs")
	}
	if a == EventID("hz-1", 2) || a == EventID("hz-2", 1) {
		t.Fatal("different revisions or hazards share an event ID")
	}
	if got := EventID("hz-pinned", 7); got != pinnedEventID {
		t.Fatalf("EventID changed: %s (was %s)", got, pinnedEventID)
	}
}
