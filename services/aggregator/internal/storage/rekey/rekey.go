// Package rekey supports the one-off migration from random to derived hazard IDs
// (domain.HazardID): both store backends use it to rewrite outbox payloads still to be published.
package rekey

import (
	"fmt"

	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"google.golang.org/protobuf/proto"
)

// Payload returns the HazardEventPublished payload with payload.hazard_id set to hazardID. The
// event_id and everything else are kept.
func Payload(payload []byte, hazardID string) ([]byte, error) {
	var ev eventv1.HazardEventPublished
	if err := proto.Unmarshal(payload, &ev); err != nil {
		return nil, fmt.Errorf("decode outbox payload: %w", err)
	}
	if ev.Payload == nil {
		return nil, fmt.Errorf("outbox payload %s has no hazard", ev.EventId)
	}
	if ev.Payload.HazardId == hazardID {
		return payload, nil
	}
	ev.Payload.HazardId = hazardID
	return proto.Marshal(&ev)
}

// Identity returns the source, source_ref_id and hazard_id carried in a HazardEventPublished
// payload, so a migration can work out the event's derived hazard ID from the payload alone.
func Identity(payload []byte) (source, sourceRefID, hazardID string, err error) {
	var ev eventv1.HazardEventPublished
	if err := proto.Unmarshal(payload, &ev); err != nil {
		return "", "", "", fmt.Errorf("decode outbox payload: %w", err)
	}
	if ev.Payload == nil {
		return "", "", "", fmt.Errorf("outbox payload %s has no hazard", ev.EventId)
	}
	return domain.SourceFromProto(ev.Payload.Source), ev.Payload.SourceRefId, ev.Payload.HazardId, nil
}
