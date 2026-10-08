package dispatcher

import (
	"encoding/json"
	"fmt"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
)

// deliveryEnvelope is the JSON body sent to external webhook endpoints.
type deliveryEnvelope struct {
	EventID    string          `json:"event_id"`
	HazardID   string          `json:"hazard_id"`
	OccurredAt string          `json:"occurred_at"`
	Source     string          `json:"source"`
	Data       json.RawMessage `json:"data"`
}

// summaryPayload is the projected SUMMARY view (no coordinates, no attributes).
type summaryPayload struct {
	HazardID   string `json:"hazard_id"`
	Source     string `json:"source"`
	HazardType string `json:"hazard_type"`
	Severity   string `json:"severity"`
	AreaName   string `json:"area_name"`
	OccurredAt string `json:"occurred_at"`
	IngestedAt string `json:"ingested_at"`
}

// fullPayload is the FULL view — all canonical fields.
type fullPayload struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    *float64       `json:"latitude,omitempty"`
	Longitude   *float64       `json:"longitude,omitempty"`
	OccurredAt  string         `json:"occurred_at"`
	IngestedAt  string         `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes,omitempty"`
}

// pingEnvelope is sent for PingWebhook calls.
type pingEnvelope struct {
	Type      string `json:"type"`
	WebhookID string `json:"webhook_id"`
	Timestamp string `json:"timestamp"`
}

// BuildPayload projects the HazardEventPublished envelope according to the
// subscription view and serialises it as the JSON delivery body.
func BuildPayload(ev *eventv1.HazardEventPublished, view commonv1.View) ([]byte, error) {
	payload := ev.GetPayload()
	if payload == nil {
		return nil, fmt.Errorf("nil payload in HazardEventPublished")
	}

	srcName := sourceString(ev.Source)
	hazardType := hazardTypeString(payload.HazardType)
	severity := severityString(payload.Severity)

	var occurredAt, ingestedAt string
	if t := payload.GetOccurredAt(); t != nil {
		occurredAt = t.AsTime().UTC().Format(time.RFC3339)
	}
	if t := payload.GetIngestedAt(); t != nil {
		ingestedAt = t.AsTime().UTC().Format(time.RFC3339)
	}

	var dataJSON []byte
	var err error

	if view == commonv1.View_VIEW_FULL {
		fp := &fullPayload{
			HazardID:    payload.HazardId,
			Source:      srcName,
			SourceRefID: payload.SourceRefId,
			HazardType:  hazardType,
			Severity:    severity,
			AreaName:    payload.AreaName,
			OccurredAt:  occurredAt,
			IngestedAt:  ingestedAt,
		}
		if payload.Latitude != nil {
			v := *payload.Latitude
			fp.Latitude = &v
		}
		if payload.Longitude != nil {
			v := *payload.Longitude
			fp.Longitude = &v
		}
		// Convert google.protobuf.Struct to map[string]any
		if payload.Attributes != nil {
			fp.Attributes = payload.Attributes.AsMap()
		}
		dataJSON, err = json.Marshal(fp)
	} else {
		// Default: SUMMARY — never include coordinates, source_ref_id, or attributes.
		sp := &summaryPayload{
			HazardID:   payload.HazardId,
			Source:     srcName,
			HazardType: hazardType,
			Severity:   severity,
			AreaName:   payload.AreaName,
			OccurredAt: occurredAt,
			IngestedAt: ingestedAt,
		}
		dataJSON, err = json.Marshal(sp)
	}
	if err != nil {
		return nil, fmt.Errorf("marshalling payload data: %w", err)
	}

	var envelopeOccurredAt string
	if t := ev.GetOccurredAt(); t != nil {
		envelopeOccurredAt = t.AsTime().UTC().Format(time.RFC3339)
	}

	env := &deliveryEnvelope{
		EventID:    ev.EventId,
		HazardID:   payload.HazardId,
		OccurredAt: envelopeOccurredAt,
		Source:     srcName,
		Data:       json.RawMessage(dataJSON),
	}

	return json.Marshal(env)
}

// BuildPingPayload builds the JSON body for a PingWebhook test delivery.
func BuildPingPayload(webhookID string) ([]byte, error) {
	env := &pingEnvelope{
		Type:      "ping",
		WebhookID: webhookID,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	return json.Marshal(env)
}

// MatchesFilter returns true if the published event satisfies the subscription filter.
func MatchesFilter(ev *eventv1.HazardEventPublished, filterSource int32, filterHazard int32, filterSeverity int32, filterSince, filterUntil *time.Time) bool {
	payload := ev.GetPayload()
	if payload == nil {
		return false
	}

	if filterSource != 0 && int32(ev.Source) != filterSource {
		return false
	}
	if filterHazard != 0 && int32(payload.HazardType) != filterHazard {
		return false
	}
	if filterSeverity != 0 && int32(payload.Severity) != filterSeverity {
		return false
	}

	var eventTime time.Time
	if t := payload.GetOccurredAt(); t != nil {
		eventTime = t.AsTime()
	}

	if filterSince != nil && eventTime.Before(*filterSince) {
		return false
	}
	if filterUntil != nil && !eventTime.Before(*filterUntil) {
		return false
	}

	return true
}

// ---------------------------------------------------------------------------
// Enum helpers
// ---------------------------------------------------------------------------

func sourceString(s commonv1.Source) string {
	switch s {
	case commonv1.Source_SOURCE_BMKG:
		return "BMKG"
	case commonv1.Source_SOURCE_PVMBG:
		return "PVMBG"
	default:
		return "UNSPECIFIED"
	}
}

func hazardTypeString(h commonv1.HazardType) string {
	switch h {
	case commonv1.HazardType_HAZARD_TYPE_SEISMIC:
		return "SEISMIC"
	case commonv1.HazardType_HAZARD_TYPE_VOLCANIC:
		return "VOLCANIC"
	default:
		return "UNSPECIFIED"
	}
}

func severityString(sev commonv1.Severity) string {
	switch sev {
	case commonv1.Severity_SEVERITY_NORMAL:
		return "NORMAL"
	case commonv1.Severity_SEVERITY_WASPADA:
		return "WASPADA"
	case commonv1.Severity_SEVERITY_SIAGA:
		return "SIAGA"
	case commonv1.Severity_SEVERITY_AWAS:
		return "AWAS"
	default:
		return "UNSPECIFIED"
	}
}
