package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"
)

const (
	formatVolcanic = "volcanic"
	formatSeismic  = "seismic"
	formatWarning  = "warning"
	formatEnvelope = "envelope"
	maxItemBytes   = 256 << 10
	maxJSONDepth   = 32
)

type sourceItem struct {
	ref string
	at  time.Time
}

func itemMetadata(raw []byte, format string) sourceItem {
	var item struct {
		ReportID       string `json:"report_id"`
		EventID        string `json:"event_id"`
		RelatedEventID string `json:"related_event_id"`
		OccurredAt     string `json:"occurred_at"`
		ReportedAt     string `json:"reported_at"`
	}
	// Decode metadata from structurally safe items only. A field of an incompatible
	// type cannot prevent the decoder from reading the other fields.
	if len(raw) > maxItemBytes || !utf8.Valid(raw) || !validDepth(raw) {
		return sourceItem{}
	}
	_ = json.Unmarshal(raw, &item)
	switch format {
	case formatVolcanic:
		at, _ := time.Parse(time.RFC3339Nano, item.ReportedAt)
		return sourceItem{ref: item.ReportID, at: at}
	case formatSeismic:
		at, _ := time.Parse(time.RFC3339Nano, item.OccurredAt)
		return sourceItem{ref: item.EventID, at: at}
	default:
		return sourceItem{ref: item.RelatedEventID}
	}
}

func validateItem(raw []byte, format, version string, adapt bool) (any, error) {
	if len(raw) > maxItemBytes {
		return nil, errors.New("payload_too_large")
	}
	if !utf8.Valid(raw) {
		return nil, errors.New("invalid_utf8")
	}
	if !validDepth(raw) {
		return nil, errors.New("json_depth_exceeded")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, errors.New("invalid_json_object")
	}
	var value any
	var required []string
	switch format {
	case formatVolcanic:
		value = new(PVMBGVolcanicRaw)
		required = []string{"report_id", "volcano_id", "alert_level", "eruption_count_24h", "ash_column_height_m", "reported_at"}
		if adapt && version >= "3" && version <= "5" {
			adaptVolcanic(fields, version)
			raw, _ = json.Marshal(fields)
		}
	case formatSeismic:
		value = new(BMKGSeismicRaw)
		required = []string{"event_id", "magnitude", "depth_km", "epicenter_lat", "epicenter_lon", "region_name", "occurred_at", "potential_tsunami"}
	case formatWarning:
		value = new(BMKGTsunamiWarningRaw)
		required = []string{"warning_id", "related_event_id", "threat_level", "affected_zones", "estimated_arrival"}
	default:
		return nil, errors.New("invalid_envelope")
	}
	for _, name := range required {
		field := fields[name]
		if len(field) == 0 || bytes.Equal(field, []byte("null")) || bytes.Equal(field, []byte(`""`)) {
			return nil, fmt.Errorf("schema_incompatible: missing or empty %s", name)
		}
	}
	if err := json.Unmarshal(raw, value); err != nil {
		var typeError *json.UnmarshalTypeError
		if errors.As(err, &typeError) {
			return nil, fmt.Errorf("schema_incompatible: invalid type for %s", typeError.Field)
		}
		return nil, errors.New("schema_incompatible: invalid field value")
	}
	switch item := value.(type) {
	case *PVMBGVolcanicRaw:
		switch item.AlertLevel {
		case "Normal", "Waspada", "Siaga", "Awas":
		default:
			return nil, errors.New("schema_incompatible: invalid alert_level")
		}
		if confidence, exists := fields["confidence_level"]; exists {
			var amount *float64
			if json.Unmarshal(confidence, &amount) != nil || amount == nil || *amount < 0 || *amount > 1 {
				return nil, errors.New("schema_incompatible: invalid confidence_level")
			}
		}
	case *BMKGTsunamiWarningRaw:
		switch item.ThreatLevel {
		case "Waspada", "Siaga", "Awas":
		default:
			return nil, errors.New("schema_incompatible: invalid threat_level")
		}
	}
	return value, nil
}

func adaptVolcanic(fields map[string]json.RawMessage, version string) {
	switch version {
	case "3":
		if fields["alert_level"] == nil && fields["volcano_alert"] != nil {
			fields["alert_level"] = fields["volcano_alert"]
			delete(fields, "volcano_alert")
		}
	case "4":
		var text string
		if json.Unmarshal(fields["eruption_count_24h"], &text) == nil {
			if n, err := strconv.ParseInt(text, 10, 32); err == nil && n >= 0 && text == strconv.FormatInt(n, 10) {
				fields["eruption_count_24h"] = json.RawMessage(text)
			}
		}
	case "5":
		if fields["ash_column_height_m"] == nil && string(fields["alert_level"]) == `"Normal"` {
			fields["ash_column_height_m"] = json.RawMessage("0")
		}
	}
}

func validDepth(raw []byte) bool {
	depth := 0
	quoted, escaped := false, false
	for _, c := range raw {
		if quoted {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '{', '[':
			depth++
			if depth > maxJSONDepth {
				return false
			}
		case '}', ']':
			depth--
		}
	}
	return true
}
