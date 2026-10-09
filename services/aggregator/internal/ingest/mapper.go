package ingest

import (
	"encoding/json"
	"fmt"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BMKGSeismicRaw struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKM          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
	RawFields        map[string]any
}

func (s *BMKGSeismicRaw) UnmarshalJSON(b []byte) error {
	type Alias BMKGSeismicRaw
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(s),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	// Capture all dynamic/unknown fields
	var all map[string]any
	if err := json.Unmarshal(b, &all); err == nil {
		delete(all, "event_id")
		delete(all, "magnitude")
		delete(all, "depth_km")
		delete(all, "epicenter_lat")
		delete(all, "epicenter_lon")
		delete(all, "region_name")
		delete(all, "occurred_at")
		delete(all, "potential_tsunami")
		s.RawFields = all
	}
	return nil
}

type BMKGTsunamiWarningRaw struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"` // Waspada, Siaga, Awas
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
	RawFields        map[string]any
}

func (w *BMKGTsunamiWarningRaw) UnmarshalJSON(b []byte) error {
	type Alias BMKGTsunamiWarningRaw
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(w),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	var all map[string]any
	if err := json.Unmarshal(b, &all); err == nil {
		delete(all, "warning_id")
		delete(all, "related_event_id")
		delete(all, "threat_level")
		delete(all, "affected_zones")
		delete(all, "estimated_arrival")
		w.RawFields = all
	}
	return nil
}

type PVMBGVolcanicRaw struct {
	ReportID         string    `json:"report_id"`
	VolcanoID        string    `json:"volcano_id"`
	AlertLevel       string    `json:"alert_level"` // Normal, Waspada, Siaga, Awas
	EruptionCount24h int       `json:"eruption_count_24h"`
	AshColumnHeightM float64   `json:"ash_column_height_m"`
	ReportedAt       time.Time `json:"reported_at"`
	RawFields        map[string]any
}

func (v *PVMBGVolcanicRaw) UnmarshalJSON(b []byte) error {
	type Alias PVMBGVolcanicRaw
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(v),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	var all map[string]any
	if err := json.Unmarshal(b, &all); err == nil {
		delete(all, "report_id")
		delete(all, "volcano_id")
		delete(all, "alert_level")
		delete(all, "eruption_count_24h")
		delete(all, "ash_column_height_m")
		delete(all, "reported_at")
		v.RawFields = all
	}
	return nil
}

// MapBMKGToHazard maps a SeismicEvent and optional related TsunamiWarnings to a canonical HazardRecord
func MapBMKGToHazard(seismic *BMKGSeismicRaw, warnings []*BMKGTsunamiWarningRaw) *domain.HazardRecord {
	attributes := make(map[string]any)
	attributes["magnitude"] = seismic.Magnitude
	attributes["depth_km"] = seismic.DepthKM
	attributes["potential_tsunami"] = seismic.PotentialTsunami

	for k, v := range seismic.RawFields {
		attributes[k] = v
	}

	// Calculate severity per spec:
	// If TsunamiWarning with threat_level == Awas -> AWAS
	// Otherwise follows threat_level of related warning
	// If no warning: magnitude < 5.0 -> NORMAL, 5.0 <= m < 6.5 -> WASPADA, m >= 6.5 -> SIAGA
	severity := "NORMAL"
	if seismic.Magnitude >= 6.5 {
		severity = "SIAGA"
	} else if seismic.Magnitude >= 5.0 {
		severity = "WASPADA"
	}

	if len(warnings) > 0 {
		var warningList []map[string]any
		highestWarningThreat := ""

		for _, w := range warnings {
			wMap := map[string]any{
				"warning_id":        w.WarningID,
				"related_event_id":  w.RelatedEventID,
				"threat_level":      w.ThreatLevel,
				"affected_zones":    w.AffectedZones,
				"estimated_arrival": w.EstimatedArrival,
			}
			for k, v := range w.RawFields {
				wMap[k] = v
			}
			warningList = append(warningList, wMap)

			switch w.ThreatLevel {
			case "Awas":
				highestWarningThreat = "AWAS"
			case "Siaga":
				if highestWarningThreat != "AWAS" {
					highestWarningThreat = "SIAGA"
				}
			case "Waspada":
				if highestWarningThreat != "AWAS" && highestWarningThreat != "SIAGA" {
					highestWarningThreat = "WASPADA"
				}
			}
		}

		attributes["tsunami_warnings"] = warningList
		if highestWarningThreat != "" {
			severity = highestWarningThreat
		}
	}

	return &domain.HazardRecord{
		Source:      "BMKG",
		SourceRefID: seismic.EventID,
		HazardType:  "SEISMIC",
		Severity:    severity,
		AreaName:    seismic.RegionName,
		Latitude:    seismic.EpicenterLat,
		Longitude:   seismic.EpicenterLon,
		OccurredAt:  seismic.OccurredAt,
		Attributes:  attributes,
	}
}

// MapPVMBGToHazard maps a VolcanicReport to a canonical HazardRecord using static volcano reference
func MapPVMBGToHazard(volcanic *PVMBGVolcanicRaw) *domain.HazardRecord {
	attributes := make(map[string]any)
	attributes["eruption_count_24h"] = volcanic.EruptionCount24h
	attributes["ash_column_height_m"] = volcanic.AshColumnHeightM
	attributes["volcano_id"] = volcanic.VolcanoID

	for k, v := range volcanic.RawFields {
		attributes[k] = v
	}

	ref, ok := domain.LookupVolcano(volcanic.VolcanoID)
	areaName := volcanic.VolcanoID
	lat := 0.0
	lon := 0.0
	if ok {
		areaName = ref.Name
		lat = ref.Latitude
		lon = ref.Longitude
	}

	severity := "NORMAL"
	switch volcanic.AlertLevel {
	case "Normal":
		severity = "NORMAL"
	case "Waspada":
		severity = "WASPADA"
	case "Siaga":
		severity = "SIAGA"
	case "Awas":
		severity = "AWAS"
	}

	return &domain.HazardRecord{
		Source:      "PVMBG",
		SourceRefID: volcanic.ReportID,
		HazardType:  "VOLCANIC",
		Severity:    severity,
		AreaName:    areaName,
		Latitude:    lat,
		Longitude:   lon,
		OccurredAt:  volcanic.ReportedAt,
		Attributes:  attributes,
	}
}

// BuildPublishedEnvelope serializes a canonical hazard record into the binary protobuf envelope
func BuildPublishedEnvelope(h *domain.HazardRecord, correlationID string) (string, []byte, error) {
	protoEvent, err := h.ToProto()
	if err != nil {
		return "", nil, fmt.Errorf("failed converting hazard to proto: %w", err)
	}

	eventID := "pub-" + uuid.New().String()
	sourceEnum := commonv1.Source_SOURCE_UNSPECIFIED
	switch h.Source {
	case "BMKG":
		sourceEnum = commonv1.Source_SOURCE_BMKG
	case "PVMBG":
		sourceEnum = commonv1.Source_SOURCE_PVMBG
	}

	envelope := &eventv1.HazardEventPublished{
		EventId:       eventID,
		SchemaVersion: 1,
		Source:        sourceEnum,
		Publisher:     "aggregator",
		CorrelationId: correlationID,
		OccurredAt:    timestamppb.Now(),
		Payload:       protoEvent,
	}

	b, err := proto.Marshal(envelope)
	if err != nil {
		return "", nil, fmt.Errorf("failed marshaling outbox envelope: %w", err)
	}

	return eventID, b, nil
}
