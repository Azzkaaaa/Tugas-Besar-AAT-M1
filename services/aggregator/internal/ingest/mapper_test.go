package ingest

import (
	"encoding/json"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func TestMapBMKGToHazard_WithoutWarning(t *testing.T) {
	tests := []struct {
		name         string
		magnitude    float64
		wantSeverity string
	}{
		{
			name:         "Minor earthquake < 5.0 -> NORMAL",
			magnitude:    4.2,
			wantSeverity: "NORMAL",
		},
		{
			name:         "Moderate earthquake 5.0 <= mag < 6.5 -> WASPADA",
			magnitude:    5.8,
			wantSeverity: "WASPADA",
		},
		{
			name:         "Strong earthquake >= 6.5 -> SIAGA",
			magnitude:    7.1,
			wantSeverity: "SIAGA",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := &BMKGSeismicRaw{
				EventID:          "bmkg-test-1",
				Magnitude:        tc.magnitude,
				DepthKM:          15.0,
				EpicenterLat:     -6.2,
				EpicenterLon:     106.8,
				RegionName:       "Jakarta Selatan",
				OccurredAt:       time.Now().UTC(),
				PotentialTsunami: false,
			}

			hazard := MapBMKGToHazard(raw, nil)
			if hazard.Source != "BMKG" {
				t.Errorf("expected Source BMKG, got %s", hazard.Source)
			}
			if hazard.SourceRefID != "bmkg-test-1" {
				t.Errorf("expected SourceRefID bmkg-test-1, got %s", hazard.SourceRefID)
			}
			if hazard.HazardType != "SEISMIC" {
				t.Errorf("expected HazardType SEISMIC, got %s", hazard.HazardType)
			}
			if hazard.Severity != tc.wantSeverity {
				t.Errorf("expected Severity %s, got %s", tc.wantSeverity, hazard.Severity)
			}
			if hazard.AreaName != "Jakarta Selatan" {
				t.Errorf("expected AreaName Jakarta Selatan, got %s", hazard.AreaName)
			}
		})
	}
}

func TestMapBMKGToHazard_WithTsunamiWarning(t *testing.T) {
	raw := &BMKGSeismicRaw{
		EventID:          "bmkg-tsunami-1",
		Magnitude:        7.8,
		DepthKM:          10.0,
		EpicenterLat:     -8.5,
		EpicenterLon:     115.2,
		RegionName:       "Selat Bali",
		OccurredAt:       time.Now().UTC(),
		PotentialTsunami: true,
	}

	warnings := []*BMKGTsunamiWarningRaw{
		{
			WarningID:        "tsunami-warn-101",
			RelatedEventID:   "bmkg-tsunami-1",
			ThreatLevel:      "Awas",
			EstimatedArrival: time.Now().Add(30 * time.Minute).UTC(),
		},
	}

	hazard := MapBMKGToHazard(raw, warnings)
	if hazard.Severity != "AWAS" {
		t.Errorf("expected Severity AWAS from warning, got %s", hazard.Severity)
	}

	// Verify tsunami warnings list is present in attributes
	warns, ok := hazard.Attributes["tsunami_warnings"].([]map[string]any)
	if !ok || len(warns) == 0 {
		t.Fatalf("expected tsunami_warnings in attributes, got %v", hazard.Attributes["tsunami_warnings"])
	}
	if warns[0]["warning_id"] != "tsunami-warn-101" {
		t.Errorf("expected warning_id tsunami-warn-101, got %v", warns[0]["warning_id"])
	}
}

func TestMapPVMBGToHazard_SchemaEvolution(t *testing.T) {
	// 1. Schema v1 (without confidence_level)
	rawV1JSON := []byte(`{
		"report_id": "pvmbg-rep-001",
		"volcano_id": "merapi",
		"alert_level": "Siaga",
		"eruption_count_24h": 3,
		"ash_column_height_m": 500,
		"reported_at": "2026-10-09T00:00:00Z",
		"seismicity_count": 42
	}`)

	var rawV1 PVMBGVolcanicRaw
	if err := json.Unmarshal(rawV1JSON, &rawV1); err != nil {
		t.Fatalf("failed unmarshaling v1 json: %v", err)
	}

	hazardV1 := MapPVMBGToHazard(&rawV1)
	if hazardV1.AreaName != "Gunung Merapi" {
		t.Errorf("expected AreaName Gunung Merapi mapped from volcano ID, got %s", hazardV1.AreaName)
	}
	if hazardV1.Severity != "SIAGA" {
		t.Errorf("expected Severity SIAGA, got %s", hazardV1.Severity)
	}
	if hazardV1.Attributes["confidence_level"] != nil {
		t.Errorf("confidence_level should be nil in v1, got %v", hazardV1.Attributes["confidence_level"])
	}
	if num, ok := hazardV1.Attributes["seismicity_count"].(float64); !ok || num != 42 {
		t.Errorf("additive dynamic field should be preserved in attributes, got %v", hazardV1.Attributes["seismicity_count"])
	}

	// 2. Schema v2 (with confidence_level and unexpected fields from schema version bump)
	rawV2JSON := []byte(`{
		"report_id": "pvmbg-rep-002",
		"volcano_id": "anak-krakatau",
		"alert_level": "Awas",
		"eruption_count_24h": 12,
		"ash_column_height_m": 2000,
		"reported_at": "2026-10-09T00:00:00Z",
		"confidence_level": 0.95,
		"sensor_status": "nominal"
	}`)

	var rawV2 PVMBGVolcanicRaw
	if err := json.Unmarshal(rawV2JSON, &rawV2); err != nil {
		t.Fatalf("failed unmarshaling v2 json: %v", err)
	}

	hazardV2 := MapPVMBGToHazard(&rawV2)
	if hazardV2.AreaName != "Gunung Anak Krakatau" {
		t.Errorf("expected AreaName Gunung Anak Krakatau, got %s", hazardV2.AreaName)
	}
	if hazardV2.Severity != "AWAS" {
		t.Errorf("expected Severity AWAS, got %s", hazardV2.Severity)
	}
	if hazardV2.Attributes["confidence_level"] != 0.95 {
		t.Errorf("expected confidence_level 0.95 in v2 attributes, got %v", hazardV2.Attributes["confidence_level"])
	}
	if hazardV2.Attributes["sensor_status"] != "nominal" {
		t.Errorf("expected sensor_status 'nominal' in v2 attributes, got %v", hazardV2.Attributes["sensor_status"])
	}
}

func TestProjection_MediaVsFull(t *testing.T) {
	record := &domain.HazardRecord{
		HazardID:    "h-123",
		Source:      "BMKG",
		SourceRefID: "ref-456",
		HazardType:  "SEISMIC",
		Severity:    "WASPADA",
		AreaName:    "Sumatera Barat",
		Latitude:    -0.9,
		Longitude:   100.3,
		OccurredAt:  time.Now().UTC(),
		IngestedAt:  time.Now().UTC(),
		Attributes: map[string]any{
			"depth_km": 20.0,
		},
	}

	// Test SUMMARY view (Media projection - 7 fields only)
	projSummary, err := record.ToProjection(commonv1.View_VIEW_SUMMARY)
	if err != nil {
		t.Fatalf("unexpected projection error: %v", err)
	}

	summary := projSummary.GetSummary()
	if summary == nil {
		t.Fatalf("expected Summary payload in VIEW_SUMMARY projection")
	}

	if summary.HazardId != "h-123" || summary.AreaName != "Sumatera Barat" {
		t.Errorf("summary fields missing or incorrect")
	}
	if summary.Source != commonv1.Source_SOURCE_BMKG {
		t.Errorf("expected SOURCE_BMKG, got %v", summary.Source)
	}
	if summary.Severity != commonv1.Severity_SEVERITY_WASPADA {
		t.Errorf("expected SEVERITY_WASPADA, got %v", summary.Severity)
	}

	// Test FULL view (BNPB internal / Field team)
	projFull, err := record.ToProjection(commonv1.View_VIEW_FULL)
	if err != nil {
		t.Fatalf("unexpected projection error: %v", err)
	}

	full := projFull.GetFull()
	if full == nil {
		t.Fatalf("expected Full payload in VIEW_FULL projection")
	}

	if full.GetLatitude() != -0.9 || full.GetLongitude() != 100.3 {
		t.Errorf("Coordinates missing or incorrect in full view")
	}
	if full.SourceRefId != "ref-456" {
		t.Errorf("SourceRefId should be ref-456, got %s", full.SourceRefId)
	}
	if full.Attributes == nil || full.Attributes.Fields["depth_km"].GetNumberValue() != 20.0 {
		t.Errorf("Attributes should contain depth_km in full view")
	}
}

func TestRawFieldsCannotOverwriteNormalizedAttributes(t *testing.T) {
	// 1. PVMBG: RawFields with conflicting normalized keys
	rawPVMBG := &PVMBGVolcanicRaw{
		ReportID:         "rep-test-01",
		VolcanoID:        "merapi",
		AlertLevel:       "Siaga",
		EruptionCount24h: 7,
		AshColumnHeightM: 1500.0,
		ReportedAt:       time.Now().UTC(),
		RawFields: map[string]any{
			"volcano_id":          "attacker-overwrite",
			"ash_column_height_m": 99999.0,
			"eruption_count_24h":  -99,
			"custom_extra_field":  "preserved",
		},
	}

	hazardPVMBG := MapPVMBGToHazard(rawPVMBG)
	if hazardPVMBG.Attributes["volcano_id"] != "merapi" {
		t.Errorf("expected normalized volcano_id 'merapi', got %v", hazardPVMBG.Attributes["volcano_id"])
	}
	if hazardPVMBG.Attributes["ash_column_height_m"] != 1500.0 {
		t.Errorf("expected normalized ash_column_height_m 1500.0, got %v", hazardPVMBG.Attributes["ash_column_height_m"])
	}
	if hazardPVMBG.Attributes["eruption_count_24h"] != 7 {
		t.Errorf("expected normalized eruption_count_24h 7, got %v", hazardPVMBG.Attributes["eruption_count_24h"])
	}
	if hazardPVMBG.Attributes["custom_extra_field"] != "preserved" {
		t.Errorf("expected non-conflicting field to be preserved, got %v", hazardPVMBG.Attributes["custom_extra_field"])
	}

	// 2. BMKG: RawFields with conflicting normalized keys
	rawBMKG := &BMKGSeismicRaw{
		EventID:          "eq-test-01",
		Magnitude:        6.2,
		DepthKM:          25.0,
		EpicenterLat:     -6.0,
		EpicenterLon:     106.0,
		RegionName:       "Jawa",
		OccurredAt:       time.Now().UTC(),
		PotentialTsunami: false,
		RawFields: map[string]any{
			"magnitude":         1.1,
			"depth_km":          999.0,
			"potential_tsunami": true,
			"extra_bmkg_prop":   "keep_me",
		},
	}

	warnings := []*BMKGTsunamiWarningRaw{
		{
			WarningID:        "w-01",
			RelatedEventID:   "eq-test-01",
			ThreatLevel:      "Waspada",
			AffectedZones:    []string{"Zone A"},
			EstimatedArrival: time.Now().UTC(),
			RawFields: map[string]any{
				"warning_id":   "overwrite_warn",
				"threat_level": "none",
			},
		},
	}

	hazardBMKG := MapBMKGToHazard(rawBMKG, warnings)
	if hazardBMKG.Attributes["magnitude"] != 6.2 {
		t.Errorf("expected normalized magnitude 6.2, got %v", hazardBMKG.Attributes["magnitude"])
	}
	if hazardBMKG.Attributes["depth_km"] != 25.0 {
		t.Errorf("expected normalized depth_km 25.0, got %v", hazardBMKG.Attributes["depth_km"])
	}
	if hazardBMKG.Attributes["potential_tsunami"] != false {
		t.Errorf("expected normalized potential_tsunami false, got %v", hazardBMKG.Attributes["potential_tsunami"])
	}
	if hazardBMKG.Attributes["extra_bmkg_prop"] != "keep_me" {
		t.Errorf("expected extra_bmkg_prop to be preserved, got %v", hazardBMKG.Attributes["extra_bmkg_prop"])
	}

	warnsList := hazardBMKG.Attributes["tsunami_warnings"].([]map[string]any)
	if warnsList[0]["warning_id"] != "w-01" {
		t.Errorf("expected warning_id 'w-01', got %v", warnsList[0]["warning_id"])
	}
	if warnsList[0]["threat_level"] != "Waspada" {
		t.Errorf("expected threat_level 'Waspada', got %v", warnsList[0]["threat_level"])
	}
}
