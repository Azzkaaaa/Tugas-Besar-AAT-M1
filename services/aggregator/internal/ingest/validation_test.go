package ingest

import (
	"strings"
	"testing"
)

const validVolcanic = `{"report_id":"r1","volcano_id":"v-merapi","alert_level":"Normal","eruption_count_24h":0,"ash_column_height_m":0,"reported_at":"2026-11-12T03:00:00Z","confidence_level":0.81,"future":{"active":true}}`

func TestPVMBGSchemaAdapters(t *testing.T) {
	versions := map[string]string{
		"3": strings.Replace(validVolcanic, `"alert_level"`, `"volcano_alert"`, 1),
		"4": strings.Replace(validVolcanic, `"eruption_count_24h":0`, `"eruption_count_24h":"0"`, 1),
		"5": strings.Replace(validVolcanic, `"ash_column_height_m":0,`, ``, 1),
	}
	for version, raw := range versions {
		t.Run(version, func(t *testing.T) {
			if _, err := validateItem([]byte(raw), formatVolcanic, version, false); err == nil {
				t.Fatal("incompatible input passed baseline validation")
			}
			adapted, err := validateItem([]byte(raw), formatVolcanic, version, true)
			if err != nil {
				t.Fatal(err)
			}
			r := adapted.(*PVMBGVolcanicRaw)
			if r.AlertLevel != "Normal" || r.AshColumnHeightM != 0 || r.EruptionCount24h != 0 || r.RawFields["confidence_level"] != 0.81 || r.RawFields["future"] == nil {
				t.Fatalf("incorrect adapted report: %+v", r)
			}
		})
	}
	if _, err := validateItem([]byte(strings.Replace(versions["5"], `"Normal"`, `"Awas"`, 1)), formatVolcanic, "5", true); err == nil {
		t.Fatal("missing height on a non-Normal report was accepted")
	}
	if _, err := validateItem([]byte(versions["4"]), formatVolcanic, "2", true); err == nil {
		t.Fatal("undeclared type conversion was accepted")
	}
}

func TestRejectPoisonItems(t *testing.T) {
	for reason, data := range map[string][]byte{
		"invalid_json_object": []byte(validVolcanic[:len(validVolcanic)-3]),
		"invalid_utf8":        []byte(strings.Replace(validVolcanic, "v-merapi", "v-\xff", 1)),
		"json_depth_exceeded": []byte(`{"new":` + strings.Repeat("[", 40) + "0" + strings.Repeat("]", 40) + "}"),
		"payload_too_large":   []byte(strings.Repeat("x", maxItemBytes+1)),
	} {
		t.Run(reason, func(t *testing.T) {
			if _, err := validateItem(data, formatVolcanic, "2", false); err == nil || err.Error() != reason {
				t.Fatalf("want %s, got %v", reason, err)
			}
		})
	}
	if _, err := validateItem([]byte(validVolcanic), formatVolcanic, "2", false); err != nil {
		t.Fatal(err)
	}
}
