package dispatcher

import (
	"encoding/json"
	"testing"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	eventv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/event/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPayloadPreservesAllSourcesTypesAndViewBoundaries(t *testing.T) {
	for _, tc := range []struct {
		source           commonv1.Source
		kind             commonv1.HazardType
		label, kindLabel string
	}{
		{commonv1.Source_SOURCE_BMKG, commonv1.HazardType_HAZARD_TYPE_SEISMIC, "BMKG", "SEISMIC"},
		{commonv1.Source_SOURCE_PVMBG, commonv1.HazardType_HAZARD_TYPE_VOLCANIC, "PVMBG", "VOLCANIC"},
		{commonv1.Source_SOURCE_SATELITE, commonv1.HazardType_HAZARD_TYPE_WILDFIRE, "SATELIT", "WILDFIRE"},
		{commonv1.Source_SOURCE_COAST_SENSOR, commonv1.HazardType_HAZARD_TYPE_COASTAL, "SENSOR_PANTAI", "COASTAL"},
	} {
		for _, view := range []commonv1.View{commonv1.View_VIEW_SUMMARY, commonv1.View_VIEW_FULL} {
			t.Run(tc.label+"/"+view.String(), func(t *testing.T) {
				attributes, _ := structpb.NewStruct(map[string]any{"critical": true, "detection_count": 20})
				latitude, longitude := -2.5, 110.0
				event := &eventv1.HazardEventPublished{EventId: "publication", Source: tc.source,
					Payload: &commonv1.HazardEvent{HazardId: "hazard", Source: tc.source, SourceRefId: "observation",
						HazardType: tc.kind, Severity: commonv1.Severity_SEVERITY_AWAS, Latitude: &latitude, Longitude: &longitude, Attributes: attributes}}
				body, err := BuildPayload(event, view)
				if err != nil {
					t.Fatal(err)
				}
				var envelope struct {
					EventID string         `json:"event_id"`
					Source  string         `json:"source"`
					Data    map[string]any `json:"data"`
				}
				if err = json.Unmarshal(body, &envelope); err != nil {
					t.Fatal(err)
				}
				if envelope.EventID != "publication" || envelope.Source != tc.label || envelope.Data["source"] != tc.label || envelope.Data["hazard_type"] != tc.kindLabel {
					t.Fatalf("lost event identity/source/type: %s", body)
				}
				for _, key := range []string{"source_ref_id", "latitude", "longitude", "attributes"} {
					_, exists := envelope.Data[key]
					if exists != (view == commonv1.View_VIEW_FULL) {
						t.Fatalf("incorrect %s projection: %s", key, body)
					}
				}
				if view == commonv1.View_VIEW_FULL && envelope.Data["attributes"].(map[string]any)["critical"] != true {
					t.Fatal("critical marker lost")
				}
			})
		}
	}
}
