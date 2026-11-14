package domain

import (
	"encoding/json"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type HazardRecord struct {
	HazardID    string         `json:"hazard_id"`
	Source      string         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  string         `json:"hazard_type"`
	Severity    string         `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    float64        `json:"latitude"`
	Longitude   float64        `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
}

func (h *HazardRecord) ToProto() (*commonv1.HazardEvent, error) {
	attrStruct, err := structpb.NewStruct(h.Attributes)
	if err != nil {
		attrStruct = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}

	lat := h.Latitude
	lon := h.Longitude

	return &commonv1.HazardEvent{
		HazardId:    h.HazardID,
		Source:      SourceToProto(h.Source),
		SourceRefId: h.SourceRefID,
		HazardType:  HazardTypeToProto(h.HazardType),
		Severity:    SeverityToProto(h.Severity),
		AreaName:    h.AreaName,
		Latitude:    &lat,
		Longitude:   &lon,
		OccurredAt:  timestamppb.New(h.OccurredAt),
		IngestedAt:  timestamppb.New(h.IngestedAt),
		Attributes:  attrStruct,
	}, nil
}

func (h *HazardRecord) ToSummaryProto() *commonv1.HazardSummary {
	return &commonv1.HazardSummary{
		HazardId:   h.HazardID,
		Source:     SourceToProto(h.Source),
		HazardType: HazardTypeToProto(h.HazardType),
		Severity:   SeverityToProto(h.Severity),
		AreaName:   h.AreaName,
		OccurredAt: timestamppb.New(h.OccurredAt),
		IngestedAt: timestamppb.New(h.IngestedAt),
	}
}

func (h *HazardRecord) ToProjection(view commonv1.View) (*commonv1.HazardProjection, error) {
	if view == commonv1.View_VIEW_FULL {
		protoEvent, err := h.ToProto()
		if err != nil {
			return nil, err
		}
		return &commonv1.HazardProjection{
			Data: &commonv1.HazardProjection_Full{
				Full: protoEvent,
			},
		}, nil
	}

	// Default to SUMMARY projection (strictly stripping coordinates, source_ref_id, attributes)
	return &commonv1.HazardProjection{
		Data: &commonv1.HazardProjection_Summary{
			Summary: h.ToSummaryProto(),
		},
	}, nil
}

// NormalizeAttributes returns attrs as they read back from storage: a JSON round trip, so typed
// values ([]map[string]any, int, structs) become the generic forms (map[string]any, []any,
// float64) that structpb and change detection expect. Unencodable attributes become empty.
func NormalizeAttributes(attrs map[string]any) map[string]any {
	out := map[string]any{}
	b, err := json.Marshal(attrs)
	if err != nil {
		return out
	}
	if err := json.Unmarshal(b, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}
