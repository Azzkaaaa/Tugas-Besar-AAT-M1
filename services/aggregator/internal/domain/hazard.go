package domain

import (
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

	sourceEnum := commonv1.Source_SOURCE_UNSPECIFIED
	switch h.Source {
	case "BMKG":
		sourceEnum = commonv1.Source_SOURCE_BMKG
	case "PVMBG":
		sourceEnum = commonv1.Source_SOURCE_PVMBG
	}

	hazardTypeEnum := commonv1.HazardType_HAZARD_TYPE_UNSPECIFIED
	switch h.HazardType {
	case "SEISMIC":
		hazardTypeEnum = commonv1.HazardType_HAZARD_TYPE_SEISMIC
	case "VOLCANIC":
		hazardTypeEnum = commonv1.HazardType_HAZARD_TYPE_VOLCANIC
	}

	severityEnum := commonv1.Severity_SEVERITY_UNSPECIFIED
	switch h.Severity {
	case "NORMAL":
		severityEnum = commonv1.Severity_SEVERITY_NORMAL
	case "WASPADA":
		severityEnum = commonv1.Severity_SEVERITY_WASPADA
	case "SIAGA":
		severityEnum = commonv1.Severity_SEVERITY_SIAGA
	case "AWAS":
		severityEnum = commonv1.Severity_SEVERITY_AWAS
	}

	lat := h.Latitude
	lon := h.Longitude

	return &commonv1.HazardEvent{
		HazardId:    h.HazardID,
		Source:      sourceEnum,
		SourceRefId: h.SourceRefID,
		HazardType:  hazardTypeEnum,
		Severity:    severityEnum,
		AreaName:    h.AreaName,
		Latitude:    &lat,
		Longitude:   &lon,
		OccurredAt:  timestamppb.New(h.OccurredAt),
		IngestedAt:  timestamppb.New(h.IngestedAt),
		Attributes:  attrStruct,
	}, nil
}

func (h *HazardRecord) ToSummaryProto() *commonv1.HazardSummary {
	sourceEnum := commonv1.Source_SOURCE_UNSPECIFIED
	switch h.Source {
	case "BMKG":
		sourceEnum = commonv1.Source_SOURCE_BMKG
	case "PVMBG":
		sourceEnum = commonv1.Source_SOURCE_PVMBG
	}

	hazardTypeEnum := commonv1.HazardType_HAZARD_TYPE_UNSPECIFIED
	switch h.HazardType {
	case "SEISMIC":
		hazardTypeEnum = commonv1.HazardType_HAZARD_TYPE_SEISMIC
	case "VOLCANIC":
		hazardTypeEnum = commonv1.HazardType_HAZARD_TYPE_VOLCANIC
	}

	severityEnum := commonv1.Severity_SEVERITY_UNSPECIFIED
	switch h.Severity {
	case "NORMAL":
		severityEnum = commonv1.Severity_SEVERITY_NORMAL
	case "WASPADA":
		severityEnum = commonv1.Severity_SEVERITY_WASPADA
	case "SIAGA":
		severityEnum = commonv1.Severity_SEVERITY_SIAGA
	case "AWAS":
		severityEnum = commonv1.Severity_SEVERITY_AWAS
	}

	return &commonv1.HazardSummary{
		HazardId:   h.HazardID,
		Source:     sourceEnum,
		HazardType: hazardTypeEnum,
		Severity:   severityEnum,
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
