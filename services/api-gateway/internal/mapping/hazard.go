package mapping

import (
	"errors"
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
)

// Hazard renders a projection. The variant must match the requested view: a backend that
// returns FULL for a SUMMARY request is treated as unavailable rather than leaking fields.
func Hazard(projection *commonv1.HazardProjection, view commonv1.View) (gw.Hazard, error) {
	var out gw.Hazard
	switch data := projection.GetData().(type) {
	case *commonv1.HazardProjection_Summary:
		if view == commonv1.View_VIEW_FULL {
			return out, problem.InvalidUpstream(errors.New("summary projection for a full request"))
		}
		summary, err := hazardSummary(data.Summary)
		if err != nil {
			return out, err
		}
		return out, out.FromHazardSummary(summary)
	case *commonv1.HazardProjection_Full:
		if view != commonv1.View_VIEW_FULL {
			return out, problem.InvalidUpstream(errors.New("full projection for a summary request"))
		}
		full, err := hazardFull(data.Full)
		if err != nil {
			return out, err
		}
		return out, out.FromHazardFull(full)
	default:
		return out, problem.InvalidUpstream(errors.New("hazard projection is empty"))
	}
}

func Hazards(projections []*commonv1.HazardProjection, view commonv1.View) ([]gw.Hazard, error) {
	items := make([]gw.Hazard, 0, len(projections))
	for _, projection := range projections {
		item, err := Hazard(projection, view)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func hazardSummary(h *commonv1.HazardSummary) (gw.HazardSummary, error) {
	if h == nil || h.GetHazardId() == "" {
		return gw.HazardSummary{}, problem.InvalidUpstream(errors.New("hazard summary without hazard_id"))
	}
	core, err := hazardCore(h.GetSource(), h.GetHazardType(), h.GetSeverity(), h.GetOccurredAt(), h.GetIngestedAt())
	if err != nil {
		return gw.HazardSummary{}, err
	}
	core.HazardId, core.AreaName = h.GetHazardId(), h.GetAreaName()
	return core, nil
}

func hazardFull(h *commonv1.HazardEvent) (gw.HazardFull, error) {
	if h == nil || h.GetHazardId() == "" || h.GetSourceRefId() == "" {
		return gw.HazardFull{}, problem.InvalidUpstream(errors.New("full hazard without hazard_id or source_ref_id"))
	}
	if h.Latitude == nil || h.Longitude == nil || !finite(h.GetLatitude()) || !finite(h.GetLongitude()) {
		return gw.HazardFull{}, problem.InvalidUpstream(errors.New("full hazard without valid coordinates"))
	}
	core, err := hazardCore(h.GetSource(), h.GetHazardType(), h.GetSeverity(), h.GetOccurredAt(), h.GetIngestedAt())
	if err != nil {
		return gw.HazardFull{}, err
	}
	attributes := map[string]any{}
	if h.GetAttributes() != nil {
		attributes = h.GetAttributes().AsMap()
	}
	return gw.HazardFull{
		HazardId:    h.GetHazardId(),
		Source:      core.Source,
		HazardType:  core.HazardType,
		Severity:    core.Severity,
		AreaName:    h.GetAreaName(),
		OccurredAt:  core.OccurredAt,
		IngestedAt:  core.IngestedAt,
		SourceRefId: h.GetSourceRefId(),
		Latitude:    h.GetLatitude(),
		Longitude:   h.GetLongitude(),
		Attributes:  attributes,
	}, nil
}

func hazardCore(
	source commonv1.Source,
	hazardType commonv1.HazardType,
	severity commonv1.Severity,
	occurredAt, ingestedAt *timestamppb.Timestamp,
) (gw.HazardSummary, error) {
	var out gw.HazardSummary
	var err error
	if out.Source, err = sources.label(source); err != nil {
		return out, err
	}
	if out.HazardType, err = hazardTypes.label(hazardType); err != nil {
		return out, err
	}
	if out.Severity, err = severities.label(severity); err != nil {
		return out, err
	}
	if out.OccurredAt, err = RequiredTime("occurred_at", occurredAt); err != nil {
		return out, err
	}
	out.IngestedAt, err = RequiredTime("ingested_at", ingestedAt)
	return out, err
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

// RequiredTime converts a timestamp that the contract marks as required.
func RequiredTime(name string, ts *timestamppb.Timestamp) (time.Time, error) {
	if ts == nil || ts.CheckValid() != nil {
		return time.Time{}, problem.InvalidUpstream(fmt.Errorf("%s is missing or invalid", name))
	}
	return ts.AsTime().UTC(), nil
}

// OptionalTime converts a timestamp that may be absent (nil is omitted from the JSON).
func OptionalTime(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil || ts.CheckValid() != nil {
		return nil
	}
	t := ts.AsTime().UTC()
	return &t
}

func Freshness(items []*commonv1.SourceFreshness) ([]gw.Freshness, error) {
	out := make([]gw.Freshness, 0, len(items))
	for _, item := range items {
		source, err := sources.label(item.GetSource())
		if err != nil {
			return nil, err
		}
		status, err := sourceStatuses.label(item.GetStatus())
		if err != nil {
			return nil, err
		}
		out = append(out, gw.Freshness{
			Source:        source,
			Status:        status,
			LastSuccessAt: OptionalTime(item.GetLastSuccessAt()),
			StaleSince:    OptionalTime(item.GetStaleSince()),
		})
	}
	return out, nil
}
