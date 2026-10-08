package mapping

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
)

// Filter maps the flat REST filter parameters (nil = absent).
func Filter(source *gw.Source, hazardType *gw.HazardType, severity *gw.Severity, since, until *time.Time) (*commonv1.HazardFilter, error) {
	filter := &commonv1.HazardFilter{}
	var err error
	if filter.Source, err = sources.parse(source); err != nil {
		return nil, err
	}
	if filter.HazardType, err = hazardTypes.parse(hazardType); err != nil {
		return nil, err
	}
	if filter.Severity, err = severities.parse(severity); err != nil {
		return nil, err
	}
	if since != nil && until != nil && !since.Before(*until) {
		return nil, problem.BadRequest("since must precede until")
	}
	if since != nil {
		filter.Since = timestamppb.New(*since)
	}
	if until != nil {
		filter.Until = timestamppb.New(*until)
	}
	return filter, nil
}

// Page maps page_size/page_token. The request validator enforces 1..100; absent sends 0,
// which the backends resolve to 20 (common.proto PageRequest).
func Page(size *int, token *string) *commonv1.PageRequest {
	page := &commonv1.PageRequest{}
	if size != nil {
		page.PageSize = int32(*size)
	}
	if token != nil {
		page.PageToken = *token
	}
	return page
}

// WebhookFilter maps a CreateWebhookRequest filter (source, hazard_type, severity only).
func WebhookFilter(filter *gw.CreateWebhookRequest_Filter) (*commonv1.HazardFilter, error) {
	if filter == nil {
		return &commonv1.HazardFilter{}, nil
	}
	return Filter(filter.Source, filter.HazardType, filter.Severity, nil, nil)
}

// WebhookFilterLabels renders a subscription filter; UNSPECIFIED fields are omitted.
func WebhookFilterLabels(filter *commonv1.HazardFilter) (gw.WebhookSubscription_Filter, error) {
	var out gw.WebhookSubscription_Filter
	var err error
	if out.Source, err = sources.optionalLabel(filter.GetSource()); err != nil {
		return out, err
	}
	if out.HazardType, err = hazardTypes.optionalLabel(filter.GetHazardType()); err != nil {
		return out, err
	}
	out.Severity, err = severities.optionalLabel(filter.GetSeverity())
	return out, err
}

// Optional returns nil for the zero value, for optional string fields such as next_page_token.
func Optional[T comparable](value T) *T {
	var zero T
	if value == zero {
		return nil
	}
	return &value
}
