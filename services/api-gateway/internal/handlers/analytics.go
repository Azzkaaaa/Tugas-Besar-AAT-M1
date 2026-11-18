package handlers

import (
	"context"
	"time"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (h *Handlers) GetAnalytics(ctx context.Context, request gw.GetAnalyticsRequestObject) (gw.GetAnalyticsResponseObject, error) {
	p := request.Params
	if p.Since != nil && p.Until != nil && !p.Since.Before(*p.Until) {
		return nil, problem.BadRequest("until must follow since")
	}
	req := &aggregatorv1.GetAnalyticsRequest{}
	if p.Source != nil {
		req.Source = string(*p.Source)
	}
	if p.Since != nil {
		req.Since = timestamppb.New(*p.Since)
	}
	if p.Until != nil {
		req.Until = timestamppb.New(*p.Until)
	}
	if p.Interval != nil {
		req.Interval = string(*p.Interval)
	}
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	resp, err := h.Clients.Aggregator.GetAnalytics(ctx, req)
	if err != nil {
		return nil, upstream(err)
	}
	out := gw.AnalyticsResult{Total: resp.Total, Categories: gw.AnalyticsCounts(resp.Categories), Severity: gw.AnalyticsCounts(resp.Severity), Areas: []gw.AnalyticsResult_Areas{}, Series: []gw.AnalyticsResult_Series{}, Partial: resp.Partial, UnavailableUnits: append([]string{}, resp.UnavailableUnits...), AsOf: resp.AsOf.AsTime()}
	if out.Categories == nil {
		out.Categories = gw.AnalyticsCounts{}
	}
	if out.Severity == nil {
		out.Severity = gw.AnalyticsCounts{}
	}
	for _, area := range resp.Areas {
		out.Areas = append(out.Areas, gw.AnalyticsResult_Areas{AreaName: area.AreaName, Count: area.Count})
	}
	for _, point := range resp.Series {
		out.Series = append(out.Series, gw.AnalyticsResult_Series{Bucket: point.Bucket.AsTime(), Counts: gw.AnalyticsCounts(point.Counts)})
	}
	return gw.GetAnalytics200JSONResponse(out), nil
}
