package handlers

import (
	"context"

	aggregatorv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/aggregator/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/mapping"
)

func (h *Handlers) ListHazards(ctx context.Context, request gw.ListHazardsRequestObject) (gw.ListHazardsResponseObject, error) {
	p := request.Params
	view, err := mapping.View(p.View)
	if err != nil {
		return nil, err
	}
	filter, err := mapping.Filter(p.Source, p.HazardType, p.Severity, p.Since, p.Until)
	if err != nil {
		return nil, err
	}
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Aggregator.ListHazards(ctx, &aggregatorv1.ListHazardsRequest{Filter: filter, View: view, Page: mapping.Page(p.PageSize, p.PageToken)})
	if err != nil {
		return nil, upstream(err)
	}
	items, err := mapping.Hazards(resp.GetItems(), view)
	if err != nil {
		return nil, err
	}
	freshness, err := mapping.Freshness(resp.GetFreshness())
	if err != nil {
		return nil, err
	}
	return gw.ListHazards200JSONResponse{Body: gw.HazardPage{
		Items:         items,
		NextPageToken: mapping.Optional(resp.GetNextPageToken()),
		Freshness:     freshness,
		Partial:       resp.GetPartial(),
	}}, nil
}

func (h *Handlers) GetHazard(ctx context.Context, request gw.GetHazardRequestObject) (gw.GetHazardResponseObject, error) {
	view, err := mapping.View(request.Params.View)
	if err != nil {
		return nil, err
	}
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Aggregator.GetHazard(ctx, &aggregatorv1.GetHazardRequest{HazardId: request.HazardId, View: view})
	if err != nil {
		return nil, upstream(err)
	}
	item, err := mapping.Hazard(resp.GetItem(), view)
	if err != nil {
		return nil, err
	}
	freshness, err := mapping.Freshness(resp.GetFreshness())
	if err != nil {
		return nil, err
	}
	return gw.GetHazard200JSONResponse{Body: gw.HazardResult{Item: item, Freshness: freshness, Partial: resp.GetPartial()}}, nil
}

func (h *Handlers) GetSourceStatuses(ctx context.Context, _ gw.GetSourceStatusesRequestObject) (gw.GetSourceStatusesResponseObject, error) {
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Aggregator.GetSourceStatuses(ctx, &aggregatorv1.GetSourceStatusesRequest{})
	if err != nil {
		return nil, upstream(err)
	}
	freshness, err := mapping.Freshness(resp.GetFreshness())
	if err != nil {
		return nil, err
	}
	return gw.GetSourceStatuses200JSONResponse{Body: gw.GetSourceStatuses200JSONResponseBody{Freshness: &freshness}}, nil
}
