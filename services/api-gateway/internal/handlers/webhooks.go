package handlers

import (
	"context"

	webhookv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/webhook/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/mapping"
)

func (h *Handlers) ListWebhooks(ctx context.Context, request gw.ListWebhooksRequestObject) (gw.ListWebhooksResponseObject, error) {
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Webhook.ListWebhooks(ctx, &webhookv1.ListWebhooksRequest{Page: mapping.Page(request.Params.PageSize, request.Params.PageToken)})
	if err != nil {
		return nil, upstream(err)
	}
	items, err := mapping.Webhooks(resp.GetItems())
	if err != nil {
		return nil, err
	}
	return gw.ListWebhooks200JSONResponse{Body: gw.WebhookPage{Items: items, NextPageToken: mapping.Optional(resp.GetNextPageToken())}}, nil
}

func (h *Handlers) CreateWebhook(ctx context.Context, request gw.CreateWebhookRequestObject) (gw.CreateWebhookResponseObject, error) {
	body := request.Body
	view, err := mapping.View(body.View)
	if err != nil {
		return nil, err
	}
	filter, err := mapping.WebhookFilter(body.Filter)
	if err != nil {
		return nil, err
	}
	req := &webhookv1.CreateWebhookRequest{Url: body.Url, Secret: body.Secret, Filter: filter, View: view}
	if body.Description != nil {
		req.Description = *body.Description
	}
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Webhook.CreateWebhook(ctx, req)
	if err != nil {
		return nil, upstream(err)
	}
	// The generated WebhookSubscription has no secret field, so it can never be echoed.
	webhook, err := mapping.Webhook(resp.GetSubscription())
	if err != nil {
		return nil, err
	}
	return gw.CreateWebhook201JSONResponse{Body: webhook}, nil
}

func (h *Handlers) GetWebhook(ctx context.Context, request gw.GetWebhookRequestObject) (gw.GetWebhookResponseObject, error) {
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Webhook.GetWebhook(ctx, &webhookv1.GetWebhookRequest{WebhookId: request.WebhookId})
	if err != nil {
		return nil, upstream(err)
	}
	webhook, err := mapping.Webhook(resp.GetSubscription())
	if err != nil {
		return nil, err
	}
	return gw.GetWebhook200JSONResponse{Body: webhook}, nil
}

// DeleteWebhook answers 204; DeleteWebhookResponse.success has no REST meaning.
func (h *Handlers) DeleteWebhook(ctx context.Context, request gw.DeleteWebhookRequestObject) (gw.DeleteWebhookResponseObject, error) {
	ctx, cancel := h.unary(ctx)
	defer cancel()
	if _, err := h.Clients.Webhook.DeleteWebhook(ctx, &webhookv1.DeleteWebhookRequest{WebhookId: request.WebhookId}); err != nil {
		return nil, upstream(err)
	}
	return gw.DeleteWebhook204Response{}, nil
}

func (h *Handlers) ListWebhookDeliveries(ctx context.Context, request gw.ListWebhookDeliveriesRequestObject) (gw.ListWebhookDeliveriesResponseObject, error) {
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Webhook.ListDeliveries(ctx, &webhookv1.ListDeliveriesRequest{
		WebhookId: request.WebhookId,
		Page:      mapping.Page(request.Params.PageSize, request.Params.PageToken),
	})
	if err != nil {
		return nil, upstream(err)
	}
	items, err := mapping.Deliveries(resp.GetItems())
	if err != nil {
		return nil, err
	}
	return gw.ListWebhookDeliveries200JSONResponse{Body: gw.WebhookDeliveryPage{Items: items, NextPageToken: mapping.Optional(resp.GetNextPageToken())}}, nil
}

// TestWebhook maps to PingWebhook; the contract defines no request body.
func (h *Handlers) TestWebhook(ctx context.Context, request gw.TestWebhookRequestObject) (gw.TestWebhookResponseObject, error) {
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Webhook.PingWebhook(ctx, &webhookv1.PingWebhookRequest{WebhookId: request.WebhookId})
	if err != nil {
		return nil, upstream(err)
	}
	return gw.TestWebhook200JSONResponse{Body: gw.PingWebhookResult{
		StatusCode: int(resp.GetStatusCode()),
		DurationMs: mapping.Millis(resp.GetDurationMs()),
		Success:    resp.GetSuccess(),
		Message:    resp.GetMessage(),
	}}, nil
}
