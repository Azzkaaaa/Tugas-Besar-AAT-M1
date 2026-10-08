package handlers

import (
	"context"

	notificationv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/notification/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/mapping"
)

func (h *Handlers) ListNotifications(ctx context.Context, request gw.ListNotificationsRequestObject) (gw.ListNotificationsResponseObject, error) {
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
	resp, err := h.Clients.Notification.ListNotifications(ctx, &notificationv1.ListNotificationsRequest{Filter: filter, View: view, Page: mapping.Page(p.PageSize, p.PageToken)})
	if err != nil {
		return nil, upstream(err)
	}
	items, err := mapping.Notifications(resp.GetItems(), view)
	if err != nil {
		return nil, err
	}
	freshness, err := mapping.Freshness(resp.GetFreshness())
	if err != nil {
		return nil, err
	}
	return gw.ListNotifications200JSONResponse{Body: gw.NotificationPage{
		Items:         items,
		NextPageToken: mapping.Optional(resp.GetNextPageToken()),
		Freshness:     freshness,
		Partial:       resp.GetPartial(),
	}}, nil
}

func (h *Handlers) GetNotification(ctx context.Context, request gw.GetNotificationRequestObject) (gw.GetNotificationResponseObject, error) {
	view, err := mapping.View(request.Params.View)
	if err != nil {
		return nil, err
	}
	ctx, cancel := h.unary(ctx)
	defer cancel()
	resp, err := h.Clients.Notification.GetNotification(ctx, &notificationv1.GetNotificationRequest{NotificationId: request.NotificationId, View: view})
	if err != nil {
		return nil, upstream(err)
	}
	item, err := mapping.Notification(resp.GetItem(), view)
	if err != nil {
		return nil, err
	}
	freshness, err := mapping.Freshness(resp.GetFreshness())
	if err != nil {
		return nil, err
	}
	noStore := "no-store"
	return gw.GetNotification200JSONResponse{
		Body:    gw.NotificationResult{Item: item, Freshness: freshness, Partial: resp.GetPartial()},
		Headers: gw.GetNotification200ResponseHeaders{CacheControl: &noStore},
	}, nil
}

// Origin and X-CSRF-Token are enforced by Envoy for cookie-authenticated mutations.
func (h *Handlers) RegisterPushToken(ctx context.Context, request gw.RegisterPushTokenRequestObject) (gw.RegisterPushTokenResponseObject, error) {
	ctx, cancel := h.unary(ctx)
	defer cancel()
	if _, err := h.Clients.Notification.RegisterPushToken(ctx, &notificationv1.RegisterPushTokenRequest{FcmToken: request.Body.FcmToken}); err != nil {
		return nil, upstream(err)
	}
	return gw.RegisterPushToken204Response{}, nil
}

func (h *Handlers) UnregisterPushToken(ctx context.Context, request gw.UnregisterPushTokenRequestObject) (gw.UnregisterPushTokenResponseObject, error) {
	ctx, cancel := h.unary(ctx)
	defer cancel()
	if _, err := h.Clients.Notification.UnregisterPushToken(ctx, &notificationv1.UnregisterPushTokenRequest{FcmToken: request.Body.FcmToken}); err != nil {
		return nil, upstream(err)
	}
	return gw.UnregisterPushToken204Response{}, nil
}
