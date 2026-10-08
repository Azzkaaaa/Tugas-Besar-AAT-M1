package mapping

import (
	"errors"
	"math"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	notificationv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/notification/v1"
	webhookv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/webhook/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
)

func Notification(n *notificationv1.DangerNotification, view commonv1.View) (gw.Notification, error) {
	if n == nil || n.GetNotificationId() == "" {
		return gw.Notification{}, problem.InvalidUpstream(errors.New("notification without notification_id"))
	}
	hazard, err := Hazard(n.GetHazard(), view)
	if err != nil {
		return gw.Notification{}, err
	}
	createdAt, err := RequiredTime("created_at", n.GetCreatedAt())
	if err != nil {
		return gw.Notification{}, err
	}
	pushStatus, err := pushStatuses.optionalLabel(n.GetPushStatus())
	if err != nil {
		return gw.Notification{}, err
	}
	return gw.Notification{
		NotificationId: n.GetNotificationId(),
		Hazard:         hazard,
		CreatedAt:      createdAt,
		Message:        n.GetMessage(),
		PushStatus:     pushStatus,
	}, nil
}

func Notifications(items []*notificationv1.DangerNotification, view commonv1.View) ([]gw.Notification, error) {
	out := make([]gw.Notification, 0, len(items))
	for _, item := range items {
		notification, err := Notification(item, view)
		if err != nil {
			return nil, err
		}
		out = append(out, notification)
	}
	return out, nil
}

// Webhook renders a subscription. The proto secret has no REST field and is never copied.
func Webhook(s *webhookv1.WebhookSubscription) (gw.WebhookSubscription, error) {
	if s == nil || s.GetWebhookId() == "" {
		return gw.WebhookSubscription{}, problem.InvalidUpstream(errors.New("webhook without webhook_id"))
	}
	filter, err := WebhookFilterLabels(s.GetFilter())
	if err != nil {
		return gw.WebhookSubscription{}, err
	}
	view, err := ViewLabel(s.GetView())
	if err != nil {
		return gw.WebhookSubscription{}, err
	}
	createdAt, err := RequiredTime("created_at", s.GetCreatedAt())
	if err != nil {
		return gw.WebhookSubscription{}, err
	}
	updatedAt, err := RequiredTime("updated_at", s.GetUpdatedAt())
	if err != nil {
		return gw.WebhookSubscription{}, err
	}
	return gw.WebhookSubscription{
		WebhookId:     s.GetWebhookId(),
		Url:           s.GetUrl(),
		Filter:        filter,
		View:          view,
		IsActive:      s.GetIsActive(),
		Description:   Optional(s.GetDescription()),
		OwnerClientId: s.GetOwnerClientId(),
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}, nil
}

func Webhooks(items []*webhookv1.WebhookSubscription) ([]gw.WebhookSubscription, error) {
	out := make([]gw.WebhookSubscription, 0, len(items))
	for _, item := range items {
		webhook, err := Webhook(item)
		if err != nil {
			return nil, err
		}
		out = append(out, webhook)
	}
	return out, nil
}

func Deliveries(items []*webhookv1.WebhookDelivery) ([]gw.WebhookDelivery, error) {
	out := make([]gw.WebhookDelivery, 0, len(items))
	for _, d := range items {
		status, err := deliveryStatuses.label(d.GetStatus())
		if err != nil {
			return nil, err
		}
		deliveredAt, err := RequiredTime("delivered_at", d.GetDeliveredAt())
		if err != nil {
			return nil, err
		}
		out = append(out, gw.WebhookDelivery{
			DeliveryId:   d.GetDeliveryId(),
			WebhookId:    d.GetWebhookId(),
			EventId:      d.GetEventId(),
			HazardId:     d.GetHazardId(),
			StatusCode:   int(d.GetStatusCode()),
			Status:       status,
			Attempt:      int(d.GetAttempt()),
			DurationMs:   Millis(d.GetDurationMs()),
			ErrorMessage: Optional(d.GetErrorMessage()),
			DeliveredAt:  deliveredAt,
		})
	}
	return out, nil
}

// Millis renders the proto int64 duration as the contract's non-negative integer.
func Millis(ms int64) int {
	return int(min(max(ms, 0), math.MaxInt32))
}
