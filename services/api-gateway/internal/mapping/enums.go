// Package mapping converts between the generated OpenAPI models (gatewayapi) and the gRPC
// contract types. REST uses unprefixed enum labels; UNSPECIFIED is never emitted.
package mapping

import (
	"fmt"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	notificationv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/notification/v1"
	webhookv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/webhook/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
)

// enum is a bidirectional label table over the generated gatewayapi enum constants.
type enum[L ~string, P comparable] struct {
	name   string
	toWire map[L]P
	toREST map[P]L
}

func newEnum[L ~string, P comparable](name string, pairs map[L]P) enum[L, P] {
	e := enum[L, P]{name: name, toWire: pairs, toREST: make(map[P]L, len(pairs))}
	for label, value := range pairs {
		e.toREST[value] = label
	}
	return e
}

// parse maps an optional REST label; nil means unset (the proto zero value). The request
// validator already rejects labels outside the contract; this guards direct callers.
func (e enum[L, P]) parse(label *L) (P, error) {
	var zero P
	if label == nil {
		return zero, nil
	}
	value, ok := e.toWire[*label]
	if !ok {
		return zero, problem.BadRequest("%s has an unknown value", e.name)
	}
	return value, nil
}

// label maps a backend value; unknown or UNSPECIFIED values are an invalid upstream response.
func (e enum[L, P]) label(value P) (L, error) {
	label, ok := e.toREST[value]
	if !ok {
		return "", problem.InvalidUpstream(fmt.Errorf("%s %v has no REST label", e.name, value))
	}
	return label, nil
}

// optionalLabel is label, except that the zero value maps to nil (omitted).
func (e enum[L, P]) optionalLabel(value P) (*L, error) {
	var zero P
	if value == zero {
		return nil, nil
	}
	label, err := e.label(value)
	return &label, err
}

var (
	sources = newEnum("source", map[gw.Source]commonv1.Source{
		gw.BMKG:  commonv1.Source_SOURCE_BMKG,
		gw.PVMBG: commonv1.Source_SOURCE_PVMBG,
	})
	hazardTypes = newEnum("hazard_type", map[gw.HazardType]commonv1.HazardType{
		gw.SEISMIC:  commonv1.HazardType_HAZARD_TYPE_SEISMIC,
		gw.VOLCANIC: commonv1.HazardType_HAZARD_TYPE_VOLCANIC,
	})
	severities = newEnum("severity", map[gw.Severity]commonv1.Severity{
		gw.NORMAL:  commonv1.Severity_SEVERITY_NORMAL,
		gw.WASPADA: commonv1.Severity_SEVERITY_WASPADA,
		gw.SIAGA:   commonv1.Severity_SEVERITY_SIAGA,
		gw.AWAS:    commonv1.Severity_SEVERITY_AWAS,
	})
	views = newEnum("view", map[gw.View]commonv1.View{
		gw.ViewSummary: commonv1.View_VIEW_SUMMARY,
		gw.ViewFull:    commonv1.View_VIEW_FULL,
	})
	sourceStatuses = newEnum("freshness status", map[gw.FreshnessStatus]commonv1.SourceStatus{
		gw.FRESH:       commonv1.SourceStatus_SOURCE_STATUS_FRESH,
		gw.STALE:       commonv1.SourceStatus_SOURCE_STATUS_STALE,
		gw.UNAVAILABLE: commonv1.SourceStatus_SOURCE_STATUS_UNAVAILABLE,
	})
	pushStatuses = newEnum("push_status", map[gw.NotificationPushStatus]notificationv1.DangerNotification_PushStatus{
		gw.NotificationPushStatusPENDING:   notificationv1.DangerNotification_PUSH_STATUS_PENDING,
		gw.NotificationPushStatusDELIVERED: notificationv1.DangerNotification_PUSH_STATUS_DELIVERED,
		gw.NotificationPushStatusFAILED:    notificationv1.DangerNotification_PUSH_STATUS_FAILED,
	})
	deliveryStatuses = newEnum("delivery status", map[gw.WebhookDeliveryStatus]webhookv1.DeliveryStatus{
		gw.WebhookDeliveryStatusSUCCESS:  webhookv1.DeliveryStatus_DELIVERY_STATUS_SUCCESS,
		gw.WebhookDeliveryStatusFAILED:   webhookv1.DeliveryStatus_DELIVERY_STATUS_FAILED,
		gw.WebhookDeliveryStatusRETRYING: webhookv1.DeliveryStatus_DELIVERY_STATUS_RETRYING,
	})
)

// View resolves any of the per-operation view types; absent means SUMMARY.
func View[L ~string](label *L) (commonv1.View, error) {
	if label == nil {
		return commonv1.View_VIEW_SUMMARY, nil
	}
	view := gw.View(*label)
	return views.parse(&view)
}

func ViewLabel(view commonv1.View) (gw.WebhookSubscriptionView, error) {
	if view == commonv1.View_VIEW_UNSPECIFIED {
		return gw.WebhookSubscriptionViewSummary, nil
	}
	label, err := views.label(view)
	return gw.WebhookSubscriptionView(label), err
}
