package store

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	webhookv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/webhook/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store persists webhook subscriptions and the delivery log. Postgres is the deployed backend;
// SQLite remains for tests and single-replica local runs.
type Store interface {
	CreateSubscription(ctx context.Context, sub *Subscription) error
	GetSubscription(ctx context.Context, webhookID string) (*Subscription, error)
	ListSubscriptions(ctx context.Context, ownerClientID, pageToken string, pageSize int) ([]*Subscription, string, error)
	DeleteSubscription(ctx context.Context, webhookID string) error
	ListActiveSubscriptions(ctx context.Context) ([]*Subscription, error)
	InsertDelivery(ctx context.Context, d *Delivery) error
	ListDeliveries(ctx context.Context, webhookID, pageToken string, pageSize int) ([]*Delivery, string, error)
	Close() error
}

// Open picks the backend from the DSN: postgres:// or postgresql:// is Postgres, anything else
// is a SQLite DSN (a file: URI, a path or :memory:). Both apply their schema before returning.
func Open(ctx context.Context, dsn string) (Store, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		return OpenPostgres(ctx, dsn)
	}
	return OpenSQLite(ctx, dsn)
}

// Subscription mirrors the proto WebhookSubscription for internal use.
type Subscription struct {
	WebhookID      string
	OwnerClientID  string
	URL            string
	Secret         string
	FilterSource   int32
	FilterHazard   int32
	FilterSeverity int32
	FilterSince    *time.Time
	FilterUntil    *time.Time
	View           int32
	Description    string
	IsActive       bool
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Delivery mirrors the proto WebhookDelivery for internal use.
type Delivery struct {
	DeliveryID   string
	WebhookID    string
	EventID      string
	HazardID     string
	StatusCode   int32
	Status       string // "SUCCESS", "FAILED", "RETRYING"
	Attempt      int32
	DurationMS   int64
	ErrorMessage string
	DeliveredAt  time.Time
}

func clampPageSize(pageSize int) int {
	if pageSize <= 0 {
		return 20
	}
	if pageSize > 100 {
		return 100
	}
	return pageSize
}

// Subscription page token: base64(last webhook_id).
func encodeSubscriptionCursor(webhookID string) string {
	return base64.StdEncoding.EncodeToString([]byte(webhookID))
}

func decodeSubscriptionCursor(pageToken string) (string, error) {
	if pageToken == "" {
		return "", nil
	}
	b, err := base64.StdEncoding.DecodeString(pageToken)
	if err != nil {
		return "", fmt.Errorf("invalid page token")
	}
	return string(b), nil
}

// deliveryCursor is the last row of a delivery page; pages are ordered by (delivered_at, delivery_id) DESC.
type deliveryCursor struct {
	DeliveredAt time.Time
	DeliveryID  string
}

// Delivery page token: base64("<delivered_at RFC 3339 nano>|<delivery_id>").
func encodeDeliveryCursor(d *Delivery) string {
	return base64.StdEncoding.EncodeToString([]byte(d.DeliveredAt.UTC().Format(time.RFC3339Nano) + "|" + d.DeliveryID))
}

func decodeDeliveryCursor(pageToken string) (*deliveryCursor, error) {
	if pageToken == "" {
		return nil, nil
	}
	b, err := base64.StdEncoding.DecodeString(pageToken)
	if err != nil {
		return nil, fmt.Errorf("invalid page token")
	}
	at, id, ok := strings.Cut(string(b), "|")
	if !ok || id == "" {
		return nil, fmt.Errorf("invalid page token")
	}
	deliveredAt, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return nil, fmt.Errorf("invalid page token")
	}
	return &deliveryCursor{DeliveredAt: deliveredAt, DeliveryID: id}, nil
}

// ---------------------------------------------------------------------------
// Proto conversion helpers
// ---------------------------------------------------------------------------

// ToProto converts a Subscription to the proto WebhookSubscription (with secret redacted).
func (sub *Subscription) ToProto(redactSecret bool) *webhookv1.WebhookSubscription {
	secret := sub.Secret
	if redactSecret {
		secret = ""
	}
	p := &webhookv1.WebhookSubscription{
		WebhookId:     sub.WebhookID,
		OwnerClientId: sub.OwnerClientID,
		Url:           sub.URL,
		Secret:        secret,
		View:          commonv1.View(sub.View),
		Description:   sub.Description,
		IsActive:      sub.IsActive,
		CreatedAt:     timestamppb.New(sub.CreatedAt),
		UpdatedAt:     timestamppb.New(sub.UpdatedAt),
		Filter: &commonv1.HazardFilter{
			Source:     commonv1.Source(sub.FilterSource),
			HazardType: commonv1.HazardType(sub.FilterHazard),
			Severity:   commonv1.Severity(sub.FilterSeverity),
		},
	}
	if sub.FilterSince != nil {
		p.Filter.Since = timestamppb.New(*sub.FilterSince)
	}
	if sub.FilterUntil != nil {
		p.Filter.Until = timestamppb.New(*sub.FilterUntil)
	}
	return p
}

// ToProto converts a Delivery to the proto WebhookDelivery.
func (d *Delivery) ToProto() *webhookv1.WebhookDelivery {
	status := webhookv1.DeliveryStatus_DELIVERY_STATUS_UNSPECIFIED
	switch d.Status {
	case "SUCCESS":
		status = webhookv1.DeliveryStatus_DELIVERY_STATUS_SUCCESS
	case "FAILED":
		status = webhookv1.DeliveryStatus_DELIVERY_STATUS_FAILED
	case "RETRYING":
		status = webhookv1.DeliveryStatus_DELIVERY_STATUS_RETRYING
	}
	return &webhookv1.WebhookDelivery{
		DeliveryId:   d.DeliveryID,
		WebhookId:    d.WebhookID,
		EventId:      d.EventID,
		HazardId:     d.HazardID,
		StatusCode:   d.StatusCode,
		Status:       status,
		Attempt:      d.Attempt,
		DurationMs:   d.DurationMS,
		ErrorMessage: d.ErrorMessage,
		DeliveredAt:  timestamppb.New(d.DeliveredAt),
	}
}

type rowScanner interface {
	Scan(dest ...any) error
}
