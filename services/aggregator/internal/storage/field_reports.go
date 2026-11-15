package storage

import (
	"context"
	"errors"
	"time"
)

var ErrReportConflict = errors.New("idempotency key already used for different report")

type FieldReport struct {
	ID            string    `json:"report_id"`
	Actor         string    `json:"submitted_by"`
	ObservedAt    time.Time `json:"observed_at"`
	AreaName      string    `json:"area_name"`
	Description   string    `json:"description"`
	ReceivedAt    time.Time `json:"received_at"`
	ProcessedAt   time.Time `json:"processed_at,omitzero"`
	CorrelationID string    `json:"correlation_id"`
}

type FieldReports interface {
	Accept(context.Context, FieldReport) error
	GetReport(context.Context, string) (*FieldReport, error)
	ProcessReports(context.Context) (int, error)
}
