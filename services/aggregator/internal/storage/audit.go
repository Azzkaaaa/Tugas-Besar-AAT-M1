package storage

import (
	"context"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

// AuditPosition is a keyset boundary in ingestion order. It is independent of
// the public occurred_at cursor, and remains stable when a hazard is revised.
type AuditPosition struct {
	At time.Time `json:"at"`
	ID string    `json:"id"`
}

type AuditQuery struct {
	Source string
	Since  time.Time
	After  AuditPosition
	Limit  int
}

type AuditPage struct {
	Items            []*domain.HazardRecord
	More             bool
	UnavailableUnits []string
}

// AuditReader owns indexed queries over all canonical partitions. References
// are visited in byte order without materializing the dataset in memory.
type AuditReader interface {
	Events(context.Context, AuditQuery) (AuditPage, error)
	References(context.Context, string, func(string) error) ([]string, error)
}

// AuditPageBytes bounds retained response data even if the requested limit is
// 1000 and individual fire attributes are large. One oversized record is allowed
// alone, so its fields are never silently truncated.
const AuditPageBytes = 8 << 20

type HazardLocation struct {
	Node string `json:"node"`
	Unit string `json:"unit"`
	Role string `json:"role"`
}

type PlacementResult struct {
	Exists           bool
	Locations        []HazardLocation
	UnavailableUnits []string
}

type PlacementReader interface {
	Locate(context.Context, string) (PlacementResult, error)
}
