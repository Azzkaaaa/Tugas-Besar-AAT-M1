// Package storage is the aggregator's view of its canonical store: the operations business logic
// needs, in domain terms. It imports no database driver. Backends live in sub-packages
// (storage/postgres, storage/cassandra) and must pass storage/storetest.
//
// The contract is deliberately narrow so that every backend can serve it natively: no
// transaction handle crosses the interface (compound writes are single calls), queries are a
// closed set with one fixed order, and page tokens are opaque.
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

// ErrInvalidPageToken is returned by List for a token it did not issue.
var ErrInvalidPageToken = errors.New("invalid page token")

// HazardStore holds canonical hazard records.
type HazardStore interface {
	// ApplyRevision upserts rev by (Source, SourceRefID). The record's HazardID is always
	// domain.HazardID(Source, SourceRefID) (rev.HazardID is ignored). An existing record keeps
	// its IngestedAt; a new one gets IngestedAt now (unless rev has one). Revision is 1 for a new
	// record and +1 per content change; the event ID derives from (HazardID, Revision).
	// Attributes are normalized (domain.NormalizeAttributes) before storing and comparing.
	//
	// When the record is new or its content changed, newEvent is called with the record as it
	// will be stored, and the event it returns is enqueued in the outbox atomically with the
	// write: either both are stored or neither. When nothing changed, nothing is written,
	// newEvent is not called and changed is false.
	ApplyRevision(ctx context.Context, rev *domain.HazardRecord,
		newEvent func(saved *domain.HazardRecord) (OutboxEvent, error),
	) (saved *domain.HazardRecord, changed bool, err error)

	// Get returns nil, nil when no record has hazardID.
	Get(ctx context.Context, hazardID string) (*domain.HazardRecord, error)

	// List returns records matching q, ordered by OccurredAt descending then HazardID ascending.
	// page.Size must be positive (callers clamp it). NextToken is empty on the last page.
	List(ctx context.Context, q HazardQuery, page PageRequest) (HazardPage, error)

	// HasData reports whether any record of source is stored.
	HasData(ctx context.Context, source string) (bool, error)
}

type SequencedHazardStore interface {
	ApplyIngestRevision(context.Context, *domain.HazardRecord, int64,
		func(*domain.HazardRecord) (OutboxEvent, error)) (*domain.HazardRecord, bool, error)
}

// HazardQuery is the closed set of filters List supports. Empty strings and zero times mean
// "any". Since is inclusive, Until exclusive.
type HazardQuery struct {
	Source     string
	HazardType string
	Severity   string
	Since      time.Time
	Until      time.Time
}

// PageRequest asks for up to Size records after Token (empty for the first page). Tokens are
// opaque and only valid for the backend that issued them and the same query.
type PageRequest struct {
	Size  int
	Token string
}

type HazardPage struct {
	Items     []*domain.HazardRecord
	NextToken string
}

// SourceState is what the store remembers about one ingest stream.
type SourceState struct {
	// Watermark is the newest OccurredAt ingested successfully (zero before the first success).
	Watermark     time.Time
	Status        string // domain.Status*; StatusUnavailable when the stream was never polled
	LastSuccessAt time.Time
	LastAttemptAt time.Time
	ErrorMessage  string
}

// SourceStateStore holds ingest progress per stream.
type SourceStateStore interface {
	// Get returns a zero state with Status StatusUnavailable for a stream never recorded.
	Get(ctx context.Context, key domain.SourceKey) (SourceState, error)
	// RecordSuccess advances the watermark and marks the stream fresh.
	RecordSuccess(ctx context.Context, key domain.SourceKey, watermark time.Time) error
	// RecordFailure marks the stream stale with cause; watermark and LastSuccessAt are kept.
	RecordFailure(ctx context.Context, key domain.SourceKey, cause string) error
}

// OutboxEvent is a message to publish, enqueued with the hazard write that produced it.
type OutboxEvent struct {
	EventID  string
	HazardID string
	Source   string
	Payload  []byte
	// TraceContext is the W3C carrier of the span that enqueued the event; the publisher
	// continues that trace. Nil when there was none.
	TraceContext map[string]string
}

// ClaimedEvent is an outbox event leased to a publisher.
type ClaimedEvent struct {
	OutboxEvent
	CreatedAt  time.Time
	RetryCount int
}

// Outbox delivers each event at least once.
//
// ClaimDue leases due events: a leased event is not returned again until the lease runs out
// (so a crashed publisher's events come back) or it is released. Backends that report
// Backend.OutboxSingleConsumer only guarantee this to one publisher at a time; others hand
// concurrent claimers disjoint sets.
type Outbox interface {
	// ClaimDue leases up to limit due events for lease, oldest first.
	ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]*ClaimedEvent, error)
	// Ack marks an event published.
	Ack(ctx context.Context, eventID string) error
	// RetryLater counts a failed attempt; the event is due again after backoff.
	RetryLater(ctx context.Context, eventID, lastErr string, backoff time.Duration) error
	// Release makes claimed but unattempted events due at once.
	Release(ctx context.Context, eventIDs []string) error
	// DeadLetter stops retrying ev and keeps it as a dead letter.
	DeadLetter(ctx context.Context, ev *ClaimedEvent, reason string) error
	// Prune deletes up to limit events published or dead-lettered more than retention ago and
	// returns how many it deleted. Backends that expire them on their own return 0.
	Prune(ctx context.Context, retention time.Duration, limit int) (int64, error)
}

// Backend is one opened store.
type Backend struct {
	Name    string
	Hazards HazardStore
	Sources SourceStateStore
	Outbox  Outbox
	Streams StreamStore
	Reports FieldReports
	Inbox   IngestInbox
	// OutboxSingleConsumer is true when at most one publisher may claim from Outbox at a time.
	OutboxSingleConsumer bool
	// Close releases the backend's connections.
	Close func()
}
