package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

var ErrCriticalNotFound = errors.New("critical event not found")
var ErrCriticalReceiptConflict = errors.New("critical receipt does not match the immutable event")

const MaxCriticalPayloadBytes = 4 << 20

func CriticalEventBytes(e CriticalEvent) int { return len(e.Payload) + 6*len(e.SourceRefID) + 4096 }

// CriticalEvent is an immutable notification revision. Payload is the exact
// full-view JSON delivered to the designated Field Team receiver.
type CriticalEvent struct {
	EventID       string            `json:"event_id"`
	HazardID      string            `json:"hazard_id"`
	SourceRefID   string            `json:"source_ref_id"`
	Payload       json.RawMessage   `json:"payload"`
	PayloadSHA256 string            `json:"payload_sha256"`
	CreatedAt     time.Time         `json:"created_at"`
	TraceContext  map[string]string `json:"trace_context,omitempty"`
	Receipt       *CriticalReceipt  `json:"receipt,omitempty"`
	Generation    int64             `json:"generation"`
	Shard         int               `json:"shard"`
}

type CriticalReceipt struct {
	EventID       string    `json:"event_id"`
	HazardID      string    `json:"hazard_id"`
	ReceiverID    string    `json:"receiver_id"`
	PayloadSHA256 string    `json:"payload_sha256"`
	ReceivedAt    time.Time `json:"received_at"`
	Signature     string    `json:"signature"`
	RecordedAt    time.Time `json:"recorded_at,omitempty"`
}

type CriticalCursor struct {
	Shard int    `json:"s"`
	After string `json:"a"`
}

type CriticalPage struct {
	Events           []CriticalEvent `json:"events"`
	Next             *CriticalCursor `json:"next,omitempty"`
	UnavailableUnits []string        `json:"unavailable_units,omitempty"`
}

// CriticalJournal is independent of broker acknowledgements and consumer DBs.
// Pending rows disappear only when the receiver's verified receipt is recorded.
// List retains all revisions, including superseded critical observations.
type CriticalJournal interface {
	Pending(context.Context, CriticalCursor, int) (CriticalPage, error)
	List(context.Context, CriticalCursor, int) (CriticalPage, error)
	Get(context.Context, string, string) (*CriticalEvent, error)
	RecordReceipt(context.Context, CriticalReceipt) error
	Version(context.Context) ([]int64, error)
	ListSnapshot(context.Context, CriticalCursor, int, []int64) (CriticalPage, error)
}

func NewCriticalEvent(h *domain.HazardRecord, trace map[string]string) (*CriticalEvent, error) {
	critical, _ := h.Attributes["critical"].(bool)
	if h.Source != domain.SourceCoastal || !critical || domain.Withdrawn(h) {
		return nil, nil
	}
	id := domain.EventID(h.HazardID, h.Revision)
	body, err := json.Marshal(struct {
		EventID string               `json:"event_id"`
		Data    *domain.HazardRecord `json:"data"`
	}{id, h})
	if err != nil {
		return nil, err
	}
	if len(body) > MaxCriticalPayloadBytes {
		return nil, errors.New("critical notification exceeds the payload capacity")
	}
	hash := sha256.Sum256(body)
	return &CriticalEvent{EventID: id, HazardID: h.HazardID, SourceRefID: h.SourceRefID,
		Payload: body, PayloadSHA256: hex.EncodeToString(hash[:]), TraceContext: trace}, nil
}
