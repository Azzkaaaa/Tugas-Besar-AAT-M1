package storage

import (
	"context"
	"errors"
	"time"
)

var ErrCheckpointConflict = errors.New("stream checkpoint changed; reload before continuing")

// StreamEntry is an immutable input, written BEFORE any canonical side effects.
// Sequence starts at 1; Cursor is the upstream position AFTER this input.
type StreamEntry struct {
	Sequence   int64     `json:"sequence"`
	Kind       string    `json:"kind"`
	Cursor     string    `json:"cursor"`
	Payload    []byte    `json:"payload"`
	ReceivedAt time.Time `json:"received_at"`
}

type StreamCheckpoint struct {
	Sequence int64  `json:"sequence"`
	Cursor   string `json:"cursor"`
}

type ParkedInput struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	Status     string    `json:"status"`
	Payload    []byte    `json:"payload,omitempty"` // base64: also preserves invalid UTF-8
}

// StreamStore belongs exclusively to Aggregator. Both canonical backends implement it.
// Append returns the ORIGINAL entry on duplicate sequence, even if the retry supplied
// different data. Commit uses compare-and-set; it follows all hazard/outbox writes.
// Cassandra partitions the journal into blocks of 1000 inputs.
type StreamStore interface {
	Checkpoint(context.Context, string) (StreamCheckpoint, error)
	Append(context.Context, string, StreamEntry) (StreamEntry, error)
	Read(context.Context, string, int64, int) ([]StreamEntry, error)
	Commit(context.Context, string, int64, StreamCheckpoint) error
	Park(context.Context, ParkedInput) error
	Parked(context.Context, string, string, int) ([]ParkedInput, error)
}

const StreamBlockSize int64 = 1000
