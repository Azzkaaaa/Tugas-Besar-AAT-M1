package storage

import (
	"context"
	"hash/fnv"
	"io"
	"time"
)

const (
	IngestShardCount = 32
	IngestPending    = "pending"
	IngestRetrying   = "retrying"
	IngestParked     = "parked"
	IngestResolved   = "resolved"
)

func IngestShard(key string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % IngestShardCount)
}

type IngestMessage struct {
	ID            string    `json:"message_id"`
	Source        string    `json:"source"`
	Stream        string    `json:"-"`
	SourceRefID   string    `json:"-"`
	ReceivedAt    time.Time `json:"received_at"`
	SchemaVersion string    `json:"-"`
	CorrelationID string    `json:"-"`
	Sequence      int64     `json:"-"`
	Format        string    `json:"-"`
	State         string    `json:"status"`
	Reason        string    `json:"reason"`
	Attempts      int       `json:"attempts"`
	NextAttempt   time.Time `json:"-"`
	Raw           []byte    `json:"-"`
	Companion     []byte    `json:"-"`
	ArchiveID     string    `json:"-"`
	ArchiveChunks int       `json:"-"`
}

type IngestCursor struct {
	Shard int    `json:"s"`
	After string `json:"a"`
}

type IngestPage struct {
	Items            []IngestMessage
	Next             *IngestCursor
	UnavailableUnits []string
}

type IngestInbox interface {
	Put(context.Context, IngestMessage) error
	GetMessage(context.Context, string) (*IngestMessage, error)
	ListMessages(context.Context, string, IngestCursor, int) (IngestPage, error)
	ChangeState(context.Context, IngestMessage, string, string, int, time.Time) (bool, error)
	ArchiveChunk(context.Context, string, int, []byte) error
	WritePayload(context.Context, IngestMessage, io.Writer) error
}
