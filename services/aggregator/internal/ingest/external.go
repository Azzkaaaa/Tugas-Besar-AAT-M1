package ingest

import (
	"context"
	"crypto/sha256"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/google/uuid"
)

// ParkExternal shares P3's durable quarantine and protected raw download with
// adapters that own their own parsers/replay (satellite and coastal). There is no
// source reference or intake sequence: P3 must not try to normalize these bytes.
func ParkExternal(ctx context.Context, inbox storage.IngestInbox, source string, raw []byte, reason, correlation string, received time.Time) error {
	m := storage.IngestMessage{Source: source, Stream: "m2/" + source, Format: "m2-raw",
		ReceivedAt: received, State: storage.IngestParked, Reason: reason, Attempts: 1, CorrelationID: correlation}
	m.ID = messageID(m, raw)
	if len(raw) <= archiveChunkSize {
		m.Raw = raw
	} else {
		// The adapters already bound the body in memory. Chunk it directly so
		// read-only containers need no additional temporary filesystem here.
		prior, err := inbox.GetMessage(ctx, m.ID)
		if err != nil || prior != nil {
			return err
		}
		digest := sha256.Sum256([]byte(m.ID))
		m.ArchiveID = uuid.NewSHA1(uuid.NameSpaceOID, digest[:]).String()
		for start := 0; start < len(raw); start += archiveChunkSize {
			if err := inbox.ArchiveChunk(ctx, m.ArchiveID, m.ArchiveChunks, raw[start:min(start+archiveChunkSize, len(raw))]); err != nil {
				return err
			}
			m.ArchiveChunks++
		}
	}
	return (&Pipeline{inbox: inbox}).put(ctx, m)
}
