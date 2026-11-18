package cassandra

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type ingestInbox struct{ db *DB }

const sequenceKey = "!sequence"

func messageShard(id string) int {
	if len(id) < 6 {
		return -1
	}
	shard, err := strconv.ParseUint(id[4:6], 16, 8)
	if err != nil || shard >= storage.IngestShardCount {
		return -1
	}
	return int(shard)
}

func (s ingestInbox) Put(ctx context.Context, m storage.IngestMessage) error {
	shard := messageShard(m.ID)
	if shard < 0 {
		return errors.New("invalid ingest message shard")
	}
	archive := gocql.UUID{}
	if m.ArchiveID != "" {
		var err error
		archive, err = gocql.ParseUUID(m.ArchiveID)
		if err != nil {
			return err
		}
	}
	const insert = `INSERT INTO ingest_inbox
		(shard, message_id, source, stream, source_ref_id, received_at, schema_version, correlation_id,
		 intake_sequence, format, state, reason, attempts, next_attempt, raw, companion, archive_id, archive_chunks)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`
	values := func(sequence int64) []any {
		return []any{shard, m.ID, m.Source, m.Stream, m.SourceRefID, m.ReceivedAt, m.SchemaVersion,
			m.CorrelationID, sequence, m.Format, m.State, m.Reason, m.Attempts, nullableTime(m.NextAttempt), m.Raw,
			m.Companion, archive, m.ArchiveChunks}
	}
	if m.SourceRefID == "" {
		b := s.db.guardBatch(ctx, inboxGuard(shard))
		b.Query(insert, values(0)...)
		_, err := s.db.guardedCAS(ctx, inboxGuard(shard), b)
		return err
	}

	// All revisions for a source reference share a shard. Reserving the sequence
	// and storing the item in one conditional batch leaves no invisible older slot.
	for range 8 {
		var existing string
		err := s.db.session.Query(`SELECT message_id FROM ingest_inbox WHERE shard = ? AND message_id = ?`, shard, m.ID).
			WithContext(ctx).Consistency(gocql.LocalQuorum).Scan(&existing)
		if !errors.Is(err, gocql.ErrNotFound) {
			return err
		}
		var prior int64
		err = s.db.session.Query(`SELECT intake_sequence FROM ingest_inbox WHERE shard = ? AND message_id = ?`, shard, sequenceKey).
			WithContext(ctx).Consistency(gocql.LocalQuorum).Scan(&prior)
		if errors.Is(err, gocql.ErrNotFound) {
			b := s.db.guardBatch(ctx, inboxGuard(shard))
			b.Query(`INSERT INTO ingest_inbox (shard,message_id,intake_sequence) VALUES (?,?,0) IF NOT EXISTS`, shard, sequenceKey)
			_, err = s.db.guardedCAS(ctx, inboxGuard(shard), b)
		}
		if err != nil {
			return err
		}
		batch := s.db.guardBatch(ctx, inboxGuard(shard)).Consistency(gocql.LocalQuorum)
		batch.Query(`UPDATE ingest_inbox SET intake_sequence = ? WHERE shard = ? AND message_id = ? IF intake_sequence = ?`,
			prior+1, shard, sequenceKey, prior)
		batch.Query(insert, values(prior+1)...)
		applied, err := s.db.guardedCAS(ctx, inboxGuard(shard), batch)
		if applied || err != nil {
			return err
		}
	}
	return errors.New("ingest sequence kept changing")
}

const inboxMetadata = `message_id, source, stream, source_ref_id, received_at, schema_version,
	correlation_id, intake_sequence, format, state, reason, attempts, next_attempt`

func messageColumns(m *storage.IngestMessage) []any {
	return []any{&m.ID, &m.Source, &m.Stream, &m.SourceRefID, &m.ReceivedAt, &m.SchemaVersion,
		&m.CorrelationID, &m.Sequence, &m.Format, &m.State, &m.Reason, &m.Attempts, &m.NextAttempt}
}

func (s ingestInbox) GetMessage(ctx context.Context, id string) (*storage.IngestMessage, error) {
	var m storage.IngestMessage
	var archive gocql.UUID
	columns := append(messageColumns(&m), &m.Raw, &m.Companion, &archive, &m.ArchiveChunks)
	err := s.db.session.Query(`SELECT `+inboxMetadata+`, raw, companion, archive_id, archive_chunks
		FROM ingest_inbox WHERE shard = ? AND message_id = ?`, messageShard(id), id).
		WithContext(ctx).Consistency(gocql.LocalQuorum).Scan(columns...)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	if m.ArchiveChunks > 0 {
		m.ArchiveID = archive.String()
	}
	return &m, err
}

func (s ingestInbox) ListMessages(ctx context.Context, source string, cursor storage.IngestCursor, limit int) (storage.IngestPage, error) {
	page := storage.IngestPage{Items: make([]storage.IngestMessage, 0, limit)}
	for shard := cursor.Shard; shard < problemShards; shard++ {
		start := sequenceKey
		if shard == cursor.Shard && cursor.After != "" {
			start = cursor.After
		}
		shardCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		iter := s.db.session.Query(`SELECT `+inboxMetadata+` FROM ingest_inbox WHERE shard = ? AND message_id > ?`, shard, start).
			WithContext(shardCtx).Consistency(gocql.LocalQuorum).PageSize(64).Iter()
		var m storage.IngestMessage
		for iter.Scan(messageColumns(&m)...) {
			if m.State == storage.IngestResolved || (source != "" && m.Source != source) {
				continue
			}
			if len(page.Items) == limit {
				_ = iter.Close()
				cancel()
				return page, nil
			}
			page.Items = append(page.Items, m)
			page.Next = &storage.IngestCursor{Shard: shard, After: m.ID}
		}
		err := iter.Close()
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return page, ctx.Err()
			}
			page.UnavailableUnits = append(page.UnavailableUnits, fmt.Sprintf("ingest-inbox-%02d", shard))
		}
	}
	page.Next = nil
	return page, nil
}

func (s ingestInbox) ChangeState(ctx context.Context, m storage.IngestMessage, state, reason string, attempts int, next time.Time) (bool, error) {
	b := s.db.guardBatch(ctx, inboxGuard(messageShard(m.ID)))
	b.Query(`UPDATE ingest_inbox SET state = ?, reason = ?, attempts = ?, next_attempt = ?
		WHERE shard = ? AND message_id = ? IF state = ? AND attempts = ?`, state, reason, attempts,
		nullableTime(next), messageShard(m.ID), m.ID, m.State, m.Attempts)
	return s.db.guardedCAS(ctx, inboxGuard(messageShard(m.ID)), b)
}

func (s ingestInbox) ArchiveChunk(ctx context.Context, id string, chunk int, raw []byte) error {
	if err := leader.CheckAuthority(ctx); err != nil {
		return err
	}
	archiveID, err := gocql.ParseUUID(id)
	if err != nil {
		return err
	}
	return s.db.session.Query(`INSERT INTO ingest_archive_chunks (archive_id, chunk, raw) VALUES (?, ?, ?)`, archiveID, chunk, raw).
		WithContext(ctx).Consistency(gocql.LocalQuorum).Exec()
}

func (s ingestInbox) WritePayload(ctx context.Context, m storage.IngestMessage, out io.Writer) error {
	if m.ArchiveChunks == 0 {
		_, err := out.Write(m.Raw)
		return err
	}
	id, err := gocql.ParseUUID(m.ArchiveID)
	if err != nil {
		return err
	}
	for chunk := range m.ArchiveChunks {
		var raw []byte
		if err := s.db.session.Query(`SELECT raw FROM ingest_archive_chunks WHERE archive_id = ? AND chunk = ?`, id, chunk).
			WithContext(ctx).Consistency(gocql.LocalQuorum).Scan(&raw); err != nil {
			return err
		}
		if _, err := out.Write(raw); err != nil {
			return err
		}
	}
	return nil
}
