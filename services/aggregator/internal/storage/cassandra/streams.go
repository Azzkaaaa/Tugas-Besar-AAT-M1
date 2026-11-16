package cassandra

import (
	"context"
	"errors"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type streams struct{ db *DB }

func (s streams) Checkpoint(ctx context.Context, stream string) (storage.StreamCheckpoint, error) {
	var p storage.StreamCheckpoint
	err := s.db.session.Query(`SELECT sequence,cursor FROM m2_stream_checkpoints WHERE stream=?`, stream).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&p.Sequence, &p.Cursor)
	if errors.Is(err, gocql.ErrNotFound) {
		return p, nil
	}
	return p, err
}
func (s streams) Append(ctx context.Context, stream string, e storage.StreamEntry) (storage.StreamEntry, error) {
	block := (e.Sequence - 1) / storage.StreamBlockSize
	_, err := s.db.session.Query(`INSERT INTO m2_stream_journal (stream,block,sequence,kind,cursor,payload,received_at) VALUES (?,?,?,?,?,?,?) IF NOT EXISTS`, stream, block, e.Sequence, e.Kind, e.Cursor, e.Payload, e.ReceivedAt).WithContext(ctx).MapScanCAS(map[string]any{})
	if err != nil {
		return e, err
	}
	err = s.db.session.Query(`SELECT sequence,kind,cursor,payload,received_at FROM m2_stream_journal WHERE stream=? AND block=? AND sequence=?`, stream, block, e.Sequence).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&e.Sequence, &e.Kind, &e.Cursor, &e.Payload, &e.ReceivedAt)
	return e, err
}
func (s streams) Read(ctx context.Context, stream string, after int64, limit int) ([]storage.StreamEntry, error) {
	// Contiguous sequences let a reader discover the next partition without a global index.
	out := []storage.StreamEntry{}
	for len(out) < limit {
		iter := s.db.session.Query(`SELECT sequence,kind,cursor,payload,received_at FROM m2_stream_journal WHERE stream=? AND block=? AND sequence>? LIMIT ?`, stream, after/storage.StreamBlockSize, after, limit-len(out)).WithContext(ctx).Iter()
		var e storage.StreamEntry
		start := after
		for iter.Scan(&e.Sequence, &e.Kind, &e.Cursor, &e.Payload, &e.ReceivedAt) {
			copyEntry := e
			copyEntry.Payload = append([]byte(nil), e.Payload...)
			out = append(out, copyEntry)
			after = e.Sequence
		}
		if err := iter.Close(); err != nil {
			return nil, err
		}
		if after == start || after%storage.StreamBlockSize != 0 {
			break
		}
	}
	return out, nil
}
func (s streams) Commit(ctx context.Context, stream string, before int64, p storage.StreamCheckpoint) error {
	var ok bool
	var err error
	if before == 0 {
		ok, err = s.db.session.Query(`INSERT INTO m2_stream_checkpoints (stream,sequence,cursor) VALUES (?,?,?) IF NOT EXISTS`, stream, p.Sequence, p.Cursor).WithContext(ctx).MapScanCAS(map[string]any{})
	} else {
		ok, err = s.db.session.Query(`UPDATE m2_stream_checkpoints SET sequence=?,cursor=? WHERE stream=? IF sequence=?`, p.Sequence, p.Cursor, stream, before).WithContext(ctx).MapScanCAS(map[string]any{})
	}
	if err != nil {
		return err
	}
	if !ok {
		return storage.ErrCheckpointConflict
	}
	return nil
}
func (s streams) Park(ctx context.Context, p storage.ParkedInput) error {
	_, err := s.db.session.Query(`INSERT INTO m2_parked_inputs (source,message_id,received_at,reason,payload) VALUES (?,?,?,?,?) IF NOT EXISTS`, p.Source, p.MessageID, p.ReceivedAt, p.Reason, p.Payload).WithContext(ctx).MapScanCAS(map[string]any{})
	return err
}
func (s streams) Parked(ctx context.Context, source, after string, limit int) ([]storage.ParkedInput, error) {
	iter := s.db.session.Query(`SELECT message_id,received_at,reason,payload FROM m2_parked_inputs WHERE source=? AND message_id>? LIMIT ?`, source, after, limit).WithContext(ctx).Iter()
	out := []storage.ParkedInput{}
	p := storage.ParkedInput{Source: source, Status: "parked", Attempts: 1}
	for iter.Scan(&p.MessageID, &p.ReceivedAt, &p.Reason, &p.Payload) {
		v := p
		v.Payload = append([]byte(nil), p.Payload...)
		out = append(out, v)
	}
	return out, iter.Close()
}

var _ storage.StreamStore = streams{}
