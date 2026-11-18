package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func journalShard(stream string, block int64) int {
	return auditShard(fmt.Sprintf("journal:%s:%d", stream, block))
}
func checkpointShard(stream string) int { return auditShard("checkpoint:" + stream) }

func (s streams) Checkpoint(ctx context.Context, stream string) (storage.StreamCheckpoint, error) {
	if !s.db.fencedSchema {
		return s.legacyCheckpoint(ctx, stream)
	}
	var p storage.StreamCheckpoint
	err := s.db.session.Query(`SELECT checkpoint_sequence,cursor FROM stream_inputs_by_shard WHERE shard=? AND family='checkpoint' AND stream=? AND block=0 AND sequence=0 AND message_id=''`, checkpointShard(stream), stream).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&p.Sequence, &p.Cursor)
	if errors.Is(err, gocql.ErrNotFound) {
		return s.legacyCheckpoint(ctx, stream)
	}
	return p, err
}
func (s streams) Append(ctx context.Context, stream string, e storage.StreamEntry) (storage.StreamEntry, error) {
	if !s.db.fencedSchema {
		return s.legacyAppend(ctx, stream, e)
	}
	block := (e.Sequence - 1) / storage.StreamBlockSize
	shard := journalShard(stream, block)
	var prior storage.StreamEntry
	err := s.db.session.Query(`SELECT sequence,kind,cursor,payload,received_at FROM m2_stream_journal WHERE stream=? AND block=? AND sequence=?`, stream, block, e.Sequence).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&prior.Sequence, &prior.Kind, &prior.Cursor, &prior.Payload, &prior.ReceivedAt)
	if err == nil {
		return prior, nil
	}
	if !errors.Is(err, gocql.ErrNotFound) {
		return e, err
	}
	b := s.db.guardBatch(ctx, streamGuard(shard))
	b.Query(`INSERT INTO stream_inputs_by_shard (shard,family,stream,block,sequence,message_id,kind,cursor,payload,received_at) VALUES (?,'journal',?,?,?,'',?,?,?,?) IF NOT EXISTS`, shard, stream, block, e.Sequence, e.Kind, e.Cursor, e.Payload, e.ReceivedAt)
	if _, err = s.db.guardedCAS(ctx, streamGuard(shard), b); err != nil {
		return e, err
	}
	err = s.db.session.Query(`SELECT sequence,kind,cursor,payload,received_at FROM stream_inputs_by_shard WHERE shard=? AND family='journal' AND stream=? AND block=? AND sequence=? AND message_id=''`, shard, stream, block, e.Sequence).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&e.Sequence, &e.Kind, &e.Cursor, &e.Payload, &e.ReceivedAt)
	return e, err
}
func (s streams) journalRows(ctx context.Context, stream string, block, after int64, limit int, legacy bool) ([]storage.StreamEntry, error) {
	stmt := `SELECT sequence,kind,cursor,payload,received_at FROM stream_inputs_by_shard WHERE shard=? AND family='journal' AND stream=? AND block=? AND sequence>? LIMIT ?`
	args := []any{journalShard(stream, block), stream, block, after, limit}
	if legacy {
		stmt = `SELECT sequence,kind,cursor,payload,received_at FROM m2_stream_journal WHERE stream=? AND block=? AND sequence>? LIMIT ?`
		args = []any{stream, block, after, limit}
	}
	iter := s.db.session.Query(stmt, args...).WithContext(ctx).Iter()
	rows := []storage.StreamEntry{}
	var e storage.StreamEntry
	for iter.Scan(&e.Sequence, &e.Kind, &e.Cursor, &e.Payload, &e.ReceivedAt) {
		v := e
		v.Payload = append([]byte(nil), e.Payload...)
		rows = append(rows, v)
	}
	return rows, iter.Close()
}
func (s streams) Read(ctx context.Context, stream string, after int64, limit int) ([]storage.StreamEntry, error) {
	if !s.db.fencedSchema {
		return s.legacyRead(ctx, stream, after, limit)
	}
	out := []storage.StreamEntry{}
	for len(out) < limit {
		block := after / storage.StreamBlockSize
		old, err := s.journalRows(ctx, stream, block, after, limit-len(out), true)
		if err != nil {
			return out, err
		}
		current, err := s.journalRows(ctx, stream, block, after, limit-len(out), false)
		if err != nil {
			return out, err
		}
		bySequence := map[int64]storage.StreamEntry{}
		for _, e := range old {
			bySequence[e.Sequence] = e
		}
		for _, e := range current {
			bySequence[e.Sequence] = e
		}
		rows := []storage.StreamEntry{}
		for _, e := range bySequence {
			rows = append(rows, e)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Sequence < rows[j].Sequence })
		if len(rows) == 0 {
			break
		}
		for _, e := range rows {
			if len(out) == limit {
				break
			}
			out = append(out, e)
			after = e.Sequence
		}
		if after%storage.StreamBlockSize != 0 {
			break
		}
	}
	return out, nil
}
func (s streams) Commit(ctx context.Context, stream string, before int64, p storage.StreamCheckpoint) error {
	if !s.db.fencedSchema {
		return s.legacyCommit(ctx, stream, before, p)
	}
	shard := checkpointShard(stream)
	var current int64
	err := s.db.session.Query(`SELECT checkpoint_sequence FROM stream_inputs_by_shard WHERE shard=? AND family='checkpoint' AND stream=? AND block=0 AND sequence=0 AND message_id=''`, shard, stream).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&current)
	if err != nil && !errors.Is(err, gocql.ErrNotFound) {
		return err
	}
	if errors.Is(err, gocql.ErrNotFound) {
		old, e := s.legacyCheckpoint(ctx, stream)
		if e != nil {
			return e
		}
		if old.Sequence != before {
			return storage.ErrCheckpointConflict
		}
		b := s.db.guardBatch(ctx, streamGuard(shard))
		b.Query(`INSERT INTO stream_inputs_by_shard (shard,family,stream,block,sequence,message_id,checkpoint_sequence,cursor) VALUES (?,'checkpoint',?,0,0,'',?,?) IF NOT EXISTS`, shard, stream, before, old.Cursor)
		if _, e = s.db.guardedCAS(ctx, streamGuard(shard), b); e != nil {
			return e
		}
	}
	b := s.db.guardBatch(ctx, streamGuard(shard))
	b.Query(`UPDATE stream_inputs_by_shard SET checkpoint_sequence=?,cursor=? WHERE shard=? AND family='checkpoint' AND stream=? AND block=0 AND sequence=0 AND message_id='' IF checkpoint_sequence=?`, p.Sequence, p.Cursor, shard, stream, before)
	ok, err := s.db.guardedCAS(ctx, streamGuard(shard), b)
	if err != nil {
		return err
	}
	if !ok {
		return storage.ErrCheckpointConflict
	}
	return nil
}
func (s streams) Park(ctx context.Context, p storage.ParkedInput) error {
	if !s.db.fencedSchema {
		return s.legacyPark(ctx, p)
	}
	shard := auditShard("parked:" + p.Source)
	b := s.db.guardBatch(ctx, streamGuard(shard))
	b.Query(`INSERT INTO stream_inputs_by_shard (shard,family,stream,block,sequence,message_id,received_at,reason,payload) VALUES (?,'parked',?,0,0,?,?,?,?) IF NOT EXISTS`, shard, p.Source, p.MessageID, p.ReceivedAt, p.Reason, p.Payload)
	_, err := s.db.guardedCAS(ctx, streamGuard(shard), b)
	return err
}
func (s streams) Parked(ctx context.Context, source, after string, limit int) ([]storage.ParkedInput, error) {
	if !s.db.fencedSchema {
		return s.legacyParked(ctx, source, after, limit)
	}
	old, err := s.legacyParked(ctx, source, after, limit)
	if err != nil {
		return nil, err
	}
	iter := s.db.session.Query(`SELECT message_id,received_at,reason,payload FROM stream_inputs_by_shard WHERE shard=? AND family='parked' AND stream=? AND block=0 AND sequence=0 AND message_id>? LIMIT ?`, auditShard("parked:"+source), source, after, limit).WithContext(ctx).Iter()
	byID := map[string]storage.ParkedInput{}
	for _, p := range old {
		byID[p.MessageID] = p
	}
	p := storage.ParkedInput{Source: source, Status: "parked", Attempts: 1}
	for iter.Scan(&p.MessageID, &p.ReceivedAt, &p.Reason, &p.Payload) {
		v := p
		v.Payload = append([]byte(nil), p.Payload...)
		byID[v.MessageID] = v
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	out := []storage.ParkedInput{}
	for _, p := range byID {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MessageID < out[j].MessageID })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
