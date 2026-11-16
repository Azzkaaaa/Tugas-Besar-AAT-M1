package postgres

import (
	"context"
	"errors"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	"github.com/jackc/pgx/v5"
)

type streams struct{ db *DB }

func (s streams) Checkpoint(ctx context.Context, stream string) (storage.StreamCheckpoint, error) {
	var p storage.StreamCheckpoint
	err := s.db.Pool.QueryRow(ctx, `SELECT sequence, cursor FROM m2_stream_checkpoints WHERE stream=$1`, stream).Scan(&p.Sequence, &p.Cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	return p, err
}

func (s streams) Append(ctx context.Context, stream string, e storage.StreamEntry) (storage.StreamEntry, error) {
	_, err := s.db.Pool.Exec(ctx, `INSERT INTO m2_stream_journal (stream,sequence,kind,cursor,payload,received_at)
		VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, stream, e.Sequence, e.Kind, e.Cursor, e.Payload, e.ReceivedAt)
	if err != nil {
		return e, err
	}
	err = s.db.Pool.QueryRow(ctx, `SELECT sequence,kind,cursor,payload,received_at FROM m2_stream_journal WHERE stream=$1 AND sequence=$2`, stream, e.Sequence).
		Scan(&e.Sequence, &e.Kind, &e.Cursor, &e.Payload, &e.ReceivedAt)
	return e, err
}

func (s streams) Read(ctx context.Context, stream string, after int64, limit int) ([]storage.StreamEntry, error) {
	rows, err := s.db.Pool.Query(ctx, `SELECT sequence,kind,cursor,payload,received_at FROM m2_stream_journal WHERE stream=$1 AND sequence>$2 ORDER BY sequence LIMIT $3`, stream, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []storage.StreamEntry
	for rows.Next() {
		var e storage.StreamEntry
		if err = rows.Scan(&e.Sequence, &e.Kind, &e.Cursor, &e.Payload, &e.ReceivedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s streams) Commit(ctx context.Context, stream string, before int64, p storage.StreamCheckpoint) error {
	var err error
	var changed int64
	if before == 0 {
		tag, e := s.db.Pool.Exec(ctx, `INSERT INTO m2_stream_checkpoints (stream,sequence,cursor) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, stream, p.Sequence, p.Cursor)
		err = e
		changed = tag.RowsAffected()
	} else {
		tag, e := s.db.Pool.Exec(ctx, `UPDATE m2_stream_checkpoints SET sequence=$1,cursor=$2 WHERE stream=$3 AND sequence=$4`, p.Sequence, p.Cursor, stream, before)
		err = e
		changed = tag.RowsAffected()
	}
	if err != nil {
		return err
	}
	if changed != 1 {
		return storage.ErrCheckpointConflict
	}
	return nil
}

func (s streams) Park(ctx context.Context, p storage.ParkedInput) error {
	_, err := s.db.Pool.Exec(ctx, `INSERT INTO m2_parked_inputs (source,message_id,received_at,reason,payload) VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, p.Source, p.MessageID, p.ReceivedAt, p.Reason, p.Payload)
	return err
}

func (s streams) Parked(ctx context.Context, source, after string, limit int) ([]storage.ParkedInput, error) {
	rows, err := s.db.Pool.Query(ctx, `SELECT message_id,source,received_at,reason,payload FROM m2_parked_inputs WHERE source=$1 AND message_id>$2 ORDER BY message_id LIMIT $3`, source, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []storage.ParkedInput{}
	for rows.Next() {
		p := storage.ParkedInput{Status: "parked", Attempts: 1}
		if err = rows.Scan(&p.MessageID, &p.Source, &p.ReceivedAt, &p.Reason, &p.Payload); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

var _ storage.StreamStore = streams{}
