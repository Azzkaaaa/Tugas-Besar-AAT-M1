package cassandra

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// snapshotChunk keeps every mutation far below Cassandra's mutation size limit.
const snapshotChunk = 256 << 10

type snapshots struct{ db *DB }

func (s snapshots) SaveSnapshot(ctx context.Context, stream string, sequence int64, state []byte) error {
	chunks := 0
	for off := 0; off < len(state) || chunks == 0; off += snapshotChunk {
		end := min(off+snapshotChunk, len(state))
		if err := s.db.session.Query(`INSERT INTO m2_stream_snapshot_chunks (stream,sequence,chunk,data) VALUES (?,?,?,?)`, stream, sequence, chunks, state[off:end]).
			WithContext(ctx).Consistency(gocql.LocalQuorum).Exec(); err != nil {
			return err
		}
		chunks++
	}
	sum := sha256.Sum256(state)
	if err := s.db.session.Query(`INSERT INTO m2_stream_snapshot_index (stream,sequence,chunks,bytes,sha256,created_at) VALUES (?,?,?,?,?,?)`,
		stream, sequence, chunks, int64(len(state)), hex.EncodeToString(sum[:]), time.Now().UTC()).WithContext(ctx).Consistency(gocql.LocalQuorum).Exec(); err != nil {
		return err
	}
	iter := s.db.session.Query(`SELECT sequence FROM m2_stream_snapshot_index WHERE stream=?`, stream).WithContext(ctx).Consistency(gocql.LocalQuorum).PageSize(64).Iter()
	var seq int64
	var stale []int64
	for n := 0; iter.Scan(&seq); n++ {
		if n >= storage.SnapshotsRetained {
			stale = append(stale, seq)
		}
	}
	if err := iter.Close(); err != nil {
		return err
	}
	for _, old := range stale {
		if err := s.db.session.Query(`DELETE FROM m2_stream_snapshot_index WHERE stream=? AND sequence=?`, stream, old).WithContext(ctx).Consistency(gocql.LocalQuorum).Exec(); err != nil {
			return err
		}
		if err := s.db.session.Query(`DELETE FROM m2_stream_snapshot_chunks WHERE stream=? AND sequence=?`, stream, old).WithContext(ctx).Consistency(gocql.LocalQuorum).Exec(); err != nil {
			return err
		}
	}
	return nil
}

func (s snapshots) LatestSnapshot(ctx context.Context, stream string, atMost int64) (int64, []byte, error) {
	type entry struct {
		seq    int64
		chunks int
		size   int64
		digest string
	}
	var candidates []entry
	iter := s.db.session.Query(`SELECT sequence,chunks,bytes,sha256 FROM m2_stream_snapshot_index WHERE stream=? AND sequence<=? LIMIT ?`, stream, atMost, storage.SnapshotsRetained).
		WithContext(ctx).Consistency(gocql.LocalQuorum).Iter()
	var e entry
	for iter.Scan(&e.seq, &e.chunks, &e.size, &e.digest) {
		candidates = append(candidates, e)
	}
	if err := iter.Close(); err != nil {
		return 0, nil, err
	}
	for _, c := range candidates {
		state := make([]byte, 0, c.size)
		rows := s.db.session.Query(`SELECT chunk,data FROM m2_stream_snapshot_chunks WHERE stream=? AND sequence=?`, stream, c.seq).WithContext(ctx).Consistency(gocql.LocalQuorum).PageSize(8).Iter()
		var chunk int
		var data []byte
		next := 0
		for rows.Scan(&chunk, &data) {
			if chunk != next {
				break
			}
			state = append(state, data...)
			next++
		}
		if err := rows.Close(); err != nil {
			return 0, nil, err
		}
		sum := sha256.Sum256(state)
		if next == c.chunks && int64(len(state)) == c.size && hex.EncodeToString(sum[:]) == c.digest {
			return c.seq, state, nil
		}
	}
	return 0, nil, nil
}

var _ storage.StreamSnapshots = snapshots{}
