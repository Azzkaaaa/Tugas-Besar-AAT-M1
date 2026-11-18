package cassandra

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// projectionShards is the number of List partitions per time bucket. It is part
// of the schema: changing it requires a new table and backfill.
const projectionShards = 8

func projectionShard(hazardID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte("projection:" + hazardID))
	return int(h.Sum32() % projectionShards)
}

// shardFailure names the shard partitions of one bucket that could not be read.
type shardFailure struct {
	shards []int
	cause  error
}

func (e *shardFailure) Error() string {
	return fmt.Sprintf("hazard bucket shards %v unavailable: %v", e.shards, e.cause)
}
func (e *shardFailure) Unwrap() error { return e.cause }

// bucketUnits returns the unavailable units of a failed bucket read.
func bucketUnits(bucket time.Time, err error) []string {
	unit := "hazard-bucket:" + bucket.UTC().Format(time.RFC3339Nano)
	var sf *shardFailure
	if !errors.As(err, &sf) {
		return []string{unit}
	}
	out := make([]string, 0, len(sf.shards))
	for _, n := range sf.shards {
		out = append(out, unit+"/shard-"+strconv.Itoa(n))
	}
	return out
}

// listOrderBefore reports whether a precedes b in List order.
func listOrderBefore(a, b *domain.HazardRecord) bool {
	if !a.OccurredAt.Equal(b.OccurredAt) {
		return a.OccurredAt.After(b.OccurredAt)
	}
	return a.HazardID < b.HazardID
}

type mergeHead struct {
	items [][]*domain.HazardRecord
	pos   []int
	order []int
}

func (m *mergeHead) Len() int { return len(m.order) }
func (m *mergeHead) Less(i, j int) bool {
	a, b := m.order[i], m.order[j]
	return listOrderBefore(m.items[a][m.pos[a]], m.items[b][m.pos[b]])
}
func (m *mergeHead) Swap(i, j int) { m.order[i], m.order[j] = m.order[j], m.order[i] }
func (m *mergeHead) Push(x any)    { m.order = append(m.order, x.(int)) }
func (m *mergeHead) Pop() any {
	x := m.order[len(m.order)-1]
	m.order = m.order[:len(m.order)-1]
	return x
}

// mergeShards merges per-shard results (each already in List order) and keeps
// the first want. The first want overall are within each shard's first want.
func mergeShards(parts [][]*domain.HazardRecord, want int) []*domain.HazardRecord {
	m := &mergeHead{items: parts, pos: make([]int, len(parts))}
	for i, p := range parts {
		if len(p) > 0 {
			m.order = append(m.order, i)
		}
	}
	heap.Init(m)
	var out []*domain.HazardRecord
	for m.Len() > 0 && len(out) < want {
		i := m.order[0]
		out = append(out, parts[i][m.pos[i]])
		m.pos[i]++
		if m.pos[i] == len(parts[i]) {
			heap.Pop(m)
		} else {
			heap.Fix(m, 0)
		}
	}
	return out
}

// readBucketShards reads every shard partition of bucket concurrently.
func readBucketShards(ctx context.Context, want int, read func(ctx context.Context, shard int) ([]*domain.HazardRecord, error)) ([]*domain.HazardRecord, error) {
	parts := make([][]*domain.HazardRecord, projectionShards)
	errs := make([]error, projectionShards)
	var wg sync.WaitGroup
	for n := 0; n < projectionShards; n++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			parts[n], errs[n] = read(ctx, n)
		}(n)
	}
	wg.Wait()
	var failed []int
	var cause error
	for n, err := range errs {
		if err != nil {
			failed = append(failed, n)
			cause = err
		}
	}
	out := mergeShards(parts, want)
	if failed != nil {
		return out, &shardFailure{shards: failed, cause: cause}
	}
	return out, nil
}

// addLegacyProjection writes the pre-0014 List row. Only migrations that run
// before the sharded table exists (0005 re-key) use it.
func addLegacyProjection(b *gocql.Batch, h *domain.HazardRecord, bucket time.Time) {
	b.Query(`INSERT INTO hazards_by_bucket (bucket, occurred_at, hazard_id, source, source_ref_id, hazard_type,
			severity, area_name, latitude, longitude, ingested_at, attributes, revision)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		bucket, h.OccurredAt, h.HazardID, h.Source, h.SourceRefID, h.HazardType,
		h.Severity, h.AreaName, h.Latitude, h.Longitude, h.IngestedAt, mustJSON(h.Attributes), h.Revision)
	b.Query(`INSERT INTO hazard_buckets (scope, bucket) VALUES (?, ?)`, scopeAll, bucket)
	b.Query(`INSERT INTO hazard_buckets (scope, bucket) VALUES (?, ?)`, h.Source, bucket)
}

// backfillShardedProjection (migration 15) copies every legacy List row into
// hazards_by_bucket_shard. Idempotent: a newer sharded row (higher revision)
// written after an interrupted run is never overwritten by an older copy.
func (db *DB) backfillShardedProjection(ctx context.Context) error {
	iter := db.session.Query(`SELECT bucket, occurred_at, hazard_id, source, source_ref_id, hazard_type, severity,
		area_name, latitude, longitude, ingested_at, attributes, revision FROM hazards_by_bucket`).WithContext(ctx).PageSize(500).Iter()
	var (
		bucket, occurred, ingested                        time.Time
		id, source, ref, kind, severity, area, attributes string
		lat, lon                                          float64
		revision                                          *int
	)
	copied := 0
	for iter.Scan(&bucket, &occurred, &id, &source, &ref, &kind, &severity, &area, &lat, &lon, &ingested, &attributes, &revision) {
		rev := 0
		if revision != nil {
			rev = *revision
		}
		var current *int
		err := db.session.Query(`SELECT revision FROM hazards_by_bucket_shard WHERE bucket=? AND shard=? AND occurred_at=? AND hazard_id=?`,
			bucket, projectionShard(id), occurred, id).WithContext(ctx).Scan(&current)
		if err != nil && !errors.Is(err, gocql.ErrNotFound) {
			_ = iter.Close()
			return err
		}
		if current != nil && *current >= rev {
			continue
		}
		if err = db.session.Query(`INSERT INTO hazards_by_bucket_shard (bucket, shard, occurred_at, hazard_id, source, source_ref_id,
			hazard_type, severity, area_name, latitude, longitude, ingested_at, attributes, revision)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, bucket, projectionShard(id), occurred, id, source, ref, kind, severity,
			area, lat, lon, ingested, attributes, rev).WithContext(ctx).Exec(); err != nil {
			_ = iter.Close()
			return err
		}
		copied++
	}
	if err := iter.Close(); err != nil {
		return err
	}
	if copied > 0 {
		slog.InfoContext(ctx, "copied List rows into sharded projection", "rows", copied, "shards", projectionShards)
	}
	return nil
}

// hasTable reports whether the keyspace already has table (schema-dependent migrations).
func (db *DB) hasTable(ctx context.Context, table string) (bool, error) {
	var name string
	err := db.session.Query(`SELECT table_name FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?`, db.keyspace, table).
		WithContext(ctx).Scan(&name)
	if errors.Is(err, gocql.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}
