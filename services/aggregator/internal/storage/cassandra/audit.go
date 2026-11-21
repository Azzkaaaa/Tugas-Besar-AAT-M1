package cassandra

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/google/uuid"
)

const auditShards = 16
const auditReadyKey = "audit_indexes_v4"

type audit struct{ db *DB }
type auditEntry struct {
	at      time.Time
	id, ref string
	active  bool
}

func auditShard(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % auditShards)
}

// The revision is a logical timestamp in these new tables. A slow backfill or
// replay cannot overwrite a newer active/withdrawn index entry after clock skew.
func addAuditProjection(b *gocql.Batch, h *domain.HazardRecord) {
	for _, scope := range []string{scopeAll, h.Source} {
		version := int64(max(1, h.Revision))
		b.Query(`INSERT INTO analytics_by_shard (scope,shard,hazard_id,occurred_at,hazard_type,severity,area_name,tsunami,active,revision)
            VALUES (?,?,?,?,?,?,?,?,?,?) USING TIMESTAMP ?`, scope, auditShard(h.HazardID), h.HazardID, h.OccurredAt, h.HazardType, h.Severity, h.AreaName, storage.HasTsunami(h), !domain.Withdrawn(h), version, version)
		b.Query(`INSERT INTO audit_events_by_shard (scope,shard,ingested_at,hazard_id,source_ref_id,active,revision)
            VALUES (?,?,?,?,?,?,?) USING TIMESTAMP ?`, scope, auditShard(h.HazardID), h.IngestedAt, h.HazardID, h.SourceRefID, !domain.Withdrawn(h), version, version)
		b.Query(`INSERT INTO audit_refs_by_shard (scope,shard,source_ref_id,hazard_id,active,revision)
            VALUES (?,?,?,?,?,?) USING TIMESTAMP ?`, scope, auditShard(h.HazardID), h.SourceRefID, h.HazardID, !domain.Withdrawn(h), version, version)
	}
}

func (s audit) ready(ctx context.Context) ([]string, error) {
	var value string
	err := s.db.session.Query(`SELECT value FROM store_meta WHERE key=?`, auditReadyKey).WithContext(ctx).Scan(&value)
	if errors.Is(err, gocql.ErrNotFound) || (err == nil && value != "ready") {
		return []string{"audit-backfill"}, nil
	}
	if err != nil {
		return []string{"audit-index-state"}, err
	}
	return nil, nil
}

func (s audit) read(ctx context.Context, q storage.AuditQuery, shard int, after auditEntry, refs bool) ([]auditEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, readUnitTimeout)
	defer cancel()
	scope := q.Source
	if scope == "" {
		scope = scopeAll
	}
	stmt := `SELECT ingested_at,hazard_id,source_ref_id,active FROM audit_events_by_shard WHERE scope=? AND shard=?`
	args := []any{scope, shard}
	if refs {
		stmt = `SELECT source_ref_id,hazard_id,active FROM audit_refs_by_shard WHERE scope=? AND shard=?`
		if after.id != "" {
			stmt += ` AND (source_ref_id,hazard_id)>(?,?)`
			args = append(args, after.ref, after.id)
		}
	} else if after.id != "" && !after.at.Before(q.Since) {
		stmt += ` AND (ingested_at,hazard_id)>(?,?)`
		args = append(args, after.at, after.id)
	} else if !q.Since.IsZero() {
		stmt += ` AND ingested_at>=?`
		args = append(args, q.Since)
	}
	iter := s.db.session.Query(stmt+` LIMIT 64`, args...).WithContext(ctx).PageSize(64).Iter()
	out := []auditEntry{}
	var row auditEntry
	for {
		var ok bool
		if refs {
			ok = iter.Scan(&row.ref, &row.id, &row.active)
		} else {
			ok = iter.Scan(&row.at, &row.id, &row.ref, &row.active)
		}
		if !ok {
			break
		}
		row.at = row.at.UTC()
		out = append(out, row)
	}
	return out, iter.Close()
}

type auditStream struct {
	shard int
	rows  []auditEntry
	at    int
	done  bool
}
type auditHeap struct {
	streams []*auditStream
	refs    bool
}

func (h auditHeap) Len() int { return len(h.streams) }
func (h auditHeap) Less(i, j int) bool {
	a, b := h.streams[i].rows[h.streams[i].at], h.streams[j].rows[h.streams[j].at]
	if h.refs {
		if a.ref != b.ref {
			return a.ref < b.ref
		}
	} else if !a.at.Equal(b.at) {
		return a.at.Before(b.at)
	}
	return a.id < b.id
}
func (h auditHeap) Swap(i, j int) { h.streams[i], h.streams[j] = h.streams[j], h.streams[i] }
func (h *auditHeap) Push(v any)   { h.streams = append(h.streams, v.(*auditStream)) }
func (h *auditHeap) Pop() any {
	n := len(h.streams)
	v := h.streams[n-1]
	h.streams = h.streams[:n-1]
	return v
}

// Keep at most 64 small metadata rows per shard; fetch hazard payloads only when
// emitting a globally ordered result. Parallelism is bounded at four queries.
func mergeAudit(ctx context.Context, q storage.AuditQuery, refs bool,
	read func(context.Context, storage.AuditQuery, int, auditEntry, bool) ([]auditEntry, error),
	visit func(auditEntry) error,
) ([]string, error) {
	streams := make([]*auditStream, auditShards)
	errorsByShard := make([]error, auditShards)
	jobs := make(chan int, auditShards)
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for shard := range jobs {
				rows, err := read(ctx, q, shard, auditEntry{at: q.After.At, id: q.After.ID}, refs)
				streams[shard] = &auditStream{shard: shard, rows: rows, done: len(rows) < 64}
				errorsByShard[shard] = err
			}
		}()
	}
	for shard := range auditShards {
		jobs <- shard
	}
	close(jobs)
	workers.Wait()
	units := []string{}
	h := &auditHeap{refs: refs}
	for shard, stream := range streams {
		if errorsByShard[shard] != nil {
			units = append(units, fmt.Sprintf("audit-shard-%02d", shard))
			stream.done = true
		}
		if len(stream.rows) > 0 {
			heap.Push(h, stream)
		}
	}
	for h.Len() > 0 && ctx.Err() == nil {
		stream := heap.Pop(h).(*auditStream)
		row := stream.rows[stream.at]
		if row.active {
			if err := visit(row); err != nil {
				return units, err
			}
		}
		stream.at++
		if stream.at == len(stream.rows) && !stream.done {
			rows, err := read(ctx, q, stream.shard, row, refs)
			stream.rows, stream.at, stream.done = rows, 0, len(rows) < 64
			if err != nil {
				units = append(units, fmt.Sprintf("audit-shard-%02d", stream.shard))
				stream.done = true
			}
		}
		if stream.at < len(stream.rows) {
			heap.Push(h, stream)
		}
	}
	if ctx.Err() != nil {
		units = append(units, "audit-deadline")
		return units, ctx.Err()
	}
	return units, nil
}

var errAuditPageFull = errors.New("audit page complete")

func (s audit) Events(ctx context.Context, q storage.AuditQuery) (storage.AuditPage, error) {
	out := storage.AuditPage{}
	initial, _ := s.ready(ctx)
	out.UnavailableUnits = append(out.UnavailableUnits, initial...)
	bytes := 0
	units, err := mergeAudit(ctx, q, false, s.read, func(row auditEntry) error {
		if len(out.Items) >= q.Limit {
			out.More = true
			return errAuditPageFull
		}
		readCtx, cancel := context.WithTimeout(ctx, readUnitTimeout)
		h, e := (hazards{s.db}).Get(readCtx, row.id)
		cancel()
		if e != nil || h == nil {
			out.UnavailableUnits = append(out.UnavailableUnits, fmt.Sprintf("canonical-shard-%02d", auditShard(row.id)))
			return nil
		}
		if domain.Withdrawn(h) {
			return nil
		}
		encoded, _ := json.Marshal(h)
		if len(out.Items) > 0 && bytes+len(encoded) > storage.AuditPageBytes {
			out.More = true
			return errAuditPageFull
		}
		out.Items = append(out.Items, h)
		bytes += len(encoded)
		return nil
	})
	out.UnavailableUnits = uniqueUnits(append(out.UnavailableUnits, units...))
	if errors.Is(err, errAuditPageFull) {
		err = nil
	}
	return out, err
}

func (s audit) References(ctx context.Context, source string, visit func(string) error) ([]string, error) {
	initial, _ := s.ready(ctx)
	units, err := mergeAudit(ctx, storage.AuditQuery{Source: source}, true, s.read, func(row auditEntry) error { return visit(row.ref) })
	return uniqueUnits(append(initial, units...)), err
}

func uniqueUnits(units []string) []string {
	sort.Strings(units)
	out := []string{}
	for _, unit := range units {
		if len(out) == 0 || out[len(out)-1] != unit {
			out = append(out, unit)
		}
	}
	return out
}

// Backfill after an upgrade without holding startup or reads. New writes already
// maintain the indexes atomically with the normal projection batch. Logical
// revisions make concurrent/repeated backfill safe.
func (db *DB) maintainAudit(ctx context.Context) {
	holder := uuid.NewString()
	for ctx.Err() == nil {
		units, err := (audit{db}).ready(ctx)
		if err == nil && len(units) == 0 {
			return
		}
		if err == nil {
			var acquired bool
			acquired, err = (leases{db}).Acquire(ctx, "audit-index-backfill", holder, 30*time.Second)
			if err == nil && acquired {
				work, cancel := context.WithCancel(ctx)
				done := make(chan struct{})
				go func() {
					defer close(done)
					tick := time.NewTicker(5 * time.Second)
					defer tick.Stop()
					for {
						select {
						case <-work.Done():
							return
						case <-tick.C:
							renew, stop := context.WithTimeout(work, 2*time.Second)
							ok, e := (leases{db}).Renew(renew, "audit-index-backfill", holder, 30*time.Second)
							stop()
							if e != nil || !ok {
								cancel()
								return
							}
						}
					}
				}()
				err = db.backfillAudit(work)
				cancel()
				<-done
				release, stop := context.WithTimeout(context.Background(), time.Second)
				_ = (leases{db}).Release(release, "audit-index-backfill", holder)
				stop()
			} else if err == nil {
				err = errors.New("audit backfill is owned by another instance")
			}
		}
		if err == nil {
			return
		}
		timer := time.NewTimer(5 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (db *DB) backfillAudit(ctx context.Context) error {
	// Copy legacy canonical rows without blocking startup. A concurrent new
	// revision wins its conditional insert, and reads retain legacy fallback.
	legacy := db.session.Query(`SELECT hazard_id FROM hazards_by_id`).WithContext(ctx).PageSize(64).Iter()
	var id string
	for legacy.Scan(&id) {
		st, err := (hazards{db}).readStoredTable(ctx, id, true, true)
		if err != nil {
			_ = legacy.Close()
			return err
		}
		if st != nil {
			if err := (hazards{db}).adoptLegacy(ctx, st); err != nil {
				_ = legacy.Close()
				return err
			}
		}
	}
	if err := legacy.Close(); err != nil {
		return err
	}
	iter := db.session.Query(`SELECT ` + hazardColumns + ` FROM canonical_events`).WithContext(ctx).PageSize(1).Iter()
	scanner := iter.Scanner()
	for scanner.Next() {
		h, err := scanHazard(scanner.Scan)
		if err != nil {
			_ = scanner.Err()
			return err
		}
		if h.HazardID == "" || criticalInternalKey(h.HazardID) {
			continue
		}
		if err := db.ensureCritical(ctx, h); err != nil {
			_ = scanner.Err()
			return err
		}
		b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
		addAuditProjection(b, h)
		if err := b.Exec(); err != nil {
			_ = scanner.Err()
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return db.session.Query(`INSERT INTO store_meta (key,value) VALUES (?,?)`, auditReadyKey, "ready").WithContext(ctx).Exec()
}

var _ storage.AuditReader = audit{}
