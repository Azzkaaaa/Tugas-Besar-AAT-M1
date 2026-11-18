package cassandra

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

type criticalJournal struct{ db *DB }

const criticalPrefix = "!critical:"
const pendingCriticalPrefix = "!pending:"
const criticalHistoryPrefix = "!history:"

func criticalInternalKey(id string) bool { return strings.HasPrefix(id, "!") }

func (db *DB) addCriticalEvent(ctx context.Context, b *gocql.Batch, h *domain.HazardRecord, trace map[string]string) error {
	e, err := storage.NewCriticalEvent(h, trace)
	if err != nil || e == nil {
		return err
	}
	previous, err := db.criticalGeneration(ctx, auditShard(h.HazardID))
	if err != nil {
		return err
	}
	next := int64(1)
	if previous != nil {
		next = *previous + 1
	}
	b.Query(`UPDATE canonical_events SET critical_generation=? WHERE shard=? IF critical_generation=?`, next, auditShard(h.HazardID), previous)
	encodedTrace, err := json.Marshal(e.TraceContext)
	if err != nil {
		return err
	}
	for _, prefix := range []string{criticalPrefix, pendingCriticalPrefix, criticalHistoryPrefix} {
		key := prefix + e.EventID
		if prefix == criticalHistoryPrefix {
			key = prefix + fmt.Sprintf("%020d", next)
		}
		b.Query(`INSERT INTO canonical_events (shard,hazard_id,critical_event_id,critical_hazard_id,critical_source_ref,critical_payload,critical_sha256,critical_trace,critical_created_at,critical_event_generation)
			VALUES (?,?,?,?,?,?,?,?,toTimestamp(now()),?) IF NOT EXISTS`, auditShard(h.HazardID), key, e.EventID, h.HazardID, h.SourceRefID, []byte(e.Payload), e.PayloadSHA256, string(encodedTrace), next)
	}
	return nil
}

// Upgrade adoption is conditional on the canonical revision. A late scan cannot
// invent a new revision, replace an immutable journal row or resurrect delivery
// after a receipt. Existing journal rows are always left alone.
func (db *DB) ensureCritical(ctx context.Context, h *domain.HazardRecord) error {
	e, err := storage.NewCriticalEvent(h, nil)
	if err != nil || e == nil {
		return err
	}
	_, err = (criticalJournal{db}).Get(ctx, h.HazardID, e.EventID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, storage.ErrCriticalNotFound) {
		return err
	}
	b := db.guardBatch(ctx, canonicalGuard(h.HazardID))
	b.Query(`UPDATE canonical_events SET critical_event_id=? WHERE shard=? AND hazard_id=? IF revision=?`, e.EventID, auditShard(h.HazardID), h.HazardID, h.Revision)
	if err := db.addCriticalEvent(ctx, b, h, nil); err != nil {
		return err
	}
	_, err = db.guardedCAS(ctx, canonicalGuard(h.HazardID), b)
	return err
}

const criticalColumns = `critical_event_id,critical_hazard_id,critical_source_ref,critical_payload,critical_sha256,critical_trace,critical_created_at,critical_receipt,critical_event_generation`

func scanCritical(scan func(...any) error) (storage.CriticalEvent, error) {
	var e storage.CriticalEvent
	var payload []byte
	var trace, receipt *string
	err := scan(&e.EventID, &e.HazardID, &e.SourceRefID, &payload, &e.PayloadSHA256, &trace, &e.CreatedAt, &receipt, &e.Generation)
	if err != nil {
		return e, err
	}
	e.Payload = append(json.RawMessage(nil), payload...)
	if trace != nil {
		if err := json.Unmarshal([]byte(*trace), &e.TraceContext); err != nil {
			return e, err
		}
	}
	if receipt != nil {
		if err := json.Unmarshal([]byte(*receipt), &e.Receipt); err != nil {
			return e, err
		}
	}
	e.CreatedAt = e.CreatedAt.UTC()
	return e, nil
}

func (s criticalJournal) Pending(ctx context.Context, cursor storage.CriticalCursor, limit int) (storage.CriticalPage, error) {
	return s.page(ctx, cursor, limit, pendingCriticalPrefix, nil)
}
func (s criticalJournal) List(ctx context.Context, cursor storage.CriticalCursor, limit int) (storage.CriticalPage, error) {
	return s.page(ctx, cursor, limit, criticalHistoryPrefix, nil)
}
func (s criticalJournal) ListSnapshot(ctx context.Context, cursor storage.CriticalCursor, limit int, versions []int64) (storage.CriticalPage, error) {
	if len(versions) != auditShards {
		return storage.CriticalPage{}, storage.ErrInvalidPageToken
	}
	return s.page(ctx, cursor, limit, criticalHistoryPrefix, versions)
}
func (s criticalJournal) page(ctx context.Context, cursor storage.CriticalCursor, limit int, prefix string, versions []int64) (page storage.CriticalPage, err error) {
	page = storage.CriticalPage{Events: []storage.CriticalEvent{}}
	defer func() {
		if len(page.UnavailableUnits) > 0 {
			page.Next = &cursor
		}
	}()
	payloadBytes := 0
	if cursor.Shard < 0 || cursor.Shard >= auditShards || limit < 1 || limit > 100 {
		return page, storage.ErrInvalidPageToken
	}
	for shard := cursor.Shard; shard < auditShards; shard++ {
		if versions != nil && versions[shard] == 0 {
			continue
		}
		after := prefix
		if shard == cursor.Shard && cursor.After != "" {
			after = prefix + cursor.After
		}
		work, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		query := `SELECT ` + criticalColumns + `,hazard_id FROM canonical_events WHERE shard=? AND hazard_id>? AND hazard_id<? LIMIT ?`
		upper := strings.TrimSuffix(prefix, ":") + ";"
		if versions != nil {
			upper = prefix + fmt.Sprintf("%020d", versions[shard]) + ";"
		}
		arguments := []any{shard, after, upper, limit - len(page.Events) + 1}
		iter := s.db.session.Query(query, arguments...).WithContext(work).PageSize(8).Iter()
		scanner := iter.Scanner()
		for scanner.Next() {
			var rowKey string
			e, err := scanCritical(func(dest ...any) error { return scanner.Scan(append(dest, &rowKey)...) })
			if err != nil {
				_ = scanner.Err()
				cancel()
				return page, err
			}
			e.Shard = shard
			if len(page.Events) == limit || (len(page.Events) > 0 && payloadBytes+storage.CriticalEventBytes(e) > 4<<20) {
				_ = scanner.Err()
				cancel()
				return page, nil
			}
			page.Events = append(page.Events, e)
			payloadBytes += storage.CriticalEventBytes(e)
			page.Next = &storage.CriticalCursor{Shard: shard, After: strings.TrimPrefix(rowKey, prefix)}
		}
		err := scanner.Err()
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return page, ctx.Err()
			}
			page.UnavailableUnits = append(page.UnavailableUnits, fmt.Sprintf("critical-%02d", shard))
		}
	}
	page.Next = nil
	if len(page.UnavailableUnits) > 0 {
		page.Next = &cursor
	}
	return page, nil
}

func (s criticalJournal) Get(ctx context.Context, hazardID, eventID string) (*storage.CriticalEvent, error) {
	e, err := scanCritical(s.db.session.Query(`SELECT `+criticalColumns+` FROM canonical_events WHERE shard=? AND hazard_id=?`, auditShard(hazardID), criticalPrefix+eventID).WithContext(ctx).Consistency(gocql.LocalSerial).Scan)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, storage.ErrCriticalNotFound
	}
	if err != nil {
		return nil, err
	}
	if e.HazardID != hazardID {
		return nil, storage.ErrCriticalNotFound
	}
	e.Shard = auditShard(hazardID)
	return &e, nil
}

func (s criticalJournal) RecordReceipt(ctx context.Context, receipt storage.CriticalReceipt) error {
	e, err := s.Get(ctx, receipt.HazardID, receipt.EventID)
	if err != nil {
		return err
	}
	if e.PayloadSHA256 != receipt.PayloadSHA256 {
		return storage.ErrCriticalReceiptConflict
	}
	if e.Receipt != nil {
		if e.Receipt.ReceiverID != receipt.ReceiverID {
			return storage.ErrCriticalReceiptConflict
		}
		return nil
	}
	// Store time is evidence of when acknowledgement became durable. The
	// separately signed receiver timestamp remains in the immutable receipt.
	var now time.Time
	if err := s.db.session.Query(`SELECT toTimestamp(now()) FROM system.local`).WithContext(ctx).Scan(&now); err != nil {
		return err
	}
	receipt.RecordedAt = now.UTC()
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	b := s.db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	b.Query(`UPDATE canonical_events SET critical_receipt=?,critical_received_at=? WHERE shard=? AND hazard_id=? IF critical_received_at=null AND critical_sha256=?`, string(encoded), receipt.RecordedAt, auditShard(receipt.HazardID), criticalPrefix+receipt.EventID, receipt.PayloadSHA256)
	b.Query(`DELETE FROM canonical_events WHERE shard=? AND hazard_id=?`, auditShard(receipt.HazardID), pendingCriticalPrefix+receipt.EventID)
	b.Query(`UPDATE canonical_events SET critical_receipt=?,critical_received_at=? WHERE shard=? AND hazard_id=?`, string(encoded), receipt.RecordedAt, auditShard(receipt.HazardID), criticalHistoryPrefix+fmt.Sprintf("%020d", e.Generation))
	ok, iter, err := b.MapExecCAS(map[string]any{})
	if iter != nil {
		if e := iter.Close(); err == nil {
			err = e
		}
	}
	if err != nil || ok {
		return err
	}
	current, err := s.Get(ctx, receipt.HazardID, receipt.EventID)
	if err == nil && current.Receipt != nil && current.Receipt.ReceiverID == receipt.ReceiverID {
		return nil
	}
	if err != nil {
		return err
	}
	return storage.ErrCriticalReceiptConflict
}

var _ storage.CriticalJournal = criticalJournal{}

func (db *DB) criticalGeneration(ctx context.Context, shard int) (*int64, error) {
	var generation *int64
	err := db.session.Query(`SELECT critical_generation FROM canonical_events WHERE shard=? LIMIT 1`, shard).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&generation)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	return generation, err
}
func (s criticalJournal) Version(ctx context.Context) ([]int64, error) {
	versions := make([]int64, auditShards)
	var workers sync.WaitGroup
	var first error
	var once sync.Once
	jobs := make(chan int, auditShards)
	for shard := range auditShards {
		jobs <- shard
	}
	close(jobs)
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for shard := range jobs {
				work, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
				generation, err := s.db.criticalGeneration(work, shard)
				cancel()
				if err != nil {
					once.Do(func() { first = err })
				} else if generation != nil {
					versions[shard] = *generation
				}
			}
		}()
	}
	workers.Wait()
	return versions, first
}
