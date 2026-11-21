package cassandra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const authorityShards = 16

// Odd versions permanently revoke an acquisition. A delayed activation from
// that same epoch cannot restore authority after voluntary release.
func guardVersion(a leader.Authority) int64 { return a.Epoch * 2 }

type guardPartition struct {
	table, column string
	key           any
}

func canonicalGuard(id string) guardPartition {
	return guardPartition{"canonical_events", "shard", auditShard(id)}
}
func inboxGuard(shard int) guardPartition { return guardPartition{"ingest_inbox", "shard", shard} }
func streamGuard(shard int) guardPartition {
	return guardPartition{"stream_inputs_by_shard", "shard", shard}
}
func sourceGuard(key domain.SourceKey) guardPartition {
	return guardPartition{"source_state", "source_key", string(key)}
}

func (db *DB) guardBatch(ctx context.Context, p guardPartition) *gocql.Batch {
	b := db.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	if a, ok := leader.AuthorityFromContext(ctx); ok {
		b.Query(`UPDATE `+p.table+` SET fence_guard=? WHERE `+p.column+`=?
			IF authority_tokens[?]=? AND authority_epochs[?]=? AND authority_until[?]>toTimestamp(now())`,
			a.Holder, p.key, a.Lease, a.Holder, a.Lease, guardVersion(a), a.Lease)
	}
	return b
}
func (db *DB) guardedCAS(ctx context.Context, p guardPartition, b *gocql.Batch) (bool, error) {
	ok, iter, err := b.MapExecCAS(map[string]any{})
	if iter != nil {
		if closeErr := iter.Close(); err == nil {
			err = closeErr
		}
	}
	if err == nil && !ok {
		if a, fenced := leader.AuthorityFromContext(ctx); fenced {
			if e := db.checkPartition(ctx, p, a); e != nil {
				return false, e
			}
		}
	}
	return ok, err
}
func (db *DB) checkPartition(ctx context.Context, p guardPartition, a leader.Authority) error {
	var epochs map[string]int64
	var tokens map[string]string
	var until map[string]time.Time
	var now time.Time
	err := db.session.Query(`SELECT authority_epochs,authority_tokens,authority_until,toTimestamp(now()) FROM `+p.table+` WHERE `+p.column+`=? LIMIT 1`, p.key).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&epochs, &tokens, &until, &now)
	if errors.Is(err, gocql.ErrNotFound) {
		logFencedRejection(ctx, p, a, "guard partition missing", 0, "", time.Time{}, time.Time{})
		return leader.ErrLostAuthority
	}
	if err != nil {
		return err
	}
	if epochs[a.Lease] != guardVersion(a) || tokens[a.Lease] != a.Holder || !until[a.Lease].After(now) {
		reason := "authority expired"
		if epochs[a.Lease] != guardVersion(a) || tokens[a.Lease] != a.Holder {
			reason = "superseded by a newer term"
		}
		logFencedRejection(ctx, p, a, reason, epochs[a.Lease], tokens[a.Lease], until[a.Lease], now)
		return leader.ErrLostAuthority
	}
	return nil
}

// logFencedRejection records a write refused by its fencing guard. Safety does not
// depend on this log; it makes stale-writer rejections (clock skew, freeze/resume,
// superseded terms) observable as evidence. storage_now is Cassandra's clock.
func logFencedRejection(ctx context.Context, p guardPartition, a leader.Authority, reason string, version int64, holder string, until, storageNow time.Time) {
	slog.WarnContext(ctx, "fenced write rejected", "component", "fencing", "reason", reason,
		"lease", a.Lease, "holder", a.Holder, "epoch", a.Epoch, "guard_version", guardVersion(a),
		"guard", p.table, "guard_key", fmt.Sprint(p.key),
		"current_guard_version", version, "current_holder", holder,
		"current_until", until, "storage_now", storageNow)
}

func (db *DB) installAuthority(ctx context.Context, p guardPartition, a leader.Authority, until time.Time) error {
	return db.installVersion(ctx, p, a, until, guardVersion(a))
}
func (db *DB) installVersion(ctx context.Context, p guardPartition, a leader.Authority, until time.Time, version int64) error {
	for range 8 {
		var epochs map[string]int64
		err := db.session.Query(`SELECT authority_epochs FROM `+p.table+` WHERE `+p.column+`=? LIMIT 1`, p.key).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&epochs)
		if err != nil && !errors.Is(err, gocql.ErrNotFound) {
			return err
		}
		var previous any
		if value, exists := epochs[a.Lease]; exists {
			if value > version {
				return leader.ErrLostAuthority
			}
			previous = value
		}
		ok, err := db.session.Query(`UPDATE `+p.table+` SET authority_epochs[?]=?,authority_tokens[?]=?,authority_until[?]=?
			WHERE `+p.column+`=? IF authority_epochs[?]=?`, a.Lease, version, a.Lease, a.Holder, a.Lease, until, p.key, a.Lease, previous).WithContext(ctx).MapScanCAS(map[string]any{})
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return fmt.Errorf("authority partition kept changing: %s/%v", p.table, p.key)
}

func sourceJob(name string) (domain.SourceKey, bool) {
	for suffix, key := range map[string]domain.SourceKey{"-bmkg-poller": domain.SourceKeyBMKGSeismic, "-pvmbg-poller": domain.SourceKeyPVMBGVolcanic, "-satellite-stream": domain.SourceKeySatellite, "-coastal-poller": domain.SourceKeyCoastal} {
		if strings.HasSuffix(name, suffix) {
			return key, true
		}
	}
	return "", false
}
func authorityPartitions(name string) []guardPartition {
	key, source := sourceJob(name)
	if !source {
		return nil
	}
	partitions := []guardPartition{sourceGuard(key)}
	for shard := range authorityShards {
		partitions = append(partitions, guardPartition{"canonical_events", "shard", shard})
	}
	for shard := range storage.IngestShardCount {
		partitions = append(partitions, inboxGuard(shard))
	}
	if key == domain.SourceKeySatellite || key == domain.SourceKeyCoastal {
		for shard := range authorityShards {
			partitions = append(partitions, streamGuard(shard))
		}
	}
	return partitions
}
func (l leases) Fence(ctx context.Context, name, holder string) (leader.Authority, error) {
	state, err := l.read(ctx, name)
	a := leader.Authority{Lease: name, Holder: holder}
	if err != nil {
		return a, err
	}
	if state.holderValue() != holder || !state.live() {
		return a, leader.ErrLostAuthority
	}
	a.Epoch = *state.epoch
	partitions := authorityPartitions(name)
	if len(partitions) == 0 {
		return a, nil
	}
	err = l.installAll(ctx, partitions, a, *state.until, guardVersion(a))
	if err != nil {
		return a, err
	}
	return a, l.ValidateAuthority(ctx, a)
}
func (l leases) installAll(ctx context.Context, partitions []guardPartition, a leader.Authority, until time.Time, version int64) error {
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan guardPartition, len(partitions))
	for _, p := range partitions {
		jobs <- p
	}
	close(jobs)
	var workers sync.WaitGroup
	var first error
	var once sync.Once
	// Each source installs up to 65 partition guards before it can run or
	// renew. Keep the work bounded, but allow enough concurrent network I/O
	// to finish the barrier within the lease window on separate hosts.
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for p := range jobs {
				if work.Err() != nil {
					return
				}
				if e := l.db.installVersion(work, p, a, until, version); e != nil {
					once.Do(func() { first = e; cancel() })
					return
				}
			}
		}()
	}
	workers.Wait()
	if first != nil {
		return first
	}
	if err := work.Err(); err != nil {
		return err
	}
	return nil
}
func (l leases) ValidateAuthority(ctx context.Context, a leader.Authority) error {
	state, err := l.read(ctx, a.Lease)
	if err != nil {
		return err
	}
	if state.holderValue() != a.Holder || state.epoch == nil || *state.epoch != a.Epoch || !state.live() {
		return leader.ErrLostAuthority
	}
	return nil
}

var _ leader.FencingStore = leases{}
