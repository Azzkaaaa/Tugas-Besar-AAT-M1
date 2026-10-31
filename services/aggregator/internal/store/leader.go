package store

import (
	"context"
	"log"
	"time"
)

const (
	leaderRetryInterval = 5 * time.Second
	leaderCheckInterval = 5 * time.Second
)

// RunAsIngestLeader runs fn only while this process holds the ingest leader lock, so exactly one
// replica polls the upstream sources. Two pollers would enqueue the same revision under different
// event IDs, which consumers cannot deduplicate.
//
// The lock is a session advisory lock on a dedicated connection: Postgres releases it when that
// session ends, so a crashed leader is replaced within leaderRetryInterval. The connection is
// pinged every leaderCheckInterval; when it fails, fn's context is cancelled. Another replica can
// take over before a partitioned leader notices, so a short overlap (up to leaderCheckInterval)
// is possible. fn must return once its context is done; it is restarted on re-election.
func (db *DB) RunAsIngestLeader(ctx context.Context, fn func(ctx context.Context)) {
	for ctx.Err() == nil {
		if db.leadOnce(ctx, fn) {
			continue
		}
		select {
		case <-ctx.Done():
		case <-time.After(leaderRetryInterval):
		}
	}
}

// leadOnce tries the lock once and, if it is taken, runs fn until leadership or ctx ends. It
// reports whether it led, so a lost leader retries at once.
func (db *DB) leadOnce(ctx context.Context, fn func(ctx context.Context)) bool {
	conn, err := db.Pool.Acquire(ctx)
	if err != nil {
		return false
	}
	var acquired bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", ingestLeaderKey).Scan(&acquired); err != nil || !acquired {
		conn.Release()
		return false
	}
	log.Println("[Leader] Acquired ingest leadership; starting pollers")

	leaderCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(leaderCtx)
	}()

	ticker := time.NewTicker(leaderCheckInterval)
	defer ticker.Stop()
	healthy := true
	for healthy {
		select {
		case <-ctx.Done():
			healthy = false
		case <-done:
			healthy = false
		case <-ticker.C:
			pingCtx, pingCancel := context.WithTimeout(ctx, leaderCheckInterval)
			if err := conn.Ping(pingCtx); err != nil {
				log.Printf("[Leader] Lost the leader lock connection (%v); stopping pollers", err)
				healthy = false
			}
			pingCancel()
		}
	}
	cancel()
	<-done

	unlockCtx, unlockCancel := context.WithTimeout(context.Background(), time.Second)
	defer unlockCancel()
	if _, err := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", ingestLeaderKey); err != nil {
		// Never hand a session that may still hold the lock back to the pool.
		_ = conn.Conn().Close(unlockCtx)
	}
	conn.Release()
	log.Println("[Leader] Released ingest leadership")
	return true
}
