package broker

import (
	"context"
	"log"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/store"
)

const (
	pruneInterval  = time.Hour
	pruneBatchSize = 1000
)

// RunOutboxPruner deletes published and dead-lettered outbox records older than retention, every
// pruneInterval until ctx is done, in small batches so it never holds long locks. Run it on one
// replica (the ingest leader); concurrent pruners would only compete for the same rows.
func RunOutboxPruner(ctx context.Context, db *store.DB, retention time.Duration) {
	ticker := time.NewTicker(pruneInterval)
	defer ticker.Stop()
	for {
		var total int64
		for ctx.Err() == nil {
			deleted, err := db.PruneOutbox(ctx, retention, pruneBatchSize)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("[OutboxPruner] Failed pruning outbox: %v", err)
				}
				break
			}
			total += deleted
			if deleted < pruneBatchSize {
				break
			}
		}
		if total > 0 {
			log.Printf("[OutboxPruner] Deleted %d outbox records older than %s", total, retention)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
