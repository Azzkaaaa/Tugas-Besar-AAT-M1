package broker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

const (
	pruneInterval  = time.Hour
	pruneBatchSize = 1000
)

// RunOutboxPruner deletes published and dead-lettered outbox records older than retention, every
// pruneInterval until ctx is done, in small batches so it never holds long locks. Run it on one
// replica (the ingest leader); concurrent pruners would only compete for the same rows.
func RunOutboxPruner(ctx context.Context, outbox storage.Outbox, retention time.Duration) {
	ticker := time.NewTicker(pruneInterval)
	defer ticker.Stop()
	for {
		var total int64
		for ctx.Err() == nil {
			deleted, err := outbox.Prune(ctx, retention, pruneBatchSize)
			if err != nil {
				if ctx.Err() == nil {
					slog.ErrorContext(ctx, "pruning outbox failed", "error", err)
				}
				break
			}
			total += deleted
			if deleted < pruneBatchSize {
				break
			}
		}
		if total > 0 {
			slog.InfoContext(ctx, "outbox pruned", "deleted", total, "retention", retention.String())
		}
		// Safe point between passes: as a leader job it may hand its lease over.
		if leader.MaybeYield(ctx) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
