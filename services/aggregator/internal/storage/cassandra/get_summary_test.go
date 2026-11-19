package cassandra

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSummaryFallbackSharedAndCancellationSafe(t *testing.T) {
	var db DB
	var active, peak atomic.Int32
	var wg sync.WaitGroup
	for range 64 {
		wg.Go(func() {
			release, err := db.acquireSummaryFallback(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			n := active.Add(1)
			if n > 1 {
				t.Error("concurrent fallback bodies", n)
			}
			peak.Store(max(peak.Load(), n))
			active.Add(-1)
			release()
		})
	}
	wg.Wait()
	if peak.Load() != 1 {
		t.Fatal("no fallback executed")
	}
	release, err := db.acquireSummaryFallback(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.acquireSummaryFallback(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled waiter admitted", err)
	}
	release()
	if _, err := db.acquireSummaryFallback(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled request acquired idle slot", err)
	}
	release, err = db.acquireSummaryFallback(context.Background())
	if err != nil {
		t.Fatal("cancelled waiter leaked slot", err)
	}
	release()
}
