package cassandra

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestOrderedListOptionsWorkers(t *testing.T) {
	for in, want := range map[int]int{-1: 4, 0: 4, 1: 1, 16: 16, 32: 32, 99: 32} {
		if got := (OrderedListOptions{Workers: in}).workers(); got != want {
			t.Fatalf("workers(%d)=%d want %d", in, got, want)
		}
	}
}

func TestOrderedWorkUsesRequestedConcurrency(t *testing.T) {
	var running, peak atomic.Int32
	err := orderedWork(context.Background(), 8, func(ctx context.Context, submit func(func(context.Context) error) error) error {
		for range 64 {
			if err := submit(func(context.Context) error {
				n := running.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				time.Sleep(5 * time.Millisecond)
				running.Add(-1)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if p := peak.Load(); p < 2 || p > 8 {
		t.Fatalf("peak concurrency %d, want 2..8", p)
	}
}

func TestOrderedListProgressReporting(t *testing.T) {
	var calls []int64
	o := OrderedListOptions{Progress: func(_ string, rows int64, _ bool) { calls = append(calls, rows) }}
	for i := int64(1); i <= 2500; i++ {
		o.report("headers", i, false)
	}
	o.report("headers", 2500, true)
	if len(calls) != 3 || calls[0] != 1000 || calls[1] != 2000 || calls[2] != 2500 {
		t.Fatalf("progress calls %v", calls)
	}
}
