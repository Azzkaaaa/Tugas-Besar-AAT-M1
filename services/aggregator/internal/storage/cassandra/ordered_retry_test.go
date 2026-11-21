package cassandra

import (
	"context"
	"errors"
	"testing"
)

func TestOrderedMaintenanceRetriesUncertainWriteAndStops(t *testing.T) {
	uncertain := errors.New("quorum reply lost")
	calls := 0
	if err := orderedRetry(context.Background(), func(context.Context) error {
		calls++
		if calls < 3 {
			return uncertain
		}
		return nil
	}); err != nil || calls != 3 {
		t.Fatalf("retry failed: calls=%d err=%v", calls, err)
	}
	calls = 0
	if err := orderedRetry(context.Background(), func(context.Context) error { calls++; return uncertain }); !errors.Is(err, uncertain) || calls != 4 {
		t.Fatalf("unbounded retry or missing error: calls=%d err=%v", calls, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls = 0
	err := orderedRetry(ctx, func(context.Context) error { calls++; cancel(); return context.Canceled })
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("retry ignored cancellation: calls=%d err=%v", calls, err)
	}
}
