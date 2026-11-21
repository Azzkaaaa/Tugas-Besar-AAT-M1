package cassandra

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestTimeBudgetShortPage(t *testing.T) {
	expired, cancel := context.WithTimeout(context.Background(), 0)
	defer cancel()
	live := context.Background()
	wrapped := fmt.Errorf("read projection: %w", context.DeadlineExceeded)
	cases := []struct {
		name         string
		ctx          context.Context
		indexPartial bool
		consumed     int
		err          error
		want         bool
	}{
		{"deadline after progress", expired, false, 3, context.DeadlineExceeded, true},
		{"wrapped driver deadline", live, false, 2, wrapped, true},
		{"no error", expired, false, 3, nil, false},
		{"no progress must retry", expired, false, 0, context.DeadlineExceeded, false},
		{"incomplete index must retry", expired, true, 3, context.DeadlineExceeded, false},
		{"node failure stays partial", live, false, 3, errors.New("unavailable replicas"), false},
		{"cancellation is not a budget", live, false, 3, context.Canceled, false},
	}
	for _, c := range cases {
		if got := timeBudgetShortPage(c.ctx, c.indexPartial, c.consumed, c.err); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
