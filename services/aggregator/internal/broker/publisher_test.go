package broker

import (
	"testing"
	"time"
)

func TestOutboxBackoff_DoublesUpToTheCap(t *testing.T) {
	cases := map[int]time.Duration{
		0:  time.Second,
		1:  time.Second,
		2:  2 * time.Second,
		3:  4 * time.Second,
		6:  32 * time.Second,
		7:  time.Minute,
		40: time.Minute,
	}
	for attempt, want := range cases {
		if got := outboxBackoff(attempt); got != want {
			t.Errorf("outboxBackoff(%d) = %s, want %s", attempt, got, want)
		}
	}
}
