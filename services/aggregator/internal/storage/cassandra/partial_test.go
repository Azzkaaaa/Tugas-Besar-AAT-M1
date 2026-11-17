package cassandra

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

type testBuckets struct {
	times      []time.Time
	at, closed int
	err        error
}

func (i *testBuckets) Scan(dst ...any) bool {
	if i.at == len(i.times) {
		return false
	}
	*dst[0].(*time.Time) = i.times[i.at]
	i.at++
	return true
}
func (i *testBuckets) Close() error { i.closed++; return i.err }

func TestPartialReadPreservesAvailableBucketsAndRetriesMissingInterval(t *testing.T) {
	base := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	for _, token := range []string{"", encodeCursor(&domain.HazardRecord{OccurredAt: base.Add(time.Hour), HazardID: "prior"})} {
		buckets := &testBuckets{times: []time.Time{base, base.Add(-time.Minute), base.Add(-2 * time.Minute)}}
		out := collectBuckets(context.Background(), buckets, "*", storage.PageRequest{Size: 2, Token: token},
			func(at time.Time, _ int) ([]*domain.HazardRecord, error) {
				if at.Equal(base.Add(-time.Minute)) {
					return nil, errors.New("lost bucket replicas")
				}
				return []*domain.HazardRecord{{HazardID: at.String(), OccurredAt: at}}, nil
			})
		if !out.Partial || len(out.Items) != 2 || len(out.UnavailableUnits) != 1 {
			t.Fatalf("discarded available data: %+v", out)
		}
		if !out.Items[0].OccurredAt.After(out.Items[1].OccurredAt) {
			t.Fatal("partial result order changed")
		}
		if token != "" && out.NextToken != token {
			t.Fatal("cursor advanced past unavailable interval")
		}
		cursor, err := decodeCursor(out.NextToken)
		if err != nil || (token == "" && cursor != nil) {
			t.Fatal("initial retry cursor did not restart the range")
		}
		if buckets.closed != 1 {
			t.Fatal("bucket iterator leaked")
		}
	}
}

func TestIndexFailureRetainsRowsAndMarksIncomplete(t *testing.T) {
	base := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	buckets := &testBuckets{times: []time.Time{base}, err: errors.New("index connection lost")}
	out := collectBuckets(context.Background(), buckets, "BMKG", storage.PageRequest{Size: 50},
		func(at time.Time, _ int) ([]*domain.HazardRecord, error) {
			return []*domain.HazardRecord{{HazardID: "available", OccurredAt: at}}, nil
		})
	if !out.Partial || len(out.Items) != 1 || out.UnavailableUnits[0] != "hazard-index:BMKG" {
		t.Fatalf("%+v", out)
	}
}
