package cassandra

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func TestProjectionShardsSpreadOneBucket(t *testing.T) {
	counts := make([]int, projectionShards)
	for i := 0; i < 8000; i++ {
		counts[projectionShard(domain.HazardID("BMKG", fmt.Sprint("burst-", i)))]++
	}
	for n, c := range counts {
		// A regional burst inside one bucket must not concentrate in one partition.
		if c < 800 || c > 1200 {
			t.Fatalf("shard %d holds %d of 8000 rows: %v", n, c, counts)
		}
	}
}

func TestMergeShardsKeepsGlobalListOrder(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	var all []*domain.HazardRecord
	parts := make([][]*domain.HazardRecord, projectionShards)
	for i := 0; i < 500; i++ {
		h := &domain.HazardRecord{HazardID: fmt.Sprintf("hz-%03d", r.Intn(1000)), OccurredAt: base.Add(time.Duration(r.Intn(40)) * time.Minute)}
		all = append(all, h)
		parts[projectionShard(h.HazardID)] = append(parts[projectionShard(h.HazardID)], h)
	}
	for _, p := range append(parts, all) {
		sort.SliceStable(p, func(i, j int) bool { return listOrderBefore(p[i], p[j]) })
	}
	for _, want := range []int{1, 17, 500, 900} {
		trimmed := make([][]*domain.HazardRecord, len(parts))
		for i, p := range parts {
			trimmed[i] = p[:min(len(p), want)]
		}
		got := mergeShards(trimmed, want)
		if len(got) != min(want, len(all)) {
			t.Fatalf("want %d: got %d", want, len(got))
		}
		for i := range got {
			if got[i] != all[i] {
				t.Fatalf("want %d: position %d out of order", want, i)
			}
		}
	}
}

func TestReadBucketShardsReportsEachFailedShard(t *testing.T) {
	bucket := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	items, err := readBucketShards(context.Background(), 10, func(_ context.Context, shard int) ([]*domain.HazardRecord, error) {
		if shard == 2 || shard == 5 {
			return nil, errors.New("timeout")
		}
		return []*domain.HazardRecord{{HazardID: fmt.Sprint("hz-", shard), OccurredAt: bucket}}, nil
	})
	if len(items) != projectionShards-2 {
		t.Fatalf("available shards discarded: %d", len(items))
	}
	units := bucketUnits(bucket, err)
	if len(units) != 2 || units[0] != "hazard-bucket:2026-03-01T00:00:00Z/shard-2" || units[1] != "hazard-bucket:2026-03-01T00:00:00Z/shard-5" {
		t.Fatalf("units: %v", units)
	}
}
