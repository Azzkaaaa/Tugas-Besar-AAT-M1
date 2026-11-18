package cassandra

import (
	"testing"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/domain"
)

func TestAuthorityActivationCoversAllSourceMutationPartitions(t *testing.T) {
	for _, job := range []string{"bmkg-poller", "pvmbg-poller", "satellite-stream", "coastal-poller"} {
		name := "bnpb-" + job
		key, ok := sourceJob(name)
		if !ok {
			t.Fatal("source job not fenced", name)
		}
		seen := map[string]int{}
		for _, p := range authorityPartitions(name) {
			seen[p.table]++
		}
		if seen["canonical_events"] != 16 || seen["ingest_inbox"] != 32 || seen["source_state"] != 1 {
			t.Fatal("incomplete activation barrier", job, seen)
		}
		if (key == domain.SourceKeySatellite || key == domain.SourceKeyCoastal) && seen["stream_inputs_by_shard"] != 16 {
			t.Fatal("future journal blocks not covered", job)
		}
	}
}
