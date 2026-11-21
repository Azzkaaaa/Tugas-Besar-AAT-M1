package cassandra

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/storage"
)

func TestAuditMergeUsesGlobalOrderAndRetainsHealthyShards(t *testing.T) {
	base := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	data := map[int][]auditEntry{}
	for shard := range auditShards {
		for n := range 2*auditReadPageSize + 2 {
			data[shard] = append(data[shard], auditEntry{at: base.Add(time.Duration(n) * time.Millisecond), id: fmt.Sprintf("%02d-%03d", shard, n), ref: fmt.Sprintf("ref-%03d", n), active: n%3 != 0})
		}
	}
	read := func(_ context.Context, _ storage.AuditQuery, shard int, after auditEntry, refs bool) ([]auditEntry, error) {
		if shard == 3 {
			return nil, errors.New("node lost")
		}
		out := []auditEntry{}
		for _, row := range data[shard] {
			if after.id != "" && (!row.at.After(after.at) && !(row.at.Equal(after.at) && row.id > after.id)) {
				continue
			}
			out = append(out, row)
			if len(out) == auditReadPageSize {
				break
			}
		}
		return out, nil
	}
	got := []auditEntry{}
	units, err := mergeAudit(context.Background(), storage.AuditQuery{}, false, read, func(row auditEntry) error { got = append(got, row); return nil })
	if err != nil || !reflect.DeepEqual(units, []string{"audit-shard-03"}) {
		t.Fatalf("%v %v", units, err)
	}
	want := []auditEntry{}
	for shard, rows := range data {
		if shard != 3 {
			for _, row := range rows {
				if row.active {
					want = append(want, row)
				}
			}
		}
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].at.Equal(want[j].at) {
			return want[i].id < want[j].id
		}
		return want[i].at.Before(want[j].at)
	})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merge lost or reordered entries: %d/%d", len(got), len(want))
	}
}

func TestNaturalReplicaPlacementUsesTokensRacksAndDistinctHosts(t *testing.T) {
	hosts := []ringHost{
		{id: "a", rack: "rack-a", dc: "dc1", tokens: []int64{-100, 100}},
		{id: "b", rack: "rack-a", dc: "dc1", tokens: []int64{-50}},
		{id: "c", rack: "rack-b", dc: "dc1", tokens: []int64{0}},
		{id: "d", rack: "rack-c", dc: "dc1", tokens: []int64{50}},
	}
	for _, tc := range []struct {
		token   int64
		options map[string]string
		want    []string
	}{
		{-100, map[string]string{"class": SimpleStrategy, "replication_factor": "3"}, []string{"a", "b", "c"}},
		{101, map[string]string{"class": SimpleStrategy, "replication_factor": "3"}, []string{"a", "b", "c"}},
		{-100, map[string]string{"class": NetworkTopologyStrategy, "dc1": "3"}, []string{"a", "c", "d"}},
		{-49, map[string]string{"class": NetworkTopologyStrategy, "dc1": "4"}, []string{"c", "d", "a", "b"}},
	} {
		out, n, err := naturalReplicas(tc.token, hosts, tc.options)
		ids := []string{}
		for _, h := range out {
			ids = append(ids, h.id)
		}
		if err != nil || n != len(tc.want) || !reflect.DeepEqual(ids, tc.want) {
			t.Fatalf("token %d: %v expected %v n=%d err=%v", tc.token, ids, tc.want, n, err)
		}
	}
}
