package cassandra

import (
	"strings"
	"testing"
)

func TestReplicationNormalized(t *testing.T) {
	r, err := Replication{}.normalized()
	if err != nil || r.cql() != "{'class': 'SimpleStrategy', 'replication_factor': 1}" {
		t.Fatalf("default = %s, %v", r.cql(), err)
	}
	r, err = Replication{Strategy: "simple", Factor: 3}.normalized()
	if err != nil || r.cql() != "{'class': 'SimpleStrategy', 'replication_factor': 3}" {
		t.Fatalf("simple 3 = %s, %v", r.cql(), err)
	}
	r, err = Replication{Strategy: "NetworkTopologyStrategy", Factor: 3, Datacenters: map[string]int{"dc2": 2, "dc1": 3}}.normalized()
	if err != nil || r.cql() != "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 2}" {
		t.Fatalf("nts = %s, %v", r.cql(), err)
	}
	for name, bad := range map[string]Replication{
		"unknown strategy":    {Strategy: "LocalStrategy"},
		"negative factor":     {Factor: -1},
		"simple with dcs":     {Datacenters: map[string]int{"dc1": 3}},
		"nts without dcs":     {Strategy: NetworkTopologyStrategy},
		"nts zero replicas":   {Strategy: NetworkTopologyStrategy, Datacenters: map[string]int{"dc1": 0}},
		"nts injection in dc": {Strategy: NetworkTopologyStrategy, Datacenters: map[string]int{"dc1': 1}; DROP": 1}},
	} {
		if _, err := bad.normalized(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestReplicationMismatch(t *testing.T) {
	simple3, _ := Replication{Factor: 3}.normalized()
	same := map[string]string{"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "3"}
	if diff := replicationMismatch(same, simple3); diff != "" {
		t.Fatalf("identical replication reported: %s", diff)
	}
	rf1 := map[string]string{"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "1"}
	if diff := replicationMismatch(rf1, simple3); !strings.Contains(diff, "'replication_factor': 1") || !strings.Contains(diff, "'replication_factor': 3") {
		t.Fatalf("rf mismatch = %q", diff)
	}
	nts, _ := Replication{Strategy: NetworkTopologyStrategy, Datacenters: map[string]int{"dc1": 3}}.normalized()
	if diff := replicationMismatch(rf1, nts); diff == "" {
		t.Fatal("strategy mismatch not reported")
	}
	ntsActual := map[string]string{"class": "org.apache.cassandra.locator.NetworkTopologyStrategy", "dc1": "3"}
	if diff := replicationMismatch(ntsActual, nts); diff != "" {
		t.Fatalf("identical nts reported: %s", diff)
	}
	extraDC := map[string]string{"class": "org.apache.cassandra.locator.NetworkTopologyStrategy", "dc1": "3", "dc2": "3"}
	if diff := replicationMismatch(extraDC, nts); diff == "" {
		t.Fatal("extra datacenter not reported")
	}
}
