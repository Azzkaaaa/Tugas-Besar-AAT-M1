package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadCassandra(t *testing.T) {
	t.Setenv("CANONICAL_STORE_CASSANDRA_HOSTS", "a:9042, b:9042")
	t.Setenv("CANONICAL_STORE_CASSANDRA_REPLICATION_STRATEGY", "NetworkTopologyStrategy")
	t.Setenv("CANONICAL_STORE_CASSANDRA_DATACENTERS", "dc1:3, dc2:2")
	t.Setenv("CANONICAL_STORE_CASSANDRA_LOCAL_DC", "dc1")
	t.Setenv("CANONICAL_STORE_CASSANDRA_BUCKET_PERIOD", "5m")
	c, err := LoadCassandra()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Hosts) != 2 || c.Hosts[1] != "b:9042" || c.ReplicationStrategy != "NetworkTopologyStrategy" ||
		c.Datacenters["dc1"] != 3 || c.Datacenters["dc2"] != 2 || c.LocalDatacenter != "dc1" ||
		c.BucketPeriod != 5*time.Minute || c.Consistency != "LOCAL_QUORUM" || c.Keyspace != "bnpb_canonical" {
		t.Fatalf("LoadCassandra = %+v", c)
	}
}

func TestLoadCassandraDefaultsAndErrors(t *testing.T) {
	t.Setenv("CANONICAL_STORE_CASSANDRA_HOSTS", "a")
	t.Setenv("CANONICAL_STORE_CASSANDRA_REPLICATION_FACTOR", "3")
	c, err := LoadCassandra()
	if err != nil || c.ReplicationStrategy != "SimpleStrategy" || c.ReplicationFactor != 3 || len(c.Datacenters) != 0 {
		t.Fatalf("LoadCassandra = %+v, %v", c, err)
	}

	t.Setenv("CANONICAL_STORE_CASSANDRA_REPLICATION_FACTOR", "zero")
	t.Setenv("CANONICAL_STORE_CASSANDRA_DATACENTERS", "dc1=3")
	if _, err := LoadCassandra(); err == nil || !strings.Contains(err.Error(), "REPLICATION_FACTOR") || !strings.Contains(err.Error(), "DATACENTERS") {
		t.Fatalf("bad values: %v", err)
	}
}

func TestParseDatacenters(t *testing.T) {
	if _, err := ParseDatacenters("dc1:3,dc1:2"); err == nil {
		t.Fatal("duplicate datacenter accepted")
	}
	got, err := ParseDatacenters("dc1:3,,dc-2:1")
	if err != nil || len(got) != 2 || got["dc-2"] != 1 {
		t.Fatalf("ParseDatacenters = %v, %v", got, err)
	}
}
