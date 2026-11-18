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
	t.Setenv("CANONICAL_STORE_CASSANDRA_NODE_FROM_RACK", "true")
	c, err := LoadCassandra()
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Hosts) != 2 || c.Hosts[1] != "b:9042" || c.ReplicationStrategy != "NetworkTopologyStrategy" ||
		c.Datacenters["dc1"] != 3 || c.Datacenters["dc2"] != 2 || c.LocalDatacenter != "dc1" ||
		c.BucketPeriod != 5*time.Minute || c.Consistency != "LOCAL_QUORUM" || c.Keyspace != "bnpb_canonical" || !c.NodeFromRack {
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

func setBaseEnv(t *testing.T) {
	for k, v := range map[string]string{
		"APP_ENV": "test", "LOG_LEVEL": "info", "AGGREGATOR_GRPC_PORT": "9091", "BMKG_BASE_URL": "x",
		"PVMBG_BASE_URL": "x", "BMKG_API_KEY": "k", "PVMBG_BEARER_TOKEN": "t", "AGGREGATOR_SERVICE_TOKEN": "s",
		"AUTH_INSECURE_SKIP_VERIFY": "true", "BMKG_POLL_INTERVAL_MS": "1000", "PVMBG_POLL_INTERVAL_MS": "1000",
		"BMKG_REQUEST_TIMEOUT_MS": "1000", "PVMBG_REQUEST_TIMEOUT_MS": "1000", "SOURCE_STALE_AFTER_MS": "1000",
		"AGGREGATOR_AMQP_URL": "amqp://x", "RABBITMQ_EXCHANGE": "e", "OUTBOX_MAX_RETRIES": "5",
		"CANONICAL_STORE_BACKEND": "cassandra", "CANONICAL_STORE_CASSANDRA_HOSTS": "h",
	} {
		t.Setenv(k, v)
	}
}

func TestLeaderConfig(t *testing.T) {
	setBaseEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Leader.Election != "cassandra" || cfg.Leader.TTL != 15*time.Second || cfg.Leader.Margin != 5*time.Second ||
		cfg.Leader.LeasePrefix != "aggregator" {
		t.Fatalf("leader defaults = %+v", cfg.Leader)
	}
	t.Setenv("LEADER_ELECTION", "Kubernetes")
	t.Setenv("LEADER_LEASE_TTL_MS", "30000")
	if cfg, err = Load(); err != nil || cfg.Leader.Election != "kubernetes" || cfg.Leader.TTL != 30*time.Second {
		t.Fatalf("leader overrides = %+v, %v", cfg.Leader, err)
	}
	t.Setenv("LEADER_ELECTION", "zookeeper")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "LEADER_ELECTION") {
		t.Fatalf("invalid election accepted: %v", err)
	}
}

func TestRequiredNotificationQueue(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("SATELLITE_BASE_URL", "")
	t.Setenv("PANTAI_BASE_URL", "")
	t.Setenv("AGGREGATOR_REQUIRE_WRITE_AUTHORITY", "true")
	t.Setenv("AGGREGATOR_REQUIRED_NOTIFICATION_QUEUE", "")
	cfg, err := Load()
	if err != nil || cfg.RequiredNotificationQueue != "bnpb.danger-notification.v1" {
		t.Fatal("M2 lacks required notification route", cfg, err)
	}
	t.Setenv("AGGREGATOR_REQUIRED_NOTIFICATION_QUEUE", " custom.notifications ")
	cfg, err = Load()
	if err != nil || cfg.RequiredNotificationQueue != "custom.notifications" {
		t.Fatal("configured consumer queue was not used", cfg, err)
	}
}

func TestYieldConfig(t *testing.T) {
	setBaseEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	l := cfg.Leader
	if l.YieldPerHour != 1 || l.YieldGrowth != 4 || l.YieldCooldown != l.RetryEvery || l.ReleaseCooldown != 2*l.TTL ||
		l.ReleaseMaxCooldown != 5*time.Minute || l.UnreachableAfter != 3 {
		t.Fatalf("yield defaults = %+v", l)
	}
	t.Setenv("LEADER_YIELD_PER_HOUR", "0")
	t.Setenv("LEADER_UNREACHABLE_AFTER", "5")
	if cfg, err = Load(); err != nil || cfg.Leader.YieldPerHour != 0 || cfg.Leader.UnreachableAfter != 5 {
		t.Fatalf("overrides = %+v, %v", cfg.Leader, err)
	}
	t.Setenv("LEADER_YIELD_GROWTH", "0.5")
	if _, err := Load(); err == nil {
		t.Fatal("growth below 1 accepted")
	}
}

func TestM2SourceConfigurationRequiresWriteAuthority(t *testing.T) {
	setBaseEnv(t)
	t.Setenv("AGGREGATOR_REQUIRE_WRITE_AUTHORITY", "false")
	t.Setenv("SATELLITE_BASE_URL", "")
	t.Setenv("PANTAI_BASE_URL", "")
	for _, key := range []string{"AGGREGATOR_REQUIRE_WRITE_AUTHORITY", "SATELLITE_BASE_URL", "PANTAI_BASE_URL"} {
		t.Run(key, func(t *testing.T) {
			value := "https://source.example"
			if key == "AGGREGATOR_REQUIRE_WRITE_AUTHORITY" {
				value = "true"
			}
			t.Setenv(key, value)
			cfg, err := Load()
			if err != nil || !cfg.RequireWriteAuthority {
				t.Fatal("M2 fencing not required", err)
			}
		})
	}
}
