package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppEnv                 string
	LogLevel               string
	GRPCPort               string
	BMKGBaseURL            string
	PVMBGBaseURL           string
	BMKGAPIKey             string
	PVMBGBearerToken       string
	ServiceToken           string
	AuthAuthority          string
	AuthAudience           string
	AuthInsecureSkipVerify bool
	BMKGPollInterval       time.Duration
	PVMBGPollInterval      time.Duration
	BMKGRequestTimeout     time.Duration
	PVMBGRequestTimeout    time.Duration
	SourceStaleThreshold   time.Duration
	// CanonicalStoreBackend is "postgres" or "cassandra".
	CanonicalStoreBackend string
	CanonicalStoreDSN     string
	Cassandra             CassandraConfig
	Leader                LeaderConfig
	AMQPURL               string
	RabbitMQExchange      string
	// OutboxMaxRetries is the max publish attempts before a record is dead-lettered.
	OutboxMaxRetries int
	// OutboxRetention is how long published and dead-lettered records stay in outbox_events.
	OutboxRetention time.Duration
}

// LeaderConfig configures leader election (see internal/leader).
type LeaderConfig struct {
	// Election is postgres, cassandra, kubernetes or single (LEADER_ELECTION; default: the
	// canonical store backend). The postgres and cassandra electors need that store backend.
	Election   string
	TTL        time.Duration
	RenewEvery time.Duration
	RetryEvery time.Duration
	Margin     time.Duration
	// Identity names this replica (LEADER_IDENTITY, else POD_NAME, else hostname-pid-random).
	Identity string
	// LeasePrefix names the leases: one per leader job, "<prefix>-<job>" (LEADER_LEASE_PREFIX).
	LeasePrefix string
	// K8sNamespace is the Kubernetes Lease namespace (default: the service account's).
	K8sNamespace string
	// Cooperative release (see internal/leader/term.go). YieldPerHour 0 disables yielding.
	YieldPerHour       float64
	YieldGrowth        float64
	YieldCooldown      time.Duration
	ReleaseCooldown    time.Duration
	ReleaseMaxCooldown time.Duration
	// UnreachableAfter is how many polls in a row must fail to reach their upstream before the
	// poller releases its lease (0 disables).
	UnreachableAfter int
}

// CassandraConfig configures the cassandra canonical store backend. LoadCassandra reads it from
// the CANONICAL_STORE_CASSANDRA_* environment; the aggregator, storecopy and storeadmin share it.
type CassandraConfig struct {
	Hosts       []string
	Keyspace    string
	Username    string
	Password    string
	Consistency string
	// LocalDatacenter routes queries to that datacenter first (LOCAL_* consistency).
	LocalDatacenter string
	// Replication of a keyspace the store creates: ReplicationStrategy is SimpleStrategy (with
	// ReplicationFactor) or NetworkTopologyStrategy (with Datacenters). Existing keyspaces keep
	// their replication; a difference is logged.
	ReplicationStrategy string
	ReplicationFactor   int
	Datacenters         map[string]int
	// BucketPeriod is the initial time span of one hazard partition, used when the keyspace is
	// new (validated by the cassandra backend). Later changes go through cmd/storeadmin.
	BucketPeriod time.Duration
}

// LoadCassandra reads the Cassandra settings from the environment (after .env). Values are only
// parsed here; the cassandra backend validates them when it opens the store.
func LoadCassandra() (CassandraConfig, error) {
	loadDotEnv()
	var errs []string
	c := CassandraConfig{
		Hosts:               splitList(os.Getenv("CANONICAL_STORE_CASSANDRA_HOSTS")),
		Keyspace:            envOr("CANONICAL_STORE_CASSANDRA_KEYSPACE", "bnpb_canonical"),
		Username:            os.Getenv("CANONICAL_STORE_CASSANDRA_USERNAME"),
		Password:            os.Getenv("CANONICAL_STORE_CASSANDRA_PASSWORD"),
		Consistency:         envOr("CANONICAL_STORE_CASSANDRA_CONSISTENCY", "LOCAL_QUORUM"),
		LocalDatacenter:     strings.TrimSpace(os.Getenv("CANONICAL_STORE_CASSANDRA_LOCAL_DC")),
		ReplicationStrategy: envOr("CANONICAL_STORE_CASSANDRA_REPLICATION_STRATEGY", "SimpleStrategy"),
		ReplicationFactor:   1,
	}
	if len(c.Hosts) == 0 {
		errs = append(errs, "missing required env var: CANONICAL_STORE_CASSANDRA_HOSTS")
	}
	if v := strings.TrimSpace(os.Getenv("CANONICAL_STORE_CASSANDRA_REPLICATION_FACTOR")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, fmt.Sprintf("invalid positive integer for env var CANONICAL_STORE_CASSANDRA_REPLICATION_FACTOR: %q", v))
		}
		c.ReplicationFactor = n
	}
	if v := strings.TrimSpace(os.Getenv("CANONICAL_STORE_CASSANDRA_DATACENTERS")); v != "" {
		dcs, err := ParseDatacenters(v)
		if err != nil {
			errs = append(errs, "CANONICAL_STORE_CASSANDRA_DATACENTERS: "+err.Error())
		}
		c.Datacenters = dcs
	}
	period, err := time.ParseDuration(envOr("CANONICAL_STORE_CASSANDRA_BUCKET_PERIOD", "1m"))
	if err != nil {
		errs = append(errs, fmt.Sprintf("invalid duration for CANONICAL_STORE_CASSANDRA_BUCKET_PERIOD: %v", err))
	}
	c.BucketPeriod = period
	if len(errs) > 0 {
		return c, errors.New(strings.Join(errs, "\n  - "))
	}
	return c, nil
}

// ParseDatacenters parses "dc1:3,dc2:2" (CANONICAL_STORE_CASSANDRA_DATACENTERS).
func ParseDatacenters(s string) (map[string]int, error) {
	out := map[string]int{}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		dc, n, ok := strings.Cut(part, ":")
		dc = strings.TrimSpace(dc)
		count, err := strconv.Atoi(strings.TrimSpace(n))
		if !ok || dc == "" || err != nil {
			return nil, fmt.Errorf("invalid entry %q (want name:count, e.g. dc1:3,dc2:3)", part)
		}
		if _, dup := out[dc]; dup {
			return nil, fmt.Errorf("datacenter %s listed twice", dc)
		}
		out[dc] = count
	}
	return out, nil
}

func Load() (*Config, error) {
	loadDotEnv()

	var errs []string

	requireEnv := func(key string) string {
		val := os.Getenv(key)
		if strings.TrimSpace(val) == "" {
			errs = append(errs, fmt.Sprintf("missing required env var: %s", key))
		}
		return val
	}

	requireInt := func(key string) int {
		valStr := requireEnv(key)
		if valStr == "" {
			return 0
		}
		val, err := strconv.Atoi(valStr)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid integer for env var %s: %v", key, err))
			return 0
		}
		return val
	}

	optionalInt := func(key string, defaultVal int) int {
		valStr := os.Getenv(key)
		if strings.TrimSpace(valStr) == "" {
			return defaultVal
		}
		val, err := strconv.Atoi(valStr)
		if err != nil || val <= 0 {
			errs = append(errs, fmt.Sprintf("invalid positive integer for env var %s: %q", key, valStr))
			return defaultVal
		}
		return val
	}

	requireDurationMS := func(key string) time.Duration {
		ms := requireInt(key)
		return time.Duration(ms) * time.Millisecond
	}

	insecureSkip := strings.EqualFold(os.Getenv("AUTH_INSECURE_SKIP_VERIFY"), "true")

	cfg := &Config{
		AppEnv:                 requireEnv("APP_ENV"),
		LogLevel:               requireEnv("LOG_LEVEL"),
		GRPCPort:               requireEnv("AGGREGATOR_GRPC_PORT"),
		BMKGBaseURL:            requireEnv("BMKG_BASE_URL"),
		PVMBGBaseURL:           requireEnv("PVMBG_BASE_URL"),
		BMKGAPIKey:             requireEnv("BMKG_API_KEY"),
		PVMBGBearerToken:       requireEnv("PVMBG_BEARER_TOKEN"),
		ServiceToken:           requireEnv("AGGREGATOR_SERVICE_TOKEN"),
		AuthAuthority:          os.Getenv("AUTH_AUTHORITY"),
		AuthAudience:           os.Getenv("AUTH_AUDIENCE"),
		AuthInsecureSkipVerify: insecureSkip,
		BMKGPollInterval:       requireDurationMS("BMKG_POLL_INTERVAL_MS"),
		PVMBGPollInterval:      requireDurationMS("PVMBG_POLL_INTERVAL_MS"),
		BMKGRequestTimeout:     requireDurationMS("BMKG_REQUEST_TIMEOUT_MS"),
		PVMBGRequestTimeout:    requireDurationMS("PVMBG_REQUEST_TIMEOUT_MS"),
		SourceStaleThreshold:   requireDurationMS("SOURCE_STALE_AFTER_MS"),
		CanonicalStoreBackend:  strings.ToLower(strings.TrimSpace(os.Getenv("CANONICAL_STORE_BACKEND"))),
		AMQPURL:                requireEnv("AGGREGATOR_AMQP_URL"),
		RabbitMQExchange:       requireEnv("RABBITMQ_EXCHANGE"),
		OutboxMaxRetries:       requireInt("OUTBOX_MAX_RETRIES"),
		OutboxRetention:        time.Duration(optionalInt("OUTBOX_RETENTION_HOURS", 168)) * time.Hour,
	}

	switch cfg.CanonicalStoreBackend {
	case "", "postgres":
		cfg.CanonicalStoreBackend = "postgres"
		cfg.CanonicalStoreDSN = requireEnv("CANONICAL_STORE_DSN")
	case "cassandra":
		c, err := LoadCassandra()
		if err != nil {
			errs = append(errs, err.Error())
		}
		cfg.Cassandra = c
	default:
		errs = append(errs, fmt.Sprintf("invalid CANONICAL_STORE_BACKEND %q (want postgres or cassandra)", cfg.CanonicalStoreBackend))
	}

	leaderMS := func(key string, def time.Duration) time.Duration {
		return time.Duration(optionalInt(key, int(def/time.Millisecond))) * time.Millisecond
	}
	cfg.Leader = LeaderConfig{
		Election:     strings.ToLower(envOr("LEADER_ELECTION", cfg.CanonicalStoreBackend)),
		TTL:          leaderMS("LEADER_LEASE_TTL_MS", 15*time.Second),
		RenewEvery:   leaderMS("LEADER_RENEW_INTERVAL_MS", 5*time.Second),
		RetryEvery:   leaderMS("LEADER_RETRY_INTERVAL_MS", 5*time.Second),
		Margin:       leaderMS("LEADER_FENCE_MARGIN_MS", 5*time.Second),
		Identity:     os.Getenv("LEADER_IDENTITY"),
		K8sNamespace: os.Getenv("LEADER_K8S_NAMESPACE"),
		LeasePrefix:  envOr("LEADER_LEASE_PREFIX", "aggregator"),
	}
	nonNegFloat := func(key string, def float64) float64 {
		v := strings.TrimSpace(os.Getenv(key))
		if v == "" {
			return def
		}
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 {
			errs = append(errs, fmt.Sprintf("invalid non-negative number for env var %s: %q", key, v))
			return def
		}
		return f
	}
	nonNegInt := func(key string, def int) int {
		v := strings.TrimSpace(os.Getenv(key))
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			errs = append(errs, fmt.Sprintf("invalid non-negative integer for env var %s: %q", key, v))
			return def
		}
		return n
	}
	cfg.Leader.YieldPerHour = nonNegFloat("LEADER_YIELD_PER_HOUR", 1)
	cfg.Leader.YieldGrowth = nonNegFloat("LEADER_YIELD_GROWTH", 4)
	cfg.Leader.YieldCooldown = leaderMS("LEADER_YIELD_COOLDOWN_MS", cfg.Leader.RetryEvery)
	cfg.Leader.ReleaseCooldown = leaderMS("LEADER_RELEASE_COOLDOWN_MS", 2*cfg.Leader.TTL)
	cfg.Leader.ReleaseMaxCooldown = leaderMS("LEADER_RELEASE_MAX_COOLDOWN_MS", 5*time.Minute)
	cfg.Leader.UnreachableAfter = nonNegInt("LEADER_UNREACHABLE_AFTER", 3)
	if cfg.Leader.YieldGrowth < 1 {
		errs = append(errs, "LEADER_YIELD_GROWTH must be at least 1")
	}

	switch cfg.Leader.Election {
	case "postgres", "cassandra", "kubernetes", "single":
	default:
		errs = append(errs, fmt.Sprintf("invalid LEADER_ELECTION %q (want postgres, cassandra, kubernetes or single)", cfg.Leader.Election))
	}

	// AUTH_AUTHORITY is required unless insecure mode is explicitly enabled.
	if cfg.AuthAuthority == "" && !insecureSkip {
		errs = append(errs, "AUTH_AUTHORITY is required (or set AUTH_INSECURE_SKIP_VERIFY=true for development)")
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("failed to load configuration:\n  - %s", strings.Join(errs, "\n  - "))
	}

	return cfg, nil
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

func loadDotEnv() {
	candidates := []string{".env", "../.env", "../../.env"}
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			k := strings.TrimSpace(parts[0])
			v := strings.Trim(strings.TrimSpace(parts[1]), "\"'")
			if os.Getenv(k) == "" && v != "" {
				_ = os.Setenv(k, v)
			}
		}
		break
	}
}

// func getEnv(key, defaultVal string) string {
// 	if val := os.Getenv(key); val != "" {
// 		return val
// 	}
// 	return defaultVal
// }

// func getEnvInt(key string, defaultVal int) int {
// 	if val := os.Getenv(key); val != "" {
// 		if i, err := strconv.Atoi(val); err == nil {
// 			return i
// 		}
// 	}
// 	return defaultVal
// }
