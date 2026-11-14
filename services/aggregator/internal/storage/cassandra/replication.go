package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

const (
	SimpleStrategy          = "SimpleStrategy"
	NetworkTopologyStrategy = "NetworkTopologyStrategy"
)

// Replication is how a new keyspace is replicated. It is applied when the keyspace is created;
// an existing keyspace keeps its own and Open only warns when they differ (see
// replicationMismatch), because changing it needs ALTER KEYSPACE and a repair.
type Replication struct {
	// Strategy is SimpleStrategy (default; one datacenter) or NetworkTopologyStrategy.
	Strategy string
	// Factor is the replica count for SimpleStrategy (default 1).
	Factor int
	// Datacenters is the replica count per datacenter for NetworkTopologyStrategy.
	Datacenters map[string]int
}

// normalized fills defaults and checks the settings.
func (r Replication) normalized() (Replication, error) {
	switch strings.ToLower(r.Strategy) {
	case "", "simple", strings.ToLower(SimpleStrategy):
		r.Strategy = SimpleStrategy
		if r.Factor == 0 {
			r.Factor = 1
		}
		if r.Factor < 0 {
			return r, fmt.Errorf("replication factor must be positive, got %d", r.Factor)
		}
		if len(r.Datacenters) > 0 {
			return r, errors.New("per-datacenter replication needs NetworkTopologyStrategy")
		}
	case "networktopology", strings.ToLower(NetworkTopologyStrategy):
		r.Strategy = NetworkTopologyStrategy
		if len(r.Datacenters) == 0 {
			return r, errors.New("NetworkTopologyStrategy needs the replica count per datacenter (e.g. dc1:3,dc2:3)")
		}
		for dc, n := range r.Datacenters {
			if !validDatacenter(dc) {
				return r, fmt.Errorf("invalid datacenter name %q", dc)
			}
			if n <= 0 {
				return r, fmt.Errorf("replica count for datacenter %s must be positive, got %d", dc, n)
			}
		}
		r.Factor = 0
	default:
		return r, fmt.Errorf("unknown replication strategy %q (want SimpleStrategy or NetworkTopologyStrategy)", r.Strategy)
	}
	return r, nil
}

// options is the replication map as Cassandra reports it in system_schema.keyspaces (short
// class name).
func (r Replication) options() map[string]string {
	if r.Strategy == SimpleStrategy {
		return map[string]string{"class": SimpleStrategy, "replication_factor": strconv.Itoa(r.Factor)}
	}
	m := map[string]string{"class": NetworkTopologyStrategy}
	for dc, n := range r.Datacenters {
		m[dc] = strconv.Itoa(n)
	}
	return m
}

// cql renders the map for CREATE/ALTER KEYSPACE ... WITH replication = <cql>.
func (r Replication) cql() string { return renderOptions(r.options()) }

// renderOptions renders a replication map as CQL, class first, then keys sorted.
func renderOptions(opts map[string]string) string {
	keys := make([]string, 0, len(opts))
	for k := range opts {
		if k != "class" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := []string{fmt.Sprintf("'class': '%s'", opts["class"])}
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("'%s': %s", k, opts[k]))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func (r Replication) String() string {
	return r.cql()
}

// keyspaceReplication returns the replication map of keyspace, or nil when it does not exist.
func keyspaceReplication(ctx context.Context, session *gocql.Session, keyspace string) (map[string]string, error) {
	var repl map[string]string
	err := session.Query(`SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = ?`, keyspace).
		WithContext(ctx).Scan(&repl)
	if errors.Is(err, gocql.ErrNotFound) {
		return nil, nil
	}
	return repl, err
}

// replicationMismatch describes how actual (from system_schema) differs from want, or "".
func replicationMismatch(actual map[string]string, want Replication) string {
	got := map[string]string{}
	for k, v := range actual {
		if k == "class" {
			v = v[strings.LastIndex(v, ".")+1:] // org.apache.cassandra.locator.SimpleStrategy
		}
		got[k] = v
	}
	wantOpts := want.options()
	same := len(got) == len(wantOpts)
	for k, v := range wantOpts {
		if got[k] != v {
			same = false
		}
	}
	if same {
		return ""
	}
	return fmt.Sprintf("keyspace has replication %s, configured %s", renderOptions(got), want.cql())
}

// validDatacenter accepts the names Cassandra datacenters usually have (letters, digits, '_',
// '-', '.'); they are interpolated into CQL.
func validDatacenter(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !(r == '_' || r == '-' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
