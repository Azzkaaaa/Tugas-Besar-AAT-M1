package cassandra

import (
	"context"
	"math"
	"strconv"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
)

// leases is a leader.LeaseStore on the leader_leases table: a row per lease whose TTL is the
// lease's expiry, written with lightweight transactions (LOCAL_SERIAL), so at most one holder
// wins. Cassandra TTLs are whole seconds; the TTL is rounded up.
type leases struct{ db *DB }

var _ leader.LeaseStore = leases{}

// Leases returns the lease store of db.
func (db *DB) Leases() leader.LeaseStore { return leases{db} }

func ttlSeconds(ttl time.Duration) string {
	return strconv.Itoa(int(math.Ceil(ttl.Seconds())))
}

func (l leases) Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	existing := map[string]any{}
	applied, err := l.db.session.Query(`INSERT INTO leader_leases (name, holder) VALUES (?, ?) IF NOT EXISTS USING TTL `+
		ttlSeconds(ttl), name, holder).WithContext(ctx).MapScanCAS(existing)
	if err != nil || applied {
		return applied, err
	}
	if h, _ := existing["holder"].(string); h == holder {
		// Still ours (e.g. after a restart with the same identity): renew it.
		return l.Renew(ctx, name, holder, ttl)
	}
	return false, nil
}

func (l leases) Renew(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	return l.db.session.Query(`UPDATE leader_leases USING TTL `+ttlSeconds(ttl)+` SET holder = ? WHERE name = ? IF holder = ?`,
		holder, name, holder).WithContext(ctx).MapScanCAS(map[string]any{})
}

func (l leases) Release(ctx context.Context, name, holder string) error {
	_, err := l.db.session.Query(`DELETE FROM leader_leases WHERE name = ? IF holder = ?`, name, holder).
		WithContext(ctx).MapScanCAS(map[string]any{})
	return err
}
