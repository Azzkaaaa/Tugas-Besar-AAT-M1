package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"github.com/jackc/pgx/v5"
)

// leases is a leader.LeaseStore on the leader_leases table. Unlike a session advisory lock, a
// lease outlives its holder's connection until it expires, so a replica whose connection dropped
// has until its local deadline to stop (see leader.Lease) before another can take over.
type leases struct{ db *DB }

var _ leader.LeaseStore = leases{}

// Leases returns the lease store of db.
func (db *DB) Leases() leader.LeaseStore { return leases{db} }

func (l leases) Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	var got string
	err := l.db.Pool.QueryRow(ctx, `
		INSERT INTO leader_leases (name, holder, expires_at)
		VALUES ($1, $2, now() + $3 * INTERVAL '1 millisecond')
		ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, expires_at = EXCLUDED.expires_at
		WHERE leader_leases.expires_at < now() OR leader_leases.holder = EXCLUDED.holder
		RETURNING holder
	`, name, holder, ttl.Milliseconds()).Scan(&got)
	if err != nil {
		if isNoRows(err) {
			return false, nil
		}
		return false, err
	}
	return got == holder, nil
}

func (l leases) Renew(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	tag, err := l.db.Pool.Exec(ctx, `
		UPDATE leader_leases SET expires_at = now() + $3 * INTERVAL '1 millisecond'
		WHERE name = $1 AND holder = $2 AND expires_at > now()
	`, name, holder, ttl.Milliseconds())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (l leases) Release(ctx context.Context, name, holder string) error {
	_, err := l.db.Pool.Exec(ctx, `DELETE FROM leader_leases WHERE name = $1 AND holder = $2`, name, holder)
	return err
}

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
