package cassandra

import (
	"context"
	"errors"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Durable epochs and storage-time expiry survive release. Partition activation
// prevents delayed older terms from overriding a newer fencing epoch.
type leases struct{ db *DB }

type leaseState struct {
	holder *string
	epoch  *int64
	until  *time.Time
	now    time.Time
}

func (s leaseState) holderValue() string {
	if s.holder == nil {
		return ""
	}
	return *s.holder
}
func (s leaseState) live() bool { return s.until != nil && s.until.After(s.now) }

var _ leader.LeaseStore = leases{}

// Leases returns the lease store of db.
func (db *DB) Leases() leader.LeaseStore { return leases{db} }

func (l leases) read(ctx context.Context, name string) (leaseState, error) {
	for range 8 {
		var s leaseState
		var oldTTL *int
		err := l.db.session.Query(`SELECT holder,epoch,expires_at,toTimestamp(now()),ttl(holder) FROM leader_leases WHERE name=?`, name).WithContext(ctx).Consistency(gocql.LocalSerial).Scan(&s.holder, &s.epoch, &s.until, &s.now, &oldTTL)
		if errors.Is(err, gocql.ErrNotFound) {
			_, err = l.db.session.Query(`INSERT INTO leader_leases (name,holder,epoch,expires_at) VALUES (?,'',0,?) IF NOT EXISTS`, name, time.Unix(0, 0)).WithContext(ctx).MapScanCAS(map[string]any{})
			if err != nil {
				return s, err
			}
			continue
		}
		if err != nil {
			return s, err
		}
		if s.epoch == nil || s.until == nil {
			until := time.Unix(0, 0)
			if oldTTL != nil && *oldTTL > 0 {
				until = s.now.Add(time.Duration(*oldTTL) * time.Second)
			}
			if s.epoch == nil {
				_, err = l.db.session.Query(`UPDATE leader_leases SET epoch=0,expires_at=? WHERE name=? IF epoch=null`, until, name).WithContext(ctx).MapScanCAS(map[string]any{})
			} else {
				_, err = l.db.session.Query(`UPDATE leader_leases SET expires_at=? WHERE name=? IF expires_at=null`, until, name).WithContext(ctx).MapScanCAS(map[string]any{})
			}
			if err != nil {
				return s, err
			}
			continue
		}
		s.now = s.now.UTC()
		return s, nil
	}
	return leaseState{}, errors.New("lease metadata kept changing")
}

func (l leases) Acquire(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	for range 8 {
		s, err := l.read(ctx, name)
		if err != nil {
			return false, err
		}
		if s.live() && s.holderValue() != holder {
			return false, nil
		}
		epoch := *s.epoch
		if !s.live() || s.holderValue() != holder {
			epoch++
		}
		ok, err := l.db.session.Query(`UPDATE leader_leases USING TTL 0 SET holder=?,epoch=?,expires_at=? WHERE name=?
			IF holder=? AND epoch=? AND expires_at=?`, holder, epoch, msUTC(s.now.Add(ttl)), name, s.holder, *s.epoch, s.until).WithContext(ctx).MapScanCAS(map[string]any{})
		if err != nil || ok {
			return ok, err
		}
	}
	return false, errors.New("lease acquisition kept changing")
}

func (l leases) Renew(ctx context.Context, name, holder string, ttl time.Duration) (bool, error) {
	s, err := l.read(ctx, name)
	if err != nil {
		return false, err
	}
	if s.holderValue() != holder || !s.live() {
		return false, nil
	}
	return l.db.session.Query(`UPDATE leader_leases SET expires_at=? WHERE name=?
		IF holder=? AND epoch=? AND expires_at=? AND expires_at>toTimestamp(now())`, msUTC(s.now.Add(ttl)), name, holder, *s.epoch, s.until).WithContext(ctx).MapScanCAS(map[string]any{})
}

func (l leases) Release(ctx context.Context, name, holder string) error {
	s, e := l.read(ctx, name)
	if e != nil {
		return e
	}
	if s.holderValue() == holder && s.epoch != nil {
		a := leader.Authority{Lease: name, Holder: holder, Epoch: *s.epoch}
		if err := l.installAll(ctx, authorityPartitions(name), a, time.Unix(0, 0), guardVersion(a)+1); err != nil && !errors.Is(err, leader.ErrLostAuthority) {
			return err
		}
	}
	_, err := l.db.session.Query(`UPDATE leader_leases SET holder='',expires_at=? WHERE name=? IF holder=?`, time.Unix(0, 0), name, holder).WithContext(ctx).MapScanCAS(map[string]any{})
	return err
}
