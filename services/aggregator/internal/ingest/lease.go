package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
)

// leaseGuard decides, after each poll, whether the poller hands its lease over.
type leaseGuard struct {
	source           string
	unreachableAfter int
	failures         int
}

func newLeaseGuard(source string, unreachableAfter int) *leaseGuard {
	return &leaseGuard{source: source, unreachableAfter: unreachableAfter}
}

// after takes the upstream fetch error of the poll just finished and reports whether the poller
// should return: after unreachableAfter transport failures in a row it releases the lease (another
// replica may reach the upstream), and otherwise it may yield to rebalance (leader.MaybeYield).
func (g *leaseGuard) after(ctx context.Context, fetchErr error) bool {
	if ctx.Err() != nil {
		return true
	}
	if unreachable(fetchErr) {
		g.failures++
	} else {
		g.failures = 0
	}
	if g.unreachableAfter > 0 && g.failures >= g.unreachableAfter {
		reason := fmt.Sprintf("%s unreachable from this replica (%d attempts in a row): %v", g.source, g.failures, fetchErr)
		g.failures = 0
		if leader.Release(ctx, reason) {
			slog.WarnContext(ctx, "handing poller to another replica", "source", g.source, "reason", reason)
			return true
		}
	}
	return leader.MaybeYield(ctx)
}

// unreachable reports whether err means the upstream could not be reached at all: DNS, refused or
// reset connections, timeouts, TLS. An HTTP response of any status means it was reached, so it is
// not counted: another replica would get the same answer.
func unreachable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}
