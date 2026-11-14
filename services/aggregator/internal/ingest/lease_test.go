package ingest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/config"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
)

func pollerCfg(base string) *config.Config {
	return &config.Config{BMKGBaseURL: base, PVMBGBaseURL: base, BMKGAPIKey: "k", PVMBGBearerToken: "t",
		BMKGRequestTimeout: 500 * time.Millisecond, PVMBGRequestTimeout: 500 * time.Millisecond}
}

// Only transport failures count as unreachable; any HTTP response means the upstream answered.
func TestUnreachableClassification(t *testing.T) {
	ctx := context.Background()
	status := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer status.Close()
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()

	_, err := NewBMKGPoller(pollerCfg(status.URL), nil, nil).fetchSeismicEvents(ctx, time.Time{}, "c")
	if err == nil || unreachable(err) {
		t.Fatalf("503 from BMKG: err=%v unreachable=%v, want an error that is not unreachable", err, unreachable(err))
	}
	_, err = NewPVMBGPoller(pollerCfg(status.URL), nil, nil).fetchVolcanicReports(ctx, time.Time{}, "c")
	if err == nil || unreachable(err) {
		t.Fatalf("503 from PVMBG: err=%v unreachable=%v", err, unreachable(err))
	}
	_, err = NewBMKGPoller(pollerCfg(closedURL), nil, nil).fetchSeismicEvents(ctx, time.Time{}, "c")
	if !unreachable(err) {
		t.Fatalf("refused connection to BMKG not unreachable: %v", err)
	}
	_, err = NewPVMBGPoller(pollerCfg(closedURL), nil, nil).fetchVolcanicReports(ctx, time.Time{}, "c")
	if !unreachable(err) {
		t.Fatalf("refused connection to PVMBG not unreachable (wrapping lost?): %v", err)
	}
	if unreachable(nil) || unreachable(errors.New("decode failed")) || unreachable(context.Canceled) {
		t.Fatal("nil, decode or cancellation errors counted as unreachable")
	}
}

// leaseCtx returns a context carrying a lease term with yielding disabled.
func leaseCtx() context.Context {
	ctx, _ := leader.NewHandoff(&leader.YieldConfig{Growth: 1}).Begin(context.Background(), &leader.Pool{})
	return ctx
}

func TestLeaseGuardReleasesAfterConsecutiveTransportFailures(t *testing.T) {
	transport := &netErr{}
	reached := errors.New("BMKG seismic endpoint returned HTTP 503")

	g := newLeaseGuard("BMKG", 3)
	ctx := leaseCtx()
	for i, err := range []error{transport, transport, reached, transport, transport} {
		if g.after(ctx, err) {
			t.Fatalf("released after call %d (failures must be consecutive; a 503 resets)", i)
		}
	}
	if !g.after(ctx, transport) {
		t.Fatal("did not release after 3 consecutive transport failures")
	}

	// Under the single elector there is nobody to hand over to: keep polling.
	g = newLeaseGuard("BMKG", 1)
	if g.after(context.Background(), transport) {
		t.Fatal("released without a lease term")
	}
	// Disabled.
	g = newLeaseGuard("BMKG", 0)
	for i := 0; i < 5; i++ {
		if g.after(leaseCtx(), transport) {
			t.Fatal("released with the unreachable hand-off disabled")
		}
	}
}

// netErr is a transport error (net.Error).
type netErr struct{}

func (*netErr) Error() string   { return "dial tcp: connection refused" }
func (*netErr) Timeout() bool   { return false }
func (*netErr) Temporary() bool { return false }
