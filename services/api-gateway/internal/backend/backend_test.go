package backend

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/resolver"
)

// replica is an in-process gRPC server that counts the calls it serves.
type replica struct {
	address string
	calls   atomic.Int32
}

func startReplica(t *testing.T) *replica {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &replica{address: listener.Addr().String()}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		r.calls.Add(1)
		return handler(ctx, req)
	}))
	healthpb.RegisterHealthServer(server, health.NewServer())
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return r
}

// fakeDNS stands in for the DNS resolver: like it, it reports addresses when built and again
// only when asked to resolve now. The test changes what it reports.
type fakeDNS struct {
	mu        sync.Mutex
	addresses []string
	resolves  atomic.Int32
	closes    atomic.Int32
	cc        resolver.ClientConn
}

func (f *fakeDNS) Scheme() string { return "fake" }

func (f *fakeDNS) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	f.mu.Lock()
	f.cc = cc
	f.mu.Unlock()
	f.report()
	return f, nil
}

func (f *fakeDNS) ResolveNow(resolver.ResolveNowOptions) {
	f.resolves.Add(1)
	f.report()
}

func (f *fakeDNS) Close() { f.closes.Add(1) }

func (f *fakeDNS) set(addresses ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addresses = addresses
}

func (f *fakeDNS) report() {
	f.mu.Lock()
	defer f.mu.Unlock()
	state := resolver.State{}
	for _, address := range f.addresses {
		state.Endpoints = append(state.Endpoints, resolver.Endpoint{Addresses: []resolver.Address{{Addr: address}}})
	}
	_ = f.cc.UpdateState(state)
}

// dialFake dials through the real refreshing wrapper around fakeDNS, with Dial's service config.
func dialFake(t *testing.T, dns *fakeDNS, interval time.Duration) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(refreshScheme+":///backend",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithResolvers(refreshingBuilder{inner: dns, interval: interval}),
		grpc.WithDisableServiceConfig(),
		grpc.WithDefaultServiceConfig(ServiceConfig))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func check(t *testing.T, conn *grpc.ClientConn, times int) {
	t.Helper()
	client := healthpb.NewHealthClient(conn)
	for i := 0; i < times; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := client.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.WaitForReady(true))
		cancel()
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
}

func TestCallsSpreadAcrossReplicas(t *testing.T) {
	a, b := startReplica(t), startReplica(t)
	dns := &fakeDNS{}
	dns.set(a.address, b.address)
	conn := dialFake(t, dns, time.Hour)

	check(t, conn, 20)
	if a.calls.Load() == 0 || b.calls.Load() == 0 {
		t.Fatalf("calls not spread: a=%d b=%d", a.calls.Load(), b.calls.Load())
	}
}

func TestRefreshFindsNewReplicasAndDropsGoneOnes(t *testing.T) {
	a, b := startReplica(t), startReplica(t)
	dns := &fakeDNS{}
	dns.set(a.address)
	conn := dialFake(t, dns, 50*time.Millisecond)

	check(t, conn, 5)
	if b.calls.Load() != 0 {
		t.Fatal("replica b called before it was in DNS")
	}

	// b scales up: no connection fails, so only the periodic refresh can find it.
	dns.set(a.address, b.address)
	deadline := time.Now().Add(5 * time.Second)
	for b.calls.Load() == 0 && time.Now().Before(deadline) {
		check(t, conn, 1)
	}
	if b.calls.Load() == 0 {
		t.Fatal("new replica b never received calls")
	}

	// a scales down: once the refresh drops it, calls only go to b.
	dns.set(b.address)
	time.Sleep(300 * time.Millisecond)
	before := a.calls.Load()
	check(t, conn, 10)
	if a.calls.Load() != before {
		t.Fatalf("removed replica a still called: %d -> %d", before, a.calls.Load())
	}
}

func TestResolverStopsRefreshingWhenClosed(t *testing.T) {
	dns := &fakeDNS{}
	conn := dialFake(t, dns, 10*time.Millisecond)
	conn.Connect()

	deadline := time.Now().Add(2 * time.Second)
	for dns.resolves.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if dns.resolves.Load() < 3 {
		t.Fatalf("refreshed %d times, want periodic refreshes", dns.resolves.Load())
	}

	_ = conn.Close()
	after := dns.resolves.Load()
	time.Sleep(100 * time.Millisecond)
	// grpc itself may ask once more while shutting down; the ticker must not.
	if got := dns.resolves.Load(); got > after+1 {
		t.Fatalf("still refreshing after close: %d -> %d", after, got)
	}
	if dns.closes.Load() != 1 {
		t.Fatalf("inner resolver closed %d times, want 1", dns.closes.Load())
	}
}

func TestDialResolvesThroughRealDNS(t *testing.T) {
	a := startReplica(t)
	_, port, _ := net.SplitHostPort(a.address)
	conn, err := Dial(net.JoinHostPort("localhost", port), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	check(t, conn, 3)
	if a.calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", a.calls.Load())
	}
}
