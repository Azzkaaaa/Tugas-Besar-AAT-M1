package backend

import (
	"sync"
	"time"

	"google.golang.org/grpc/resolver"
)

const refreshScheme = "dns-refresh"

// refreshingBuilder wraps the DNS resolver, which only resolves again after a connection error,
// and asks it to resolve every interval. The DNS resolver rate-limits itself to one lookup per
// 30 s, so shorter intervals behave like 30 s.
type refreshingBuilder struct {
	inner    resolver.Builder
	interval time.Duration
}

func newRefreshingBuilder(interval time.Duration) refreshingBuilder {
	return refreshingBuilder{inner: resolver.Get("dns"), interval: interval}
}

func (b refreshingBuilder) Scheme() string { return refreshScheme }

func (b refreshingBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	inner, err := b.inner.Build(target, cc, opts)
	if err != nil {
		return nil, err
	}
	r := &refreshingResolver{Resolver: inner, stop: make(chan struct{}), done: make(chan struct{})}
	go r.refresh(b.interval)
	return r, nil
}

type refreshingResolver struct {
	resolver.Resolver
	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func (r *refreshingResolver) refresh(interval time.Duration) {
	defer close(r.done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-ticker.C:
			r.Resolver.ResolveNow(resolver.ResolveNowOptions{})
		}
	}
}

// Close stops refreshing before closing the DNS resolver, so it is never asked to resolve after
// it was closed.
func (r *refreshingResolver) Close() {
	r.closeOnce.Do(func() {
		close(r.stop)
		<-r.done
		r.Resolver.Close()
	})
}
