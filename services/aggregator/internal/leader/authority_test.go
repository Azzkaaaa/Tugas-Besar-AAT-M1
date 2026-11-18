package leader

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type deniedAuthority struct{}

func (deniedAuthority) Fence(context.Context, string, string) (Authority, error) {
	return Authority{}, ErrLostAuthority
}

type fencedMemStore struct{ memStore }

func (s *fencedMemStore) Fence(_ context.Context, name, holder string) (Authority, error) {
	return Authority{Lease: name, Holder: holder, Epoch: 1}, nil
}
func (s *fencedMemStore) ValidateAuthority(context.Context, Authority) error { return nil }

func TestEveryAcquisitionReceivesANewContextIdentity(t *testing.T) {
	store := &fencedMemStore{}
	l := newLease(store, "same-pod")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tokens := make(chan Authority, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.Run(ctx, func(job context.Context) {
			a, ok := AuthorityFromContext(job)
			if !ok {
				t.Error("missing authority context")
				cancel()
				return
			}
			tokens <- a
			if len(tokens) == 2 {
				cancel()
			}
		})
	}()
	<-done
	if len(tokens) != 2 {
		t.Fatal("did not observe two terms")
	}
	a, b := <-tokens, <-tokens
	if a.Holder == b.Holder || a.Lease != l.Name || b.Lease != l.Name {
		t.Fatalf("reused term: %+v %+v", a, b)
	}
}
func (deniedAuthority) ValidateAuthority(context.Context, Authority) error { return ErrLostAuthority }

func TestExpiredLiveContextCannotSendUpstreamRequest(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(204) }))
	defer server.Close()
	ctx := WithAuthority(context.Background(), Authority{Lease: "poll", Holder: "old", Epoch: 1}, deniedAuthority{})
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	client := http.Client{Transport: GuardTransport(http.DefaultTransport)}
	if _, err := client.Do(req); !errors.Is(err, ErrLostAuthority) {
		t.Fatal("expired request not rejected", err)
	}
	if calls != 0 || ctx.Err() != nil {
		t.Fatal("authority check relied on cancellation or sent a request")
	}
	if newTermIdentity() == newTermIdentity() {
		t.Fatal("term identity reused")
	}
}
