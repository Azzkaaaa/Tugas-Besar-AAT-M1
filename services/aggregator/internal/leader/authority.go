package leader

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
)

var ErrLostAuthority = errors.New("leader authority expired or superseded")

// Authority identifies one acquisition, rather than the lifetime of a pod.
// Epoch is allocated by storage and must never be reused after expiry/release.
type Authority struct {
	Lease  string
	Holder string
	Epoch  int64
}

// FencingStore installs authority at its mutation boundaries before work starts
// or a renewal is considered successful. Validate is also used before upstream
// I/O; writes must still enforce authority atomically in their own transaction.
type FencingStore interface {
	Fence(context.Context, string, string) (Authority, error)
	ValidateAuthority(context.Context, Authority) error
}

type authorityKey struct{}
type authorityContext struct {
	token Authority
	store FencingStore
}

func WithAuthority(ctx context.Context, token Authority, store FencingStore) context.Context {
	return context.WithValue(ctx, authorityKey{}, authorityContext{token, store})
}
func AuthorityFromContext(ctx context.Context) (Authority, bool) {
	a, ok := ctx.Value(authorityKey{}).(authorityContext)
	return a.token, ok
}
func CheckAuthority(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a, ok := ctx.Value(authorityKey{}).(authorityContext); ok && a.store != nil {
		err := a.store.ValidateAuthority(ctx, a.token)
		if errors.Is(err, ErrLostAuthority) {
			// Refused before any upstream I/O or write: a stale holder neither polls nor saves.
			slog.WarnContext(ctx, "fenced operation refused", "component", "fencing",
				"lease", a.token.Lease, "holder", a.token.Holder, "epoch", a.token.Epoch)
		}
		return err
	}
	return nil
}

type authorityTransport struct{ next http.RoundTripper }

func (t authorityTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := CheckAuthority(req.Context()); err != nil {
		return nil, err
	}
	return t.next.RoundTrip(req)
}
func GuardTransport(next http.RoundTripper) http.RoundTripper { return authorityTransport{next} }
