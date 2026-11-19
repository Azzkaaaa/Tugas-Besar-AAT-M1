// Package exchange carries per-request data the strict handlers cannot see in their typed
// request objects: the correlation ID and the raw request (for undeclared query parameters).
package exchange

import (
	"context"
	"io"
	"net/http"
)

type Exchange struct {
	Request       *http.Request
	CorrelationID string
	// ResponseBody owns streaming workers. Close it before returning admission,
	// including errors/panics that bypass the generated response visitor.
	ResponseBody io.Closer
}

type key struct{}

func With(ctx context.Context, e *Exchange) context.Context { return context.WithValue(ctx, key{}, e) }

// From returns the current exchange; the gateway middleware always installs one.
func From(ctx context.Context) *Exchange {
	e, _ := ctx.Value(key{}).(*Exchange)
	if e == nil {
		return &Exchange{}
	}
	return e
}
