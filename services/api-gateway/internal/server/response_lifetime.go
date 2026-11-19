package server

import (
	"context"
	"net/http"

	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/exchange"
)

type responseReleases struct{ items []func() }
type responseReleasesKey struct{}

// Generated strict handlers return before JSON encoding/writing. Keep their
// admission credits until the outer HTTP handler has completed that work,
// including a blocked response writer or a panic while rendering.
func responseLifetime(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		releases := &responseReleases{}
		defer func() {
			if body := exchange.From(r.Context()).ResponseBody; body != nil {
				_ = body.Close()
			}
			for i := len(releases.items) - 1; i >= 0; i-- {
				releases.items[i]()
			}
		}()
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), responseReleasesKey{}, releases)))
	})
}

func isFullStream(request any) bool {
	switch req := request.(type) {
	case gw.StreamHazardEventsRequestObject:
		return req.Params.View != nil && *req.Params.View == gw.StreamHazardEventsParamsViewFull
	case gw.StreamSeismicHazardsRequestObject:
		return req.Params.View != nil && *req.Params.View == gw.StreamSeismicHazardsParamsViewFull
	case gw.StreamVolcanicHazardsRequestObject:
		return req.Params.View != nil && *req.Params.View == gw.StreamVolcanicHazardsParamsViewFull
	default:
		return false
	}
}

// holdResponse returns a local fallback for direct strict-middleware callers;
// production requests always register with the outer responseLifetime instead.
func holdResponse(ctx context.Context, release func()) func() {
	if releases, ok := ctx.Value(responseReleasesKey{}).(*responseReleases); ok {
		releases.items = append(releases.items, release)
		return func() {}
	}
	return release
}

func isFullRead(request any) bool {
	switch req := request.(type) {
	case gw.ListHazardsRequestObject:
		return req.Params.View != nil && *req.Params.View == gw.ListHazardsParamsViewFull
	case gw.GetHazardRequestObject:
		return req.Params.View != nil && *req.Params.View == gw.GetHazardParamsViewFull
	case gw.ListNotificationsRequestObject:
		return req.Params.View != nil && *req.Params.View == gw.ListNotificationsParamsViewFull
	case gw.GetNotificationRequestObject:
		return req.Params.View != nil && *req.Params.View == gw.GetNotificationParamsViewFull
	default:
		return false
	}
}
