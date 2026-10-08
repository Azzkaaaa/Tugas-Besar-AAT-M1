// Package server assembles the HTTP surface from the generated gatewayapi strict server: the
// embedded-spec request validator, contract-derived problem responses, and middleware.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	validator "github.com/oapi-codegen/nethttp-middleware"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc/metadata"

	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/contract"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/exchange"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/handlers"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
)

const correlationHeader = "X-Correlation-ID"

var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type Options struct {
	Clients           handlers.Clients
	Contract          *contract.Contract
	RequestTimeout    time.Duration
	MaxInFlight       int
	RetryAfterSeconds int
	Heartbeat         time.Duration
}

type gateway struct {
	contract   *contract.Contract
	retryAfter int
	inFlight   chan struct{}
}

// New returns the gateway handler.
func New(opts Options) http.Handler {
	g := &gateway{contract: opts.Contract, retryAfter: opts.RetryAfterSeconds, inFlight: make(chan struct{}, max(opts.MaxInFlight, 1))}
	strict := gw.NewStrictHandlerWithOptions(
		&handlers.Handlers{Clients: opts.Clients, Timeout: opts.RequestTimeout, Heartbeat: opts.Heartbeat},
		[]gw.StrictMiddlewareFunc{g.limit},
		gw.StrictHTTPServerOptions{
			RequestErrorHandlerFunc:  g.requestError,
			ResponseErrorHandlerFunc: g.responseError,
		},
	)

	router := chi.NewRouter()
	router.Use(g.recoverer, g.correlation)
	router.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		g.write(w, r, nil, problem.New(http.StatusNotFound, "No API operation matches this path."))
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		g.write(w, r, nil, problem.New(http.StatusMethodNotAllowed, "The method is not allowed on this path."))
	})
	api := router.With(g.validate())
	gw.HandlerWithOptions(strict, gw.ChiServerOptions{BaseRouter: api, ErrorHandlerFunc: g.requestError})

	return otelhttp.NewHandler(router, "api-gateway", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		if route := chi.RouteContext(r.Context()); route != nil && route.RoutePattern() != "" {
			return r.Method + " " + route.RoutePattern()
		}
		return r.Method
	}))
}

// validate checks every request against the embedded contract: parameter types, enum labels,
// bounds, patterns, and body schemas (additionalProperties:false included). Authentication is
// Envoy's job (cookie → bearer → token exchange); backends enforce scopes.
func (g *gateway) validate() func(http.Handler) http.Handler {
	return validator.OapiRequestValidatorWithOptions(g.contract.Doc, &validator.Options{
		Options:               openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
		DoNotValidateServers:  true,
		SilenceServersWarning: true,
		ErrorHandlerWithOpts: func(_ context.Context, err error, w http.ResponseWriter, r *http.Request, opts validator.ErrorHandlerOpts) {
			var op *contract.Operation
			if opts.MatchedRoute != nil && opts.MatchedRoute.Route != nil {
				op = g.contract.Operation(opts.MatchedRoute.Route.Method, opts.MatchedRoute.Route.Path)
			}
			e := problem.New(opts.StatusCode, "")
			switch opts.StatusCode {
			case http.StatusBadRequest:
				e = problem.BadRequest("%s", validationDetail(err))
			case http.StatusNotFound:
				e = problem.New(http.StatusNotFound, "No API operation matches this path.")
			case http.StatusMethodNotAllowed:
				e = problem.New(http.StatusMethodNotAllowed, "The method is not allowed on this path.")
			default:
				slog.ErrorContext(r.Context(), "request validation failed", "error", err)
				e = problem.New(http.StatusInternalServerError, "Unexpected gateway failure.")
			}
			g.write(w, r, op, e)
		},
	})
}

// validationDetail keeps the first line of a kin-openapi error, which names the parameter or
// body field and the violated constraint.
func validationDetail(err error) string {
	line, _, _ := strings.Cut(err.Error(), "\n")
	if line == "" {
		return "The request does not match the API contract."
	}
	return line
}

// correlation reuses a valid X-Correlation-ID or creates one, echoes it, and installs the
// exchange plus the outgoing gRPC metadata (actor bearer, correlation ID) on the context.
// Cookies never reach this service; Envoy turns them into the bearer.
func (g *gateway) correlation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(correlationHeader)
		invalid := id != "" && !correlationPattern.MatchString(id)
		if id == "" || invalid {
			id = uuid.NewString()
		}
		r.Header.Set(correlationHeader, id)
		w.Header().Set(correlationHeader, id)
		ex := &exchange.Exchange{CorrelationID: id}
		md := metadata.Pairs("x-correlation-id", id)
		if authorization := r.Header.Get("Authorization"); authorization != "" {
			md.Set("authorization", authorization)
		}
		r = r.WithContext(metadata.NewOutgoingContext(exchange.With(r.Context(), ex), md))
		ex.Request = r
		if invalid {
			g.write(w, r, nil, problem.BadRequest("X-Correlation-ID must be 1-128 characters of A-Z, a-z, 0-9, '.', '_', ':' or '-'"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// limit bounds concurrent unary requests. Streams are long-lived and not counted.
func (g *gateway) limit(next gw.StrictHandlerFunc, operationID string) gw.StrictHandlerFunc {
	if strings.HasPrefix(operationID, "Stream") {
		return next
	}
	return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
		select {
		case g.inFlight <- struct{}{}:
			defer func() { <-g.inFlight }()
			return next(ctx, w, r, request)
		default:
			return nil, problem.New(http.StatusTooManyRequests, "Too many requests; retry later.")
		}
	}
}

func (g *gateway) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				if recovered == http.ErrAbortHandler {
					panic(recovered)
				}
				slog.ErrorContext(r.Context(), "panic serving request", "panic", recovered)
				g.write(w, r, nil, problem.New(http.StatusInternalServerError, "Unexpected gateway failure."))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// requestError covers parameter binding and body decoding failures in the generated code.
func (g *gateway) requestError(w http.ResponseWriter, r *http.Request, err error) {
	g.write(w, r, nil, problem.BadRequest("%s", validationDetail(err)))
}

// responseError renders handler failures. Errors that are not *problem.Error are gateway bugs.
func (g *gateway) responseError(w http.ResponseWriter, r *http.Request, err error) {
	var gatewayErr *problem.Error
	if !errors.As(err, &gatewayErr) {
		gatewayErr = problem.New(http.StatusInternalServerError, "Unexpected gateway failure.")
	}
	if gatewayErr.Status >= http.StatusInternalServerError || errors.Unwrap(gatewayErr) != nil {
		slog.WarnContext(r.Context(), "request failed", "status", gatewayErr.Status, "error", err)
	}
	g.write(w, r, nil, gatewayErr)
}

// write renders a problem with a status the operation documents. Without an explicit
// operation, the matched chi route pattern (equal to the OpenAPI path) selects it.
func (g *gateway) write(w http.ResponseWriter, r *http.Request, op *contract.Operation, e *problem.Error) {
	if op == nil {
		if route := chi.RouteContext(r.Context()); route != nil {
			op = g.contract.Operation(r.Method, route.RoutePattern())
		}
	}
	if op != nil {
		e = problem.Fit(e, op.Allows)
	}
	problem.Write(w, r, e, exchange.From(r.Context()).CorrelationID, g.retryAfter)
}
