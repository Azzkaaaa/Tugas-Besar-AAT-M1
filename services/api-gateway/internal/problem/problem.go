// Package problem carries gateway errors and renders them as RFC 9457 problem details.
package problem

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"
)

// Error is an HTTP-level failure. Detail is always safe for every client class.
type Error struct {
	Status int
	Detail string
	cause  error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%d %s: %v", e.Status, e.Detail, e.cause)
	}
	return fmt.Sprintf("%d %s", e.Status, e.Detail)
}

func (e *Error) Unwrap() error { return e.cause }

func New(status int, detail string) *Error { return &Error{Status: status, Detail: detail} }

func BadRequest(format string, args ...any) *Error {
	return New(http.StatusBadRequest, fmt.Sprintf(format, args...))
}

// InvalidUpstream reports a backend response the gateway cannot render under the contract.
func InvalidUpstream(cause error) *Error {
	return &Error{Status: http.StatusServiceUnavailable, Detail: details[http.StatusServiceUnavailable], cause: cause}
}

// FromGRPC maps a backend status. Backend messages are never echoed to the client.
func FromGRPC(err error) *Error {
	st, _ := status.FromError(err)
	code := http.StatusServiceUnavailable
	switch st.Code() {
	case codes.InvalidArgument, codes.FailedPrecondition:
		code = http.StatusBadRequest
	case codes.Unauthenticated:
		code = http.StatusUnauthorized
	case codes.PermissionDenied:
		code = http.StatusForbidden
	case codes.NotFound:
		code = http.StatusNotFound
	case codes.OutOfRange:
		code = http.StatusConflict
	case codes.ResourceExhausted:
		code = http.StatusTooManyRequests
	}
	return &Error{Status: code, Detail: details[code], cause: err}
}

var details = map[int]string{
	http.StatusBadRequest:          "The request is invalid.",
	http.StatusUnauthorized:        "Authentication is missing, invalid, or expired.",
	http.StatusForbidden:           "The caller is not allowed to perform this request.",
	http.StatusNotFound:            "The requested resource was not found.",
	http.StatusConflict:            "The snapshot or cursor can no longer be continued; start over.",
	http.StatusTooManyRequests:     "Too many requests; retry later.",
	http.StatusServiceUnavailable:  "A dependency is unavailable; retry later.",
	http.StatusInternalServerError: "Unexpected gateway failure.",
}

var slugs = map[int]string{
	http.StatusBadRequest:          "invalid-request",
	http.StatusUnauthorized:        "unauthenticated",
	http.StatusForbidden:           "forbidden",
	http.StatusNotFound:            "not-found",
	http.StatusConflict:            "snapshot-expired",
	http.StatusTooManyRequests:     "rate-limited",
	http.StatusServiceUnavailable:  "unavailable",
	http.StatusInternalServerError: "internal",
	http.StatusMethodNotAllowed:    "method-not-allowed",
}

// Fit narrows a status to one the operation documents: 409→400, 429→503, then 503, else 500.
func Fit(e *Error, allows func(int) bool) *Error {
	if allows(e.Status) {
		return e
	}
	for _, fallback := range map[int][]int{
		http.StatusConflict:        {http.StatusBadRequest},
		http.StatusTooManyRequests: {http.StatusServiceUnavailable},
	}[e.Status] {
		if allows(fallback) {
			return &Error{Status: fallback, Detail: details[fallback], cause: e}
		}
	}
	for _, fallback := range []int{http.StatusServiceUnavailable, http.StatusInternalServerError} {
		if allows(fallback) {
			return &Error{Status: fallback, Detail: details[fallback], cause: e}
		}
	}
	return &Error{Status: http.StatusInternalServerError, Detail: details[http.StatusInternalServerError], cause: e}
}

// Write renders the problem. The caller has already set X-Correlation-ID.
func Write(w http.ResponseWriter, r *http.Request, e *Error, correlationID string, retryAfterSeconds int) {
	slug := slugs[e.Status]
	if slug == "" {
		slug = "error"
	}
	header := w.Header()
	header.Set("Content-Type", "application/problem+json")
	switch e.Status {
	case http.StatusUnauthorized:
		header.Set("WWW-Authenticate", "Bearer")
	case http.StatusTooManyRequests, http.StatusServiceUnavailable:
		header.Set("Retry-After", strconv.Itoa(max(retryAfterSeconds, 1)))
	}
	w.WriteHeader(e.Status)
	_ = json.NewEncoder(w).Encode(gatewayapi.Problem{
		Type:          "urn:bnpb:problem:" + slug,
		Title:         http.StatusText(e.Status),
		Status:        e.Status,
		Detail:        e.Detail,
		Instance:      r.URL.EscapedPath(),
		CorrelationId: correlationID,
	})
}
