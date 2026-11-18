package m2

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/aggregator/internal/leader"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

type statusError struct {
	Status     int
	RetryAfter time.Duration
}

func (e *statusError) Error() string { return fmt.Sprintf("upstream_http_%d", e.Status) }
func client(timeout time.Duration) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxConnsPerHost = 2
	t.MaxIdleConnsPerHost = 2
	t.ResponseHeaderTimeout = timeout
	t.DialContext = (&net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}).DialContext
	return &http.Client{Transport: otelhttp.NewTransport(leader.GuardTransport(t)), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
func get(ctx context.Context, c *http.Client, url, token string, max int) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, httpFailure(resp)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, int64(max)+1))
	if err != nil {
		return nil, err
	}
	if len(b) > max {
		return b, errors.New("body_size_exceeded")
	}
	return b, nil
}
func httpFailure(r *http.Response) *statusError {
	var retry time.Duration
	if n, err := strconv.Atoi(r.Header.Get("Retry-After")); err == nil {
		retry = time.Duration(n) * time.Second
	} else if t, err := http.ParseTime(r.Header.Get("Retry-After")); err == nil {
		retry = time.Until(t)
	}
	if retry < 0 {
		retry = 0
	}
	if retry > time.Minute {
		retry = time.Minute
	}
	return &statusError{r.StatusCode, retry}
}
func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type sseFrame struct {
	ID, Kind string
	Data     []byte
}

// ReadSSE implements multiline data, CRLF, comments, and bounded frame sizes.
// A half frame at EOF is NOT committed: the peer must terminate it with a blank line.
func ReadSSE(r io.Reader, max int, consume func(sseFrame) error) error {
	return ReadSSECaptured(r, max, consume, nil)
}

// Breaker is local to the single elected coastal poller, using monotonic elapsed
// time. Only one probe runs after OpenFor. Successful /healthz is never consulted.
type Breaker struct {
	Failures, Threshold int
	OpenUntil           time.Time
	OpenFor             time.Duration
}

func (b *Breaker) Allow(now time.Time) bool { return !now.Before(b.OpenUntil) }
func (b *Breaker) Success()                 { b.Failures = 0; b.OpenUntil = time.Time{} }
func (b *Breaker) Fail(now time.Time, err error) {
	b.Failures++
	if b.Failures >= b.Threshold {
		b.OpenUntil = now.Add(b.OpenFor)
	}
	var se *statusError
	if errors.As(err, &se) && se.RetryAfter > 0 {
		until := now.Add(se.RetryAfter)
		if until.After(b.OpenUntil) {
			b.OpenUntil = until
		}
	}
}
