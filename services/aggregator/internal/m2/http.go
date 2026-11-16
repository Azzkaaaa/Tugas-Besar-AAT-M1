package m2

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
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
	return &http.Client{Transport: otelhttp.NewTransport(t), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
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
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 4096), max+1)
	f := sseFrame{}
	size := 0
	for s.Scan() {
		line := s.Text()
		if line == "" {
			if len(f.Data) > 0 {
				if f.Kind == "" {
					f.Kind = "message"
				}
				f.Data = f.Data[:len(f.Data)-1]
				if err := consume(f); err != nil {
					return err
				}
			}
			f = sseFrame{}
			size = 0
			continue
		}
		size += len(line)
		if size > max {
			return errors.New("sse_frame_size_exceeded")
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		name, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch name {
		case "id":
			if strings.ContainsRune(value, 0) {
				return errors.New("invalid_sse_id")
			}
			f.ID = value
		case "event":
			f.Kind = value
		case "data":
			f.Data = append(f.Data, []byte(value+"\n")...)
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	return io.EOF
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
