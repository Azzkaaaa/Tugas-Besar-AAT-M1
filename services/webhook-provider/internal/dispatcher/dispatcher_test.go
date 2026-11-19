package dispatcher

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type countedBody struct {
	io.Reader
	reads  int
	closed bool
}

func TestStreamingResponseClosesAfterStatus(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(202)
		w.(http.Flusher).Flush() // Unknown length; the receiver never ends its stream.
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	d := New(3*time.Second, 1)
	start := time.Now()
	result := d.DispatchOnce(context.Background(), server.URL, "key", []byte("{}"))
	if result.Err != nil || result.StatusCode != 202 || time.Since(start) > time.Second {
		t.Fatalf("stream held worker after status: %+v duration=%v", result, time.Since(start))
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("streaming response connection was not closed")
	}
}

func (b *countedBody) Read(p []byte) (int, error) { b.reads++; return b.Reader.Read(p) }
func (b *countedBody) Close() error               { b.closed = true; return nil }

func TestResponseBodyDrainIsBounded(t *testing.T) {
	for _, size := range []int64{-1, 0, 16, 64 << 10, (64 << 10) + 1, 1 << 40} {
		t.Run(strings.ReplaceAll(time.Duration(size).String(), "/", "_"), func(t *testing.T) {
			body := &countedBody{Reader: strings.NewReader(strings.Repeat("x", 64<<10))}
			d := New(time.Second, 1)
			d.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, ContentLength: size, Body: body, Header: make(http.Header)}, nil
			})
			result := d.DispatchOnce(context.Background(), "http://example.invalid", "key", []byte("{}"))
			if result.Err != nil || result.StatusCode != 200 || !body.closed {
				t.Fatalf("result=%+v closed=%v", result, body.closed)
			}
			if (size < 0 || size > 64<<10) && body.reads != 0 {
				t.Fatalf("unbounded response was read %d times", body.reads)
			}
			if size > 0 && size <= 64<<10 && body.reads == 0 {
				t.Fatal("small response was not drained for connection reuse")
			}
		})
	}
}
