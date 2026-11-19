package server

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	feedv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/feed/v1"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/contract"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/handlers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type streamingCheckWriter struct {
	header  http.Header
	writer  *io.PipeWriter
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *streamingCheckWriter) Header() http.Header { return w.header }
func (*streamingCheckWriter) WriteHeader(int)       {}
func (*streamingCheckWriter) Flush()                {}
func (w *streamingCheckWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return w.writer.Write(p)
}

// Run against the ignored Rust serve_stream_memory_fixture, with both native
// test processes in separate capped containers. Ordinary unit runs skip it.
func TestFullSSELargeFireMemory(t *testing.T) {
	target := os.Getenv("FEED_STREAM_TEST_TARGET")
	if target == "" {
		t.Skip("requires the disposable Rust feed transport fixture")
	}
	connection, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	feed := feedv1.NewEventFeedServiceClient(connection)
	doc, err := contract.Load()
	if err != nil {
		t.Fatal(err)
	}
	handler := New(Options{Contract: doc, Clients: handlers.Clients{Feed: feed, Aggregator: &lifetimeClient{}},
		MaxInFlight: 32, ReservedInFlight: 4, RequestTimeout: 5 * time.Second, Heartbeat: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	request := httptest.NewRequest("GET", "/api/v1/feeds/events?view=full", nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer field")
	reader, output := io.Pipe()
	defer reader.Close()
	writer := &streamingCheckWriter{header: make(http.Header), writer: output, entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer output.Close()
		handler.ServeHTTP(writer, request)
	}()
	var release sync.Once
	defer func() { cancel(); release.Do(func() { close(writer.release) }); _ = reader.Close(); <-done }()
	select {
	case <-writer.entered:
	case <-ctx.Done():
		t.Fatal("large SSE did not reach blocked HTTP output")
	}
	for range 24 {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/feeds/events?view=full", nil))
		if response.Code != 429 {
			t.Fatal("concurrent FULL SSE was not shed", response.Code, response.Body.String())
		}
	}
	// The actual Rust endpoint also rejects another FULL pipeline and still
	// applies authorization before checking its occupied slot.
	for _, test := range []struct {
		token string
		want  codes.Code
	}{{"field", codes.ResourceExhausted}, {"media", codes.PermissionDenied}} {
		upstreamCtx, stop := context.WithTimeout(metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+test.token)), time.Second)
		stream, err := feed.SubscribeEvents(upstreamCtx, &feedv1.SubscribeEventsRequest{View: commonv1.View_VIEW_FULL})
		if err == nil {
			_, err = stream.Recv()
		}
		stop()
		if status.Code(err) != test.want {
			t.Fatalf("feed admission/scope: %s: %v", test.token, err)
		}
	}
	summary := &stopStreamWriter{httptest.NewRecorder()}
	summaryRequest := httptest.NewRequest("GET", "/api/v1/feeds/events?view=summary", nil).WithContext(ctx)
	summaryRequest.Header.Set("Authorization", "Bearer media")
	handler.ServeHTTP(summary, summaryRequest)
	if summary.Code != 200 {
		t.Fatal("SUMMARY unavailable while FULL output blocked", summary.Code)
	}
	release.Do(func() { close(writer.release) })
	buffer := bufio.NewReader(reader)
	for _, prefix := range []string{"id: ", "event: hazard.upsert"} {
		line, err := buffer.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, prefix) {
			t.Fatal("invalid SSE framing", line, err)
		}
	}
	prefix := make([]byte, len("data: "))
	if _, err := io.ReadFull(buffer, prefix); err != nil || string(prefix) != "data: " {
		t.Fatal("invalid data prefix", string(prefix), err)
	}
	decoder := json.NewDecoder(buffer)
	depth, keys := 0, 0
	for {
		token, err := decoder.Token()
		if err != nil {
			t.Fatal(err)
		}
		if token == "detection_keys" {
			if token, err := decoder.Token(); err != nil || token != json.Delim('[') {
				t.Fatal("missing exact detection list", token, err)
			}
			for decoder.More() {
				var key string
				if err := decoder.Decode(&key); err != nil {
					t.Fatal(err)
				}
				want := fmt.Sprintf("N20|2026-11-19|0600|%.8f|106.12345678", -6.1+float64(keys)*0.00000001)
				if key != want {
					t.Fatalf("key %d changed: %q != %q", keys, key, want)
				}
				keys++
			}
			if _, err := decoder.Token(); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
		if depth == 0 {
			break
		}
	}
	if keys != 200000 {
		t.Fatal("truncated FULL SSE detection list", keys)
	}
	cancel()
	_ = reader.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("client cancellation did not join gateway workers")
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	process, _ := os.ReadFile("/proc/self/status")
	peak, _ := os.ReadFile("/sys/fs/cgroup/memory.peak")
	for _, line := range strings.Split(string(process), "\n") {
		if strings.HasPrefix(line, "VmHWM:") {
			t.Log(line)
		}
	}
	t.Logf("STREAM_MEMORY keys=%d shed_full=24 heap_bytes=%d go_sys_bytes=%d container_peak_bytes=%s", keys, memory.HeapAlloc, memory.Sys, strings.TrimSpace(string(peak)))
}
